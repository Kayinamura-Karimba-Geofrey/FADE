package features

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/config"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/events"
)

// opTimeout bounds a feature computation when the caller has no deadline.
const opTimeout = 5 * time.Second

// Hooks exposes optional instrumentation without importing the metrics
// package (which would create an import cycle through ingest).
type Hooks struct {
	Capped func(n int)
	Redis  func(time.Duration)
	Cycle  func(time.Duration)
}

func (h Hooks) capped(n int) {
	if h.Capped != nil && n > 0 {
		h.Capped(n)
	}
}

func (h Hooks) redis(d time.Duration) {
	if h.Redis != nil {
		h.Redis(d)
	}
}

func (h Hooks) cycle(d time.Duration) {
	if h.Cycle != nil {
		h.Cycle(d)
	}
}

// Store computes feature vectors from Redis state.
type Store struct {
	redis         redis.Cmdable
	cfg           config.Features
	lateTolerance time.Duration
	dedupeTTL     time.Duration
	script        *redis.Script
	hooks         Hooks
	dayKeys       sync.Map // UTC day number -> []string, oldest first
	hourKeys      sync.Map // UTC hour number -> []string, oldest first
	location      *time.Location
}

// NewStore constructs a store. The caller owns the Redis client.
func NewStore(client redis.Cmdable, cfg config.Features, lateTolerance, dedupeTTL time.Duration, hooks Hooks) *Store {
	return &Store{
		redis:         client,
		cfg:           cfg,
		lateTolerance: lateTolerance,
		dedupeTTL:     dedupeTTL,
		script:        redis.NewScript(featureLua),
		hooks:         hooks,
		location:      cfg.Location(),
	}
}

// Raw is the flat aggregate result returned by the Lua script.
type Raw struct {
	Status string `json:"status"`

	Capped        int   `json:"capped"`
	Vel1m         int64 `json:"vel1m"`
	Vel10m        int64 `json:"vel10m"`
	Vel1h         int64 `json:"vel1h"`
	Amount10m     int64 `json:"amt10m"`
	Amount24h     int64 `json:"amt24h"`
	DistinctRecip int64 `json:"distinct_recip"`
	RecipPresent  int64 `json:"recip_present"`
	SmallRun      int64 `json:"small_run"`

	DailyCount  int64   `json:"daily_count"`
	DailySum    int64   `json:"daily_sum"`
	DailySumsq  int64   `json:"daily_sumsq"`
	BaseDevice  string  `json:"base_device"`
	BaseLat     float64 `json:"base_lat"`
	BaseLon     float64 `json:"base_lon"`
	BaseSIM     string  `json:"base_sim"`
	BaseSIMChg  int64   `json:"base_simchg"`
	BaseCashout int64   `json:"base_cashout"`
	BasePresent int64   `json:"base_present"`

	DevAccounts   int64 `json:"dev_accounts"`
	DevPresent    int64 `json:"dev_present"`
	EdgeLastMS    int64 `json:"edge_last_ms"`
	SenderCounts  int64 `json:"sender_accounts"`
	SenderPresent int64 `json:"sender_present"`
	LastInboundMS int64 `json:"last_inbound_ms"`
	LastCashoutMS int64 `json:"last_cashout_ms"`
	In1h          int64 `json:"in1h"`
	Out1h         int64 `json:"out1h"`

	AgentIn24   int64 `json:"agent_in24"`
	AgentOut24  int64 `json:"agent_out24"`
	AgentNet    int64 `json:"agent_net"`
	WatermarkMS int64 `json:"watermark_ms"`
	SIMChanged  int64 `json:"sim_changed"`
}

// Compute checks dedupe and late status, atomically updates Redis, and builds
// the vector. Duplicate and late events return without any state mutation; the
// caller should still commit the Kafka offset (late events are dead-lettered).
func (s *Store) Compute(ctx context.Context, ev *events.Event) (FeatureVector, Status, error) {
	ctx, cancel := ensureDeadline(ctx)
	defer cancel()

	payload := s.scriptInput(ev)
	start := time.Now()
	res, err := s.script.Run(ctx, s.redis, nil, payload).Text()
	s.hooks.redis(time.Since(start))
	if err != nil {
		return FeatureVector{}, "", fmt.Errorf("feature script: %w", err)
	}
	raw, status, err := decodeRawJSON([]byte(res))
	if err != nil {
		return FeatureVector{}, "", err
	}
	if status != StatusOK {
		return FeatureVector{}, status, nil
	}
	s.hooks.capped(raw.Capped)

	vector := s.buildVector(ev, raw)
	if ev.TxnType == events.P2PSend || ev.TxnType == events.BankTransfer {
		late := raw.WatermarkMS > 0 && ev.EventTimeMS < raw.WatermarkMS
		start = time.Now()
		length, err := s.cycleLength(ctx, ev.Sender.AccountID, ev.Recipient.AccountID, ev.EventTimeMS, late)
		s.hooks.cycle(time.Since(start))
		if err != nil {
			return FeatureVector{}, "", err
		}
		vector.CycleLen2To4 = int64(length)
	}
	return vector, StatusOK, nil
}

func ensureDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, opTimeout)
}

func (s *Store) buildVector(ev *events.Event, raw Raw) FeatureVector {
	cfg := s.cfg
	v := NewVector(ev.EventID, ev.Sender.AccountAgeDays, ev.Sender.KYCTier, cfg.SchemaVersion)
	senderOut := ev.IsOutbound()
	senderIn := !senderOut
	recipientIn := ev.CreditsRecipient()

	if senderOut {
		v.Velocity1m = raw.Vel1m + 1
		v.Velocity10m = raw.Vel10m + 1
		v.Velocity1h = raw.Vel1h + 1
		v.AmountSum10m = raw.Amount10m + ev.Amount
		v.AmountSum24h = raw.Amount24h + ev.Amount
	} else {
		v.Velocity1m = raw.Vel1m
		v.Velocity10m = raw.Vel10m
		v.Velocity1h = raw.Vel1h
		v.AmountSum10m = raw.Amount10m
		v.AmountSum24h = raw.Amount24h
	}

	v.DistinctRecip24h = raw.DistinctRecip
	if senderOut && raw.RecipPresent == 0 {
		v.DistinctRecip24h++
	}
	v.AmountVsAvg30d, v.AmountZScore30d = AmountStats(
		ev.Amount, raw.DailyCount, raw.DailySum, raw.DailySumsq, cfg.Stats.MinCount30d)
	v.NearLimitRatio = NearLimitRatio(ev.Amount, ev.Sender.KYCTier, cfg)
	v.SmallTxnRun1h = raw.SmallRun
	if senderOut {
		lower := int64(float64(cfg.Structuring.ThresholdRWF) * cfg.Structuring.LowerRatio)
		upper := int64(float64(cfg.Structuring.ThresholdRWF) * cfg.Structuring.UpperRatio)
		if ev.Amount >= lower && ev.Amount <= upper {
			v.SmallTxnRun1h++
		}
	}

	v.NewDevice = raw.BasePresent == 1 && raw.BaseDevice != "" && raw.BaseDevice != ev.Sender.DeviceID
	if raw.BasePresent == 1 {
		v.NewGeoKm = HaversineKM(raw.BaseLat, raw.BaseLon,
			ev.Sender.Geo.Lat, ev.Sender.Geo.Lon, cfg.Geo.EarthRadiusKM)
	}
	v.SIMChanged72h = raw.SIMChanged == 1
	if !v.SIMChanged72h && raw.BaseSIMChg >= 0 {
		v.SIMChanged72h = ev.EventTimeMS-raw.BaseSIMChg <= int64(cfg.Windows.SimChange72hS)*1000
	}
	if raw.SIMChanged == 1 {
		v.TimeSinceSIMChange = 0
	} else if raw.BaseSIMChg >= 0 {
		v.TimeSinceSIMChange = (ev.EventTimeMS - raw.BaseSIMChg) / 60000
		if v.TimeSinceSIMChange < 0 {
			v.TimeSinceSIMChange = 0
		}
	}

	v.AccPerDevice7d = raw.DevAccounts
	if raw.DevPresent == 0 {
		v.AccPerDevice7d++
	}
	v.RecipNewForSender = raw.EdgeLastMS == 0 || raw.EdgeLastMS > ev.EventTimeMS ||
		raw.EdgeLastMS <= ev.EventTimeMS-int64(cfg.Windows.Recipient30dS)*1000
	v.RecipDistinctSenders24h = raw.SenderCounts
	if recipientIn && raw.SenderPresent == 0 {
		v.RecipDistinctSenders24h++
	}
	if raw.LastInboundMS > 0 && raw.LastCashoutMS >= raw.LastInboundMS {
		v.RecipCashoutLagMin = (raw.LastCashoutMS - raw.LastInboundMS) / 60000
	}

	in1h := raw.In1h
	out1h := raw.Out1h
	if senderIn {
		in1h += ev.Amount
	}
	if senderOut {
		out1h += ev.Amount
	}
	v.InboundOutboundRatio1h = Ratio1h(in1h, out1h)
	v.NightHour = NightHourIn(time.UnixMilli(ev.EventTimeMS), s.location, cfg)

	agentID := ev.AgentAccountID()
	if agentID != "" {
		agentIn, agentOut := int64(0), int64(0)
		switch ev.TxnType {
		case events.CashIn:
			agentIn = ev.Amount
		case events.CashOut:
			agentOut = ev.Amount
		case events.BankTransfer:
			if ev.Recipient.IsAgent {
				agentIn = ev.Amount
			}
		}
		v.AgentCicoRatio24h = CicoRatio(raw.AgentIn24+agentIn, raw.AgentOut24+agentOut)
		net := raw.AgentNet + agentIn - agentOut
		v.AgentFloatDeltaPct = 100 * float64(net) / float64(cfg.Agent.FloatBaselineRWF)
	}
	return v
}

func (s *Store) scriptInput(ev *events.Event) []byte {
	cfg := s.cfg
	t := ev.EventTimeMS
	eventTime := time.UnixMilli(t).UTC()
	currentHour := eventTime.Format("2006010215")
	currentDay := eventTime.Format("20060102")

	agentID := ev.AgentAccountID()
	agentIn, agentOut := int64(0), int64(0)
	switch ev.TxnType {
	case events.CashIn:
		agentIn = ev.Amount
	case events.CashOut:
		agentOut = ev.Amount
	case events.BankTransfer:
		if ev.Recipient.IsAgent {
			agentIn = ev.Amount
		}
	}
	senderOut := boolInt(ev.IsOutbound())
	senderIn := boolInt(!ev.IsOutbound())
	recipientIn := boolInt(ev.CreditsRecipient())
	recipientOut := boolInt(!ev.CreditsRecipient())

	p := scriptPayload{
		EventID:     ev.EventID,
		T:           t,
		TxnType:     ev.TxnType,
		Amount:      ev.Amount,
		SenderID:    ev.Sender.AccountID,
		RecipientID: ev.Recipient.AccountID,
		DeviceID:    ev.Sender.DeviceID,
		Lat:         ev.Sender.Geo.Lat,
		Lon:         ev.Sender.Geo.Lon,
		SIM:         ev.Sender.SIMSerialHash,

		LateTolMS:               s.lateTolerance.Milliseconds(),
		DedupeTTLS:              int64(s.dedupeTTL.Seconds()),
		StateTTLS:               cfg.Retention.StateTTLS,
		WindowTTLS:              cfg.Retention.WindowTTLS,
		EdgeTTLS:                cfg.Retention.EdgeTTLS,
		DeviceTTLS:              cfg.Retention.DeviceTTLS,
		Cap:                     cfg.Caps.WindowZSet,
		HistCap:                 cfg.Caps.StateHistory,
		DayKey:                  currentDay,
		DayCutoff:               eventTime.AddDate(0, 0, -cfg.Stats.DailyBuckets+1).Format("20060102"),
		DayKeys:                 s.dayKeysFor(eventTime, cfg.Stats.DailyWindowDays),
		HourKeys:                s.hourKeysFor(eventTime),
		CurrentHour:             currentHour,
		HourCutoff:              eventTime.Add(-time.Duration(cfg.Retention.WindowTTLS) * time.Second).Format("2006010215"),
		AgentID:                 agentID,
		Baseline:                cfg.Agent.FloatBaselineRWF,
		SenderOut:               senderOut,
		SenderIn:                senderIn,
		RecipientIn:             recipientIn,
		RecipientOut:            recipientOut,
		SmallLower:              int64(float64(cfg.Structuring.ThresholdRWF) * cfg.Structuring.LowerRatio),
		SmallUpper:              int64(float64(cfg.Structuring.ThresholdRWF) * cfg.Structuring.UpperRatio),
		AgentIn:                 agentIn,
		AgentOut:                agentOut,
		AgentTTLS:               cfg.Retention.StateTTLS,
		AgentHourTTLS:           cfg.Retention.WindowTTLS,
		WindowRetentionMS:       int64(cfg.Retention.WindowTTLS) * 1000,
		DeviceRetentionMS:       int64(cfg.Retention.DeviceTTLS) * 1000,
		EdgeRetentionMS:         int64(cfg.Retention.EdgeTTLS) * 1000,
		RecipSendersRetentionMS: int64(cfg.Retention.DeviceTTLS) * 1000,
	}
	payload, err := json.Marshal(p)
	if err != nil {
		// Every field is a JSON-safe scalar; this cannot fail for a valid config.
		payload = []byte(`{}`)
	}
	return payload
}

// scriptPayload is the single ARGV value consumed by the Lua script. One JSON
// argument keeps the Redis command small and cuts hot-path allocations.
type scriptPayload struct {
	EventID     string  `json:"event_id"`
	T           int64   `json:"t"`
	TxnType     string  `json:"txn_type"`
	Amount      int64   `json:"amount"`
	SenderID    string  `json:"sender_id"`
	RecipientID string  `json:"recipient_id"`
	DeviceID    string  `json:"device_id"`
	Lat         float64 `json:"lat"`
	Lon         float64 `json:"lon"`
	SIM         string  `json:"sim"`

	LateTolMS               int64    `json:"late_tol_ms"`
	DedupeTTLS              int64    `json:"dedupe_ttl_s"`
	StateTTLS               int      `json:"state_ttl_s"`
	WindowTTLS              int      `json:"window_ttl_s"`
	EdgeTTLS                int      `json:"edge_ttl_s"`
	DeviceTTLS              int      `json:"device_ttl_s"`
	Cap                     int      `json:"cap"`
	HistCap                 int      `json:"hist_cap"`
	DayKey                  string   `json:"day_key"`
	DayCutoff               string   `json:"day_cutoff"`
	DayKeys                 []string `json:"day_keys"`
	HourKeys                []string `json:"hour_keys"`
	CurrentHour             string   `json:"current_hour"`
	HourCutoff              string   `json:"hour_cutoff"`
	AgentID                 string   `json:"agent_id"`
	Baseline                int64    `json:"baseline"`
	SenderOut               int      `json:"sender_out"`
	SenderIn                int      `json:"sender_in"`
	RecipientIn             int      `json:"recipient_in"`
	RecipientOut            int      `json:"recipient_out"`
	SmallLower              int64    `json:"small_lower"`
	SmallUpper              int64    `json:"small_upper"`
	AgentIn                 int64    `json:"agent_in"`
	AgentOut                int64    `json:"agent_out"`
	AgentTTLS               int      `json:"agent_ttl_s"`
	AgentHourTTLS           int      `json:"agent_hour_ttl_s"`
	WindowRetentionMS       int64    `json:"window_retention_ms"`
	DeviceRetentionMS       int64    `json:"device_retention_ms"`
	EdgeRetentionMS         int64    `json:"edge_retention_ms"`
	RecipSendersRetentionMS int64    `json:"recip_senders_retention_ms"`
}

func (s *Store) dayKeysFor(eventTime time.Time, days int) []string {
	key := eventTime.UnixMilli() / 86_400_000
	if cached, ok := s.dayKeys.Load(key); ok {
		return cached.([]string)
	}
	keys := make([]string, 0, days)
	for i := days - 1; i >= 0; i-- {
		keys = append(keys, eventTime.AddDate(0, 0, -i).Format("20060102"))
	}
	actual, _ := s.dayKeys.LoadOrStore(key, keys)
	return actual.([]string)
}

func (s *Store) hourKeysFor(eventTime time.Time) []string {
	key := eventTime.UnixMilli() / 3_600_000
	if cached, ok := s.hourKeys.Load(key); ok {
		return cached.([]string)
	}
	keys := make([]string, 0, 24)
	for i := 23; i >= 0; i-- {
		keys = append(keys, eventTime.Add(-time.Duration(i)*time.Hour).Format("2006010215"))
	}
	actual, _ := s.hourKeys.LoadOrStore(key, keys)
	return actual.([]string)
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func decodeRawJSON(data []byte) (Raw, Status, error) {
	var w rawWire
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&w); err != nil {
		return Raw{}, "", fmt.Errorf("decode feature script response: %w", err)
	}
	r := Raw{
		Status: w.Status, Capped: int(toInt(w.Capped)),
		Vel1m: toInt(w.Vel1m), Vel10m: toInt(w.Vel10m), Vel1h: toInt(w.Vel1h),
		Amount10m: toInt(w.Amount10m), Amount24h: toInt(w.Amount24h),
		DistinctRecip: toInt(w.DistinctRecip), RecipPresent: toInt(w.RecipPresent),
		SmallRun: toInt(w.SmallRun), DailyCount: toInt(w.DailyCount),
		DailySum: toInt(w.DailySum), DailySumsq: toInt(w.DailySumsq),
		BaseDevice: w.BaseDevice, BaseLat: w.BaseLat, BaseLon: w.BaseLon, BaseSIM: w.BaseSIM,
		BaseSIMChg: toInt(w.BaseSIMChg), BaseCashout: toInt(w.BaseCashout),
		BasePresent: toInt(w.BasePresent), DevAccounts: toInt(w.DevAccounts),
		DevPresent: toInt(w.DevPresent), EdgeLastMS: toInt(w.EdgeLastMS),
		SenderCounts: toInt(w.SenderCounts), SenderPresent: toInt(w.SenderPresent),
		LastInboundMS: toInt(w.LastInboundMS), LastCashoutMS: toInt(w.LastCashoutMS),
		In1h: toInt(w.In1h), Out1h: toInt(w.Out1h),
		AgentIn24: toInt(w.AgentIn24), AgentOut24: toInt(w.AgentOut24), AgentNet: toInt(w.AgentNet),
		WatermarkMS: toInt(w.WatermarkMS), SIMChanged: toInt(w.SIMChanged),
	}
	switch Status(r.Status) {
	case StatusOK:
		return r, StatusOK, nil
	case StatusDuplicate:
		return Raw{Status: r.Status}, StatusDuplicate, nil
	case StatusLate:
		return Raw{Status: r.Status, WatermarkMS: r.WatermarkMS}, StatusLate, nil
	default:
		return Raw{}, "", fmt.Errorf("feature script returned unknown status %q", r.Status)
	}
}

type rawWire struct {
	Status string `json:"status"`

	Capped        json.Number `json:"capped"`
	Vel1m         json.Number `json:"vel1m"`
	Vel10m        json.Number `json:"vel10m"`
	Vel1h         json.Number `json:"vel1h"`
	Amount10m     json.Number `json:"amt10m"`
	Amount24h     json.Number `json:"amt24h"`
	DistinctRecip json.Number `json:"distinct_recip"`
	RecipPresent  json.Number `json:"recip_present"`
	SmallRun      json.Number `json:"small_run"`
	DailyCount    json.Number `json:"daily_count"`
	DailySum      json.Number `json:"daily_sum"`
	DailySumsq    json.Number `json:"daily_sumsq"`
	BaseDevice    string      `json:"base_device"`
	BaseLat       float64     `json:"base_lat"`
	BaseLon       float64     `json:"base_lon"`
	BaseSIM       string      `json:"base_sim"`
	BaseSIMChg    json.Number `json:"base_simchg"`
	BaseCashout   json.Number `json:"base_cashout"`
	BasePresent   json.Number `json:"base_present"`
	DevAccounts   json.Number `json:"dev_accounts"`
	DevPresent    json.Number `json:"dev_present"`
	EdgeLastMS    json.Number `json:"edge_last_ms"`
	SenderCounts  json.Number `json:"sender_accounts"`
	SenderPresent json.Number `json:"sender_present"`
	LastInboundMS json.Number `json:"last_inbound_ms"`
	LastCashoutMS json.Number `json:"last_cashout_ms"`
	In1h          json.Number `json:"in1h"`
	Out1h         json.Number `json:"out1h"`
	AgentIn24     json.Number `json:"agent_in24"`
	AgentOut24    json.Number `json:"agent_out24"`
	AgentNet      json.Number `json:"agent_net"`
	WatermarkMS   json.Number `json:"watermark_ms"`
	SIMChanged    json.Number `json:"sim_changed"`
}

func toInt(n json.Number) int64 {
	if n == "" {
		return 0
	}
	if v, err := n.Int64(); err == nil {
		return v
	}
	v, _ := strconv.ParseFloat(string(n), 64)
	return int64(v)
}
