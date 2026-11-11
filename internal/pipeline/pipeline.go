package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/events"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/features"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/obs"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
)

// Deps are the pipeline collaborators.
type Deps struct {
	Rules    *rules.Manager
	Features FeatureComputer
	Scorer   rules.Scorer
	Idem     Idempotency
	Fallback Idempotency
	Lists    *ListCache
	Sink     DecisionSink
	Store    Store
	Metrics  *obs.Metrics
	Logger   *slog.Logger
	Config   Config
}

// Pipeline evaluates transactions and produces rules-only decisions.
type Pipeline struct {
	rules    *rules.Manager
	features FeatureComputer
	scorer   rules.Scorer
	idem     Idempotency
	fallback Idempotency
	lists    *ListCache
	sink     DecisionSink
	store    Store
	metrics  *obs.Metrics
	logger   *slog.Logger
	cfg      Config
	sem      chan struct{}
	now      func() time.Time
	pool     sync.Pool
}

// New wires a pipeline with safe defaults for optional collaborators.
func New(d Deps) *Pipeline {
	if d.Config.RequestBudget == 0 {
		d.Config = DefaultConfig()
	}
	if d.Scorer == nil {
		d.Scorer = rules.NoopScorer{}
	}
	if d.Idem == nil {
		d.Idem = NewMemoryIdempotency()
	}
	if d.Fallback == nil {
		d.Fallback = NewMemoryIdempotency()
	}
	if d.Lists == nil {
		d.Lists = NewListCache(NewMemoryListSource(), d.Logger)
	}
	if d.Sink == nil {
		d.Sink = DiscardSink{}
	}
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	semSize := d.Config.MaxConcurrency
	if semSize <= 0 {
		semSize = 2000
	}
	p := &Pipeline{
		rules: d.Rules, features: d.Features, scorer: d.Scorer, idem: d.Idem,
		fallback: d.Fallback, lists: d.Lists, sink: d.Sink, store: d.Store,
		metrics: d.Metrics, logger: d.Logger, cfg: d.Config,
		sem: make(chan struct{}, semSize), now: time.Now,
	}
	p.pool.New = func() any { return new(rules.Result) }
	return p
}

// Rules exposes the rule manager.
func (p *Pipeline) Rules() *rules.Manager { return p.rules }

// Lists exposes the list cache.
func (p *Pipeline) Lists() *ListCache { return p.lists }

// Store exposes the decision store.
func (p *Pipeline) Store() Store { return p.store }

// Config returns the pipeline configuration.
func (p *Pipeline) Config() Config { return p.cfg }

// Decide evaluates one event. It never waits on Postgres; the decision sink
// batches and writes asynchronously.
func (p *Pipeline) Decide(ctx context.Context, ev *events.Event, opts Options) (out *Decision, err error) {
	start := p.now()

	select {
	case p.sem <- struct{}{}:
		defer func() { <-p.sem }()
	default:
		if p.metrics != nil {
			p.metrics.Shed.Inc()
		}
		return nil, ErrShed
	}

	if _, ok := ctx.Deadline(); !ok && p.cfg.RequestBudget > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.cfg.RequestBudget)
		defer cancel()
	}

	defer func() {
		if rec := recover(); rec != nil {
			id := ""
			if ev != nil {
				id = ev.EventID
			}
			p.logger.Error("decision panic recovered", "panic", rec, "event_id", id)
			if ev != nil {
				out = p.degraded(ctx, ev, CauseRulePanic, opts, rules.EmptyRuleSet())
			} else {
				out = nil
			}
			err = nil
		}
		if out != nil {
			out.LatencyUS = p.now().Sub(start).Microseconds()
			out.LatencyMS = float64(out.LatencyUS) / 1000
			if p.metrics != nil {
				p.metrics.ObserveStage("total", p.now().Sub(start).Seconds())
				p.metrics.Decisions.WithLabelValues(string(out.Decision), boolLabel(out.Degraded)).Inc()
			}
		}
	}()

	if ev == nil || ev.EventID == "" || ev.TxnType == "" || ev.Amount <= 0 {
		return nil, &Error{Kind: "INVALID_EVENT", Status: http.StatusBadRequest, Message: "invalid event"}
	}
	if p.features == nil {
		return nil, &Error{Kind: KindInternal, Status: 500, Message: "feature computer is not configured"}
	}

	stage := p.now()
	p.metrics.ObserveStage("validate", p.now().Sub(stage).Seconds())

	// ---------------------------------------------------------- idempotency
	stage = p.now()
	key := opts.IdempotencyKey
	if key == "" {
		key = ev.EventID
	}
	hash := payloadHash(ev)
	owner := false
	redisDown := false
	idem := p.idem
	if idem != nil {
		stored, isOwner, ierr := idem.Begin(ctx, key, hash)
		switch {
		case ierr == nil && !isOwner:
			p.replayCount()
			dec, derr := decodeDecision(stored)
			if derr != nil {
				return nil, &Error{Kind: KindInternal, Status: 500, Message: "decode stored decision", Err: derr}
			}
			dec.Replay = true
			p.metrics.ObserveStage("idem", p.now().Sub(stage).Seconds())
			return dec, nil
		case errors.Is(ierr, ErrIdempotencyConflict):
			p.metrics.ObserveStage("idem", p.now().Sub(stage).Seconds())
			return nil, ErrIdempotencyConflict
		case errors.Is(ierr, ErrIdempotencyInFlight):
			p.metrics.ObserveStage("idem", p.now().Sub(stage).Seconds())
			return nil, ErrIdempotencyInFlight
		case ierr != nil:
			redisDown = true
			idem = p.fallback
			stored, isOwner, ierr := idem.Begin(ctx, key, hash)
			if ierr != nil {
				p.logger.Warn("fallback idempotency failed", "error", ierr)
			} else if !isOwner {
				p.replayCount()
				if dec, derr := decodeDecision(stored); derr == nil {
					dec.Replay = true
					p.metrics.ObserveStage("idem", p.now().Sub(stage).Seconds())
					return dec, nil
				}
			} else {
				owner = true
			}
		default:
			owner = isOwner
		}
	} else {
		owner = true
	}
	p.metrics.ObserveStage("idem", p.now().Sub(stage).Seconds())

	complete := func(dec *Decision) {
		if !owner || idem == nil {
			return
		}
		raw, merr := json.Marshal(dec)
		if merr != nil {
			return
		}
		if cerr := idem.Complete(ctx, key, hash, raw, p.cfg.IdempotencyTTL); cerr != nil {
			p.logger.Warn("idempotency completion failed", "error", cerr)
		}
	}

	// -------------------------------------------------------------- features
	if redisDown {
		dec := p.degraded(ctx, ev, CauseRedis, opts, p.currentRuleSet())
		complete(dec)
		p.sink.Enqueue(dec.ToStored())
		return dec, nil
	}

	stage = p.now()
	fctx, cancel := context.WithTimeout(ctx, p.cfg.FeatureTimeout)
	vector, status, ferr := p.features.Compute(fctx, ev)
	cancel()
	p.metrics.ObserveStage("features", p.now().Sub(stage).Seconds())
	if ferr != nil {
		cause := CauseRedis
		if errors.Is(ferr, context.DeadlineExceeded) || errors.Is(fctx.Err(), context.DeadlineExceeded) {
			cause = CauseFeatureTimeout
		}
		dec := p.degraded(ctx, ev, cause, opts, p.currentRuleSet())
		complete(dec)
		p.sink.Enqueue(dec.ToStored())
		return dec, nil
	}
	switch status {
	case features.StatusDuplicate:
		if dec := p.storedDecision(ctx, ev.EventID); dec != nil {
			dec.Replay = true
			p.replayCount()
			complete(dec)
			return dec, nil
		}
		return nil, &Error{Kind: "DUPLICATE_EVENT", Status: http.StatusConflict,
			Message: "event_id was already decided and no stored decision is available"}
	case features.StatusLate:
		return nil, &Error{Kind: "EVENT_LATE", Status: http.StatusUnprocessableEntity,
			Message: "event is older than the late-tolerance watermark"}
	case features.StatusOK:
	default:
		return nil, &Error{Kind: KindInternal, Status: 500, Message: "unknown feature status"}
	}

	rs := p.currentRuleSet()
	if rs.Empty {
		dec := p.degraded(ctx, ev, CauseNoRuleSet, opts, rs)
		if opts.IncludeFeatures {
			dec.Features = &vector
		}
		complete(dec)
		p.sink.Enqueue(dec.ToStored())
		return dec, nil
	}

	// ----------------------------------------------------------------- rules
	stage = p.now()
	evalCtx := rules.EvalContextFor(&vector, ev.Amount, ev.Fee, ev.TxnType, ev.Channel,
		int64(ev.Sender.KYCTier), ev.Sender.BalanceBefore)
	res := p.pool.Get().(*rules.Result)
	defer p.pool.Put(res)
	evalErr := rs.EvaluateSafe(&vector, evalCtx, res)
	if p.rules != nil {
		p.rules.RecordEval(evalErr)
	}
	p.metrics.ObserveStage("rules", p.now().Sub(stage).Seconds())
	if evalErr != nil {
		p.logger.Error("rule evaluation panicked", "event_id", ev.EventID, "rule_set", rs.Version)
		dec := p.degraded(ctx, ev, CauseRulePanic, opts, rs)
		complete(dec)
		p.sink.Enqueue(dec.ToStored())
		return dec, nil
	}

	// ------------------------------------------------------------- combine
	stage = p.now()
	ml := rules.MLResult{Status: rules.MLNotConfigured}
	if p.scorer != nil {
		if result, serr := p.scorer.Score(ctx, &vector); serr == nil {
			ml = result
		} else {
			ml = rules.MLResult{Status: rules.MLError}
			p.logger.Warn("ml scoring failed; rules-only decision", "error", serr)
		}
	}
	risk, decision := rules.Combine(rs, res, ml)

	countHits := func() {
		for i := 0; i < res.FiredCount; i++ {
			if p.metrics != nil {
				p.metrics.RuleHits.WithLabelValues(res.Fired[i].Rule.ID, string(rules.Enforce)).Inc()
			}
		}
		for i := 0; i < res.ShadowCount; i++ {
			if p.metrics != nil {
				p.metrics.RuleHits.WithLabelValues(res.Shadow[i], string(rules.Shadow)).Inc()
			}
		}
	}
	countHits()

	// --------------------------------------------------- lists and tier limit
	stage = p.now()
	blocked, allowed := p.lists.Check(ev.Sender.AccountID, ev.Sender.DeviceID, ev.Sender.MSISDNHash)
	reasons := make([]Reason, 0, res.FiredCount+2)
	explain := opts.IncludeExplain || decision.Severity() >= rules.StepUp.Severity()
	if explain {
		for i := 0; i < res.FiredCount; i++ {
			r := res.Fired[i].Rule
			reasons = append(reasons, Reason{
				Source: "rule", ID: r.ID, Text: r.Reason.Render(&evalCtx),
				Weight: r.Weight, Mode: r.Mode,
			})
		}
	}
	if blocked != nil {
		decision = rules.Block
		if risk < 85 {
			risk = 85
		}
		reasons = append(reasons, Reason{Source: "list", ID: "LIST_BLOCK",
			Text: "subject on BLOCK list (" + blocked.Subject + ")"})
	} else if decision != rules.Block && allowed != nil {
		decision = rules.Allow
		reasons = append(reasons, Reason{Source: "list", ID: "LIST_ALLOW",
			Text: "subject on ALLOW list (" + allowed.Subject + ")"})
	} else if limit := p.cfg.TierLimit(int64(ev.Sender.KYCTier)); limit > 0 && ev.Amount > limit {
		if decision.Severity() < rules.Hold.Severity() {
			decision = rules.Hold
		}
		if risk < 65 {
			risk = 65
		}
		reasons = append(reasons, Reason{Source: "tier", ID: "TIER_LIMIT_EXCEEDED",
			Text: "amount above the daily KYC tier limit"})
	}
	p.metrics.ObserveStage("overrides", p.now().Sub(stage).Seconds())
	p.metrics.ObserveStage("combine", p.now().Sub(stage).Seconds())

	shadow := make([]string, res.ShadowCount)
	copy(shadow, res.Shadow[:res.ShadowCount])
	now := p.now()
	dec := &Decision{
		DecisionID:     NewDecisionID(now),
		EventID:        ev.EventID,
		Decision:       decision,
		RiskScore:      clamp(risk),
		RuleScore:      res.RuleScore,
		MLScore:        ml.Score,
		MLStatus:       ml.Status,
		Degraded:       false,
		RuleSetVersion: rs.Version,
		ModelVersion:   modelVersion(ml.ModelVersion),
		Reasons:        reasons,
		ShadowHits:     shadow,
		DecidedAt:      now.UTC(),
		AccountID:      ev.Sender.AccountID,
		RecipientID:    ev.Recipient.AccountID,
		Amount:         ev.Amount,
		TxnType:        ev.TxnType,
		EventTime:      time.UnixMilli(ev.EventTimeMS).UTC(),
	}
	if opts.IncludeFeatures {
		vec := vector
		dec.Features = &vec
	}
	complete(dec)
	p.sink.Enqueue(dec.ToStored())
	return dec, nil
}

func (p *Pipeline) currentRuleSet() *rules.RuleSet {
	if p.rules == nil {
		return rules.EmptyRuleSet()
	}
	return p.rules.Current()
}

func (p *Pipeline) storedDecision(ctx context.Context, eventID string) *Decision {
	if p.store == nil {
		return nil
	}
	rec, err := p.store.DecisionByEventID(ctx, eventID)
	if err != nil || rec == nil || rec.Decision == nil {
		return nil
	}
	return rec.Decision.Clone()
}

// degraded builds a fail-policy decision. Stateless checks (lists, tier limit
// and rules flagged stateless) still run.
func (p *Pipeline) degraded(ctx context.Context, ev *events.Event, cause string, opts Options, rs *rules.RuleSet) *Decision {
	if rs == nil {
		rs = rules.EmptyRuleSet()
	}
	mode := "open"
	if !rs.Empty {
		if m, ok := rs.FailMode[ev.TxnType]; ok {
			mode = m
		} else if m, ok := rs.FailMode["default"]; ok {
			mode = m
		} else {
			mode = rules.FailStepUp
		}
	}
	decision := rules.Allow
	switch mode {
	case rules.FailClosed:
		decision = rules.Block
	case rules.FailStepUp:
		decision = rules.StepUp
	}
	if p.metrics != nil {
		p.metrics.FailPolicy.WithLabelValues(cause, mode).Inc()
	}

	vector := features.FeatureVector{}
	evalCtx := rules.EvalContextFor(&vector, ev.Amount, ev.Fee, ev.TxnType, ev.Channel,
		int64(ev.Sender.KYCTier), ev.Sender.BalanceBefore)
	score := 0
	action := rules.Allow
	reasons := []Reason{{Source: "fail_policy", ID: "FAIL_POLICY_" + cause,
		Text: "decision degraded: " + cause}}
	for _, r := range rs.Rules() {
		if !r.Stateless || !r.Match(&evalCtx) {
			continue
		}
		if r.Mode == rules.Shadow {
			continue
		}
		score += r.Weight
		action = rules.MaxDecision(action, r.Action)
		reasons = append(reasons, Reason{Source: "rule", ID: r.ID,
			Text: r.Reason.Render(&evalCtx), Weight: r.Weight, Mode: r.Mode})
	}
	if score > 100 {
		score = 100
	}
	risk, combined := rules.Combine(rs, &rules.Result{RuleScore: score, RuleAction: action}, rules.MLResult{Status: rules.MLNotConfigured})
	decision = rules.MaxDecision(decision, combined)

	blocked, allowed := p.lists.Check(ev.Sender.AccountID, ev.Sender.DeviceID, ev.Sender.MSISDNHash)
	if blocked != nil {
		decision = rules.Block
		risk = 100
		reasons = append(reasons, Reason{Source: "list", ID: "LIST_BLOCK",
			Text: "subject on BLOCK list (" + blocked.Subject + ")"})
	} else if decision != rules.Block && allowed != nil {
		decision = rules.Allow
		reasons = append(reasons, Reason{Source: "list", ID: "LIST_ALLOW",
			Text: "subject on ALLOW list (" + allowed.Subject + ")"})
	} else if limit := p.cfg.TierLimit(int64(ev.Sender.KYCTier)); limit > 0 && ev.Amount > limit {
		if decision.Severity() < rules.Hold.Severity() {
			decision = rules.Hold
		}
		if risk < 65 {
			risk = 65
		}
		reasons = append(reasons, Reason{Source: "tier", ID: "TIER_LIMIT_EXCEEDED",
			Text: "amount above the daily KYC tier limit"})
	}

	now := p.now()
	dec := &Decision{
		DecisionID: NewDecisionID(now), EventID: ev.EventID, Decision: decision,
		RiskScore: clamp(risk), RuleScore: score, MLScore: nil, MLStatus: rules.MLNotConfigured,
		Degraded: true, RuleSetVersion: rs.Version, ModelVersion: nil,
		Reasons: reasons, ShadowHits: []string{}, DecidedAt: now.UTC(),
		AccountID: ev.Sender.AccountID, RecipientID: ev.Recipient.AccountID,
		Amount: ev.Amount, TxnType: ev.TxnType, EventTime: time.UnixMilli(ev.EventTimeMS).UTC(),
	}
	if opts.IncludeFeatures {
		dec.Features = &vector
	}
	return dec
}

func decodeDecision(raw []byte) (*Decision, error) {
	var d Decision
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, err
	}
	return &d, nil
}

func payloadHash(ev *events.Event) string {
	raw, err := json.Marshal(ev)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func (p *Pipeline) replayCount() {
	if p.metrics != nil {
		p.metrics.IdempotentReplays.Inc()
	}
}

func boolLabel(v bool) string {
	if v {
		return "true"
	}
	return "false"
}

func clamp(v int) int {
	if v < 0 {
		return 0
	}
	if v > 100 {
		return 100
	}
	return v
}

func modelVersion(v string) *string {
	if v == "" {
		return nil
	}
	return &v
}
