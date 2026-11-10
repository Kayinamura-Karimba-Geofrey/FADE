package features

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/config"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/events"
)

var (
	testRedisAddr string
	testConfig    config.Features
)

func TestMain(m *testing.M) {
	cfg, err := config.LoadFeatures("../../config/features.parity.yaml")
	if err != nil {
		panic(err)
	}
	testConfig = cfg

	addr := os.Getenv("FADE_TEST_REDIS_ADDR")
	cleanup := func() {}
	if addr == "" {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		container, err := rediscontainer.Run(ctx, "redis:7.4.1-alpine3.20")
		cancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "features integration tests skipped: %v\n", err)
		} else {
			cleanup = func() {
				_ = testcontainers.TerminateContainer(container)
			}
			endpoint, err := container.Endpoint(context.Background(), "")
			if err != nil {
				panic(err)
			}
			addr = endpoint
		}
	}
	testRedisAddr = addr
	code := m.Run()
	cleanup()
	os.Exit(code)
}

func testStore(t *testing.T, lateTolerance time.Duration) (*Store, *redis.Client) {
	t.Helper()
	if testRedisAddr == "" {
		t.Skip("set FADE_TEST_REDIS_ADDR or start Docker for feature integration tests")
	}
	client := redis.NewClient(&redis.Options{Addr: testRedisAddr})
	if err := client.FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("flush redis: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return NewStore(client, testConfig, lateTolerance, 24*time.Hour, Hooks{}), client
}

func ev(id string, t time.Time, sender, recipient, txn string, amount int64) *events.Event {
	e := &events.Event{
		EventID:     id,
		EventTimeMS: t.UnixMilli(),
		TxnType:     txn,
		Amount:      amount,
		Currency:    "RWF",
		Sender: events.Sender{
			AccountID:     sender,
			DeviceID:      "dev_" + sender + "_0",
			SIMSerialHash: "sim_" + sender,
			Geo:           events.Geo{Lat: -1.9, Lon: 30.0},
			KYCTier:       2,
		},
		Recipient: events.Recipient{AccountID: recipient},
	}
	return e
}

func TestComputeBasicAndDuplicate(t *testing.T) {
	store, client := testStore(t, time.Minute)
	base := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	first := ev("01KDWCY10Q768RM6TMTHZS5X82", base, "acct_000001", "acct_000002", events.P2PSend, 1000)
	v, status, err := store.Compute(context.Background(), first)
	if err != nil || status != StatusOK {
		t.Fatalf("first: status=%s err=%v", status, err)
	}
	if v.Velocity1m != 1 || v.AmountSum10m != 1000 || v.AmountSum24h != 1000 ||
		v.DistinctRecip24h != 1 || v.AccPerDevice7d != 1 || !v.RecipNewForSender ||
		v.RecipDistinctSenders24h != 1 {
		t.Fatalf("first vector: %+v", v)
	}
	if v.NearLimitRatio != 1000.0/500000.0 {
		t.Fatalf("near limit: %v", v.NearLimitRatio)
	}
	if v.InboundOutboundRatio1h != 1.0/1001.0 {
		t.Fatalf("ratio: %v", v.InboundOutboundRatio1h)
	}

	_, status, err = store.Compute(context.Background(), first)
	if err != nil || status != StatusDuplicate {
		t.Fatalf("duplicate: status=%s err=%v", status, err)
	}
	if n := client.ZCard(context.Background(), accountSendKey("acct_000001")).Val(); n != 1 {
		t.Fatalf("duplicate mutated send window: %d", n)
	}

	second := ev("01KDWD5SFGWJN64YJRXSFYSE2Q", base.Add(2*time.Minute), "acct_000001", "acct_000002", events.P2PSend, 2000)
	v, status, err = store.Compute(context.Background(), second)
	if err != nil || status != StatusOK {
		t.Fatalf("second: status=%s err=%v", status, err)
	}
	if v.Velocity1m != 1 || v.Velocity10m != 2 || v.AmountSum10m != 3000 ||
		v.AmountSum24h != 3000 || v.DistinctRecip24h != 1 || v.RecipNewForSender {
		t.Fatalf("second vector: %+v", v)
	}
}

func TestLateEventDoesNotMutate(t *testing.T) {
	store, client := testStore(t, time.Minute)
	base := time.Date(2026, 1, 1, 10, 10, 0, 0, time.UTC)
	if _, _, err := store.Compute(context.Background(), ev("01KDWD5SFGWJN64YJRXSFYSE2Q", base, "acct_000001", "acct_000002", events.P2PSend, 500)); err != nil {
		t.Fatal(err)
	}
	late := ev("01KDWCY10Q768RM6TMTHZS5X82", base.Add(-2*time.Minute), "acct_000001", "acct_000003", events.P2PSend, 900)
	_, status, err := store.Compute(context.Background(), late)
	if err != nil || status != StatusLate {
		t.Fatalf("late status=%s err=%v", status, err)
	}
	if n := client.ZCard(context.Background(), accountSendKey("acct_000001")).Val(); n != 1 {
		t.Fatalf("late mutated window: %d", n)
	}
	if client.Exists(context.Background(), dedupeKey(late.EventID)).Val() != 0 {
		t.Fatal("late event was acknowledged in dedupe")
	}

	// Exactly LATE_TOLERANCE_S old is accepted and does not move the watermark.
	acceptedLate := ev("01KDWDP9SRCWJZPKPFQ937RTRM", base.Add(-time.Minute), "acct_000001", "acct_000003", events.P2PSend, 700)
	v, status, err := store.Compute(context.Background(), acceptedLate)
	if err != nil || status != StatusOK {
		t.Fatalf("accepted late: status=%s err=%v", status, err)
	}
	if v.AmountSum24h != 700 || v.Velocity1m != 1 {
		t.Fatalf("accepted late vector: %+v", v)
	}
	if wm := client.HGet(context.Background(), accountStateKey("acct_000001"), "watermark_ms").Val(); wm != fmt.Sprint(base.UnixMilli()) {
		t.Fatalf("watermark moved: %s", wm)
	}
}

func TestDeviceSIMAndGeo(t *testing.T) {
	store, _ := testStore(t, time.Minute)
	base := time.Date(2026, 1, 2, 10, 0, 0, 0, time.UTC)
	a := ev("01KDWCY10Q768RM6TMTHZS5X82", base, "acct_000010", "acct_000011", events.P2PSend, 1000)
	if _, _, err := store.Compute(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	b := ev("01KDWD5SFGWJN64YJRXSFYSE2Q", base.Add(time.Hour), "acct_000010", "acct_000011", events.P2PSend, 1000)
	b.Sender.DeviceID = "dev_other"
	b.Sender.SIMSerialHash = "sim_other"
	b.Sender.Geo = events.Geo{Lat: -2.0, Lon: 30.1}
	v, _, err := store.Compute(context.Background(), b)
	if err != nil {
		t.Fatal(err)
	}
	if !v.NewDevice || !v.SIMChanged72h || v.TimeSinceSIMChange != 0 || v.NewGeoKm <= 0 {
		t.Fatalf("change vector: %+v", v)
	}
	c := ev("01KDWDP9SRCWJZPKPFQ937RTRM", base.Add(2*time.Hour), "acct_000010", "acct_000011", events.P2PSend, 1000)
	c.Sender.DeviceID = "dev_other"
	c.Sender.SIMSerialHash = "sim_other"
	c.Sender.Geo = b.Sender.Geo
	v, _, err = store.Compute(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if v.NewDevice || !v.SIMChanged72h || v.TimeSinceSIMChange != 60 || v.NewGeoKm != 0 {
		t.Fatalf("steady vector: %+v", v)
	}
}

func TestCycleLengths(t *testing.T) {
	store, _ := testStore(t, time.Minute)
	base := time.Date(2026, 1, 3, 10, 0, 0, 0, time.UTC)
	send := func(id, from, to string, minute int) FeatureVector {
		v, status, err := store.Compute(context.Background(), ev(id, base.Add(time.Duration(minute)*time.Minute), from, to, events.P2PSend, 100))
		if err != nil || status != StatusOK {
			t.Fatalf("cycle send: %s %v", status, err)
		}
		return v
	}
	send("01KDWCY10Q768RM6TMTHZS5X82", "acct_000020", "acct_000021", 0)
	send("01KDWD5SFGWJN64YJRXSFYSE2Q", "acct_000021", "acct_000020", 1)
	v := send("01KDWDP9SRCWJZPKPFQ937RTRM", "acct_000020", "acct_000021", 2)
	if v.CycleLen2To4 != 2 {
		t.Fatalf("2-cycle = %d", v.CycleLen2To4)
	}

	send("01KDWDRXAEFJN4A77T5PCC1D87", "acct_000030", "acct_000031", 0)
	send("01KDWEDVYYR92VW67NXRVB5PNN", "acct_000031", "acct_000032", 1)
	v = send("01KDWFGM99PCDHT8RJV275C465", "acct_000032", "acct_000030", 2)
	if v.CycleLen2To4 != 3 {
		t.Fatalf("3-cycle = %d", v.CycleLen2To4)
	}

	send("01KDWHX57GF5XSH5HCHDXMVVCS", "acct_000040", "acct_000041", 0)
	send("01KDW1P8QRDB6B1D0GD8Z738GW", "acct_000041", "acct_000042", 1)
	send("01KDW1NQ58F9H0D8G072HFM67R", "acct_000042", "acct_000043", 2)
	v = send("01KDW1HJB8ZKJGKCKEP5EJSXVV", "acct_000043", "acct_000040", 3)
	if v.CycleLen2To4 != 4 {
		t.Fatalf("4-cycle = %d", v.CycleLen2To4)
	}
}

func TestAgentAndDailyStats(t *testing.T) {
	store, _ := testStore(t, time.Minute)
	base := time.Date(2026, 1, 4, 10, 0, 0, 0, time.UTC)
	agent := "spec_agent_000"
	for i := 0; i < 5; i++ {
		e := ev(fmt.Sprintf("agent-event-%d", i), base.Add(time.Duration(i)*time.Hour), "acct_000050", agent, events.CashIn, 10000)
		e.Sender.DeviceID = "dev_agent_customer"
		e.Recipient.IsAgent = true
		e.AgentID = &agent
		e.Sender.KYCTier = 3
		v, status, err := store.Compute(context.Background(), e)
		if err != nil || status != StatusOK {
			t.Fatalf("cash-in: %s %v", status, err)
		}
		if v.Velocity1m != 0 || v.AmountSum10m != 0 {
			t.Fatalf("cash-in counted as outbound: %+v", v)
		}
		if i == 4 && (v.AgentCicoRatio24h != 50001.0/1.0 || math.Abs(v.AgentFloatDeltaPct-50) > 1e-9) {
			t.Fatalf("agent vector: ratio=%v float=%v", v.AgentCicoRatio24h, v.AgentFloatDeltaPct)
		}
	}
	for i := 0; i < 5; i++ {
		id := []string{"01KDWCY10Q768RM6TMTHZS5X82", "01KDWD5SFGWJN64YJRXSFYSE2Q",
			"01KDWDP9SRCWJZPKPFQ937RTRM", "01KDWDRXAEFJN4A77T5PCC1D87",
			"01KDWEDVYYR92VW67NXRVB5PNN"}[i]
		e := ev(id, base.Add(time.Duration(10+i)*time.Hour), "acct_000060", "acct_000061", events.P2PSend, 1000)
		v, status, err := store.Compute(context.Background(), e)
		if err != nil || status != StatusOK {
			t.Fatalf("daily: %s %v", status, err)
		}
		if i == 4 && (v.AmountVsAvg30d != 1.0 || v.AmountZScore30d != 0) {
			t.Fatalf("insufficient-history stats: %+v", v)
		}
	}
}

func TestHelperMathAndCoding(t *testing.T) {
	if got := HaversineKM(-1.9441, 30.0619, -1.9441, 30.0619, 6371.0088); got != 0 {
		t.Fatalf("zero distance = %v", got)
	}
	if got := HaversineKM(0, 0, 0, 1, 6371.0088); math.Abs(got-111.195) > 0.01 {
		t.Fatalf("one degree = %v", got)
	}
	ratio, z := AmountStats(100, 0, 0, 0, 5)
	if ratio != 1 || z != 0 {
		t.Fatalf("default stats = %v %v", ratio, z)
	}
	ratio, z = AmountStats(200, 5, 500, 0, 5)
	if ratio != 2 || z != 0 {
		t.Fatalf("zero variance = %v %v", ratio, z)
	}
	if _, z = AmountStats(200, 5, 500, 60_000, 5); z == 0 {
		t.Fatal("nonzero variance produced zero z")
	}
	if Ratio1h(2, 1) != 1.5 || CicoRatio(3, 1) != 2 {
		t.Fatal("ratio helpers")
	}
	cfg := testConfig
	if !NightHour(time.Date(2026, 1, 1, 22, 0, 0, 0, time.UTC), cfg) {
		t.Fatal("22:00 UTC is 00:00 Kigali")
	}
	if NearLimitRatio(500000, 2, cfg) != 1 {
		t.Fatal("near limit")
	}
	if NearLimitRatio(1, 99, cfg) != 0 {
		t.Fatal("unknown tier")
	}
	if boolInt(true) != 1 || boolInt(false) != 0 {
		t.Fatal("boolInt")
	}
}

func TestHooksAndJSONContract(t *testing.T) {
	var capped, redisCalls, cycles int
	h := Hooks{
		Capped: func(n int) { capped += n },
		Redis:  func(time.Duration) { redisCalls++ },
		Cycle:  func(time.Duration) { cycles++ },
	}
	h.capped(3)
	h.redis(time.Millisecond)
	h.cycle(time.Millisecond)
	if capped != 3 || redisCalls != 1 || cycles != 1 {
		t.Fatal("hooks not invoked")
	}
	v := NewVector("01KDWCY10Q768RM6TMTHZS5X82", 10, 3, testConfig.SchemaVersion)
	payload, err := v.MarshalCanonical()
	if err != nil || len(payload) == 0 {
		t.Fatalf("marshal: %v", err)
	}
	if SchemaVersion() != config.SchemaVersion {
		t.Fatal("schema version")
	}
	if v.TimeSinceSIMChange != -1 || v.RecipCashoutLagMin != -1 || v.AmountVsAvg30d != 1 {
		t.Fatalf("vector defaults: %+v", v)
	}
}

func TestDecodeRawJSON(t *testing.T) {
	if _, status, err := decodeRawJSON([]byte(`{"status":"duplicate"}`)); err != nil || status != StatusDuplicate {
		t.Fatalf("duplicate decode: %s %v", status, err)
	}
	raw, status, err := decodeRawJSON([]byte(`{"status":"late","watermark_ms":123}`))
	if err != nil || status != StatusLate || raw.WatermarkMS != 123 {
		t.Fatalf("late decode: %+v %s %v", raw, status, err)
	}
	raw, status, err = decodeRawJSON([]byte(`{"status":"ok","base_device":"dev","base_lat":-1.5,"base_lon":30.25,"sim_changed":1,"in1h":42}`))
	if err != nil || status != StatusOK || raw.BaseDevice != "dev" || raw.BaseLat != -1.5 ||
		raw.BaseLon != 30.25 || raw.SIMChanged != 1 || raw.In1h != 42 {
		t.Fatalf("ok decode: %+v %s %v", raw, status, err)
	}
	if _, _, err := decodeRawJSON([]byte(`{"status":"weird"}`)); err == nil {
		t.Fatal("unknown status accepted")
	}
	if _, _, err := decodeRawJSON([]byte(`{`)); err == nil {
		t.Fatal("bad json accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gotCtx, gotCancel := ensureDeadline(ctx)
	if _, ok := gotCtx.Deadline(); !ok {
		t.Fatal("ensureDeadline did not add a deadline")
	}
	gotCancel()
	ctx2, cancel2 := context.WithDeadline(ctx, time.Now().Add(time.Hour))
	defer cancel2()
	got, stop := ensureDeadline(ctx2)
	stop()
	if got != ctx2 {
		t.Fatal("existing deadline replaced")
	}
}

func TestRedisKeyLayout(t *testing.T) {
	cases := map[string]string{
		dedupeKey("e"):           "dedupe:e",
		accountStateKey("a"):     "s:acc:a",
		accountHistoryKey("a"):   "w:acc:a:hist",
		accountSendKey("a"):      "w:acc:a:send",
		accountInKey("a"):        "w:acc:a:in",
		accountRecipKey("a"):     "w:acc:a:recip",
		accountDailyKey("a"):     "s:acc:a:daily",
		deviceKey("d"):           "w:dev:d",
		recipientSendersKey("r"): "w:recip:r:senders",
		recipientInKey("r"):      "w:recip:r:in",
		recipientSendKey("r"):    "w:recip:r:send",
		edgeKey("a", "b"):        "g:edge:a:b",
		outEdgesKey("a"):         "g:out:a",
		agentStateKey("g"):       "a:agent:g",
		agentHourlyKey("g"):      "a:agent:g:hr",
		cycleCacheKey("a", "b"):  "cyc:a:b",
	}
	for got, want := range cases {
		if got != want {
			t.Fatalf("key = %q want %q", got, want)
		}
	}
	if toInt("") != 0 || toInt(json.Number("")) != 0 ||
		toInt(json.Number("1.1458642505e+14")) != 114586425050000 {
		t.Fatal("toInt conversions")
	}
}

func TestCashOutAndBankTransferAgentBranches(t *testing.T) {
	store, _ := testStore(t, time.Minute)
	base := time.Date(2026, 1, 5, 10, 0, 0, 0, time.UTC)
	agent := "spec_agent_000"
	out := ev("01M6ZW06807TFTK1P7CPF6Q3AT", base, "acct_000070", agent, events.CashOut, 5000)
	out.Recipient.IsAgent = true
	out.AgentID = &agent
	out.Sender.KYCTier = 1
	v, status, err := store.Compute(context.Background(), out)
	if err != nil || status != StatusOK {
		t.Fatalf("cash-out: %s %v", status, err)
	}
	if v.AgentCicoRatio24h != 1.0/5001.0 || v.AgentFloatDeltaPct != -5 {
		t.Fatalf("cash-out agent vector: %+v", v)
	}
	in := ev("01M6ZW9B70CRE2PH59XBCW76TN", base.Add(time.Hour), "acct_000071", agent, events.BankTransfer, 10000)
	in.Recipient.IsAgent = true
	in.Channel = events.ChannelAPI
	v, status, err = store.Compute(context.Background(), in)
	if err != nil || status != StatusOK {
		t.Fatalf("bank transfer: %s %v", status, err)
	}
	if math.Abs(v.AgentFloatDeltaPct-5) > 1e-9 {
		t.Fatalf("bank transfer agent float: %+v", v)
	}
}
