package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"
	redpandacontainer "github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/config"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/events"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/features"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/obs"
)

var (
	testRedisAddr string
	testBrokers   string
	testConfig    config.Features
)

func TestMain(m *testing.M) {
	cfg, err := config.LoadFeatures("../../config/features.parity.yaml")
	if err != nil {
		panic(err)
	}
	testConfig = cfg

	var cleanups []func()
	testRedisAddr = os.Getenv("FADE_TEST_REDIS_ADDR")
	if testRedisAddr == "" {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		container, err := rediscontainer.Run(ctx, "redis:7.4.1-alpine3.20")
		cancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "ingest redis tests skipped: %v\n", err)
		} else {
			cleanups = append(cleanups, func() { _ = testcontainers.TerminateContainer(container) })
			endpoint, err := container.Endpoint(context.Background(), "")
			if err != nil {
				panic(err)
			}
			testRedisAddr = endpoint
		}
	}
	testBrokers = os.Getenv("FADE_TEST_KAFKA_BROKERS")
	if testBrokers == "" {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		container, err := redpandacontainer.Run(ctx, "redpandadata/redpanda:v24.3.4")
		cancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "ingest kafka tests skipped: %v\n", err)
		} else {
			cleanups = append(cleanups, func() { _ = testcontainers.TerminateContainer(container) })
			broker, err := container.KafkaSeedBroker(context.Background())
			if err != nil {
				panic(err)
			}
			testBrokers = broker
		}
	}
	code := m.Run()
	for _, cleanup := range cleanups {
		cleanup()
	}
	os.Exit(code)
}

type capturePublisher struct {
	records []published
	err     error
}

type published struct {
	topic   string
	key     string
	value   []byte
	headers []kgo.RecordHeader
}

func (p *capturePublisher) Publish(_ context.Context, topic string, key, value []byte, headers []kgo.RecordHeader) error {
	if p.err != nil {
		return p.err
	}
	p.records = append(p.records, published{topic: topic, key: string(key), value: append([]byte(nil), value...), headers: headers})
	return nil
}

func eventJSON(id, eventTime, sender, recipient string, amount int64, txn string) []byte {
	fee := events.FeeForAmount(amount)
	ev := map[string]any{
		"event_id": id, "event_time": eventTime, "txn_type": txn, "amount": amount,
		"currency": "RWF", "fee": fee,
		"sender": map[string]any{
			"account_id": sender, "msisdn_hash": "h_72f288c3b5d3bf73876cf9477decd11b",
			"device_id": "dev_001160_0", "sim_serial_hash": "sim_33118b02606d997bc86e39011659742c",
			"geo":              map[string]any{"lat": -1.51, "lon": 29.654, "cell_id": "MSZ-04"},
			"account_age_days": 648, "kyc_tier": 2, "balance_before": 298700,
		},
		"recipient": map[string]any{
			"account_id": recipient, "msisdn_hash": "h_1349fa8dbd76f209b7d64891e86ff70c",
			"is_agent": false, "first_seen_by_sender": true,
		},
		"channel": "USSD", "agent_id": nil, "session_id": "top-VEN9RZED",
	}
	raw, _ := json.Marshal(ev)
	return raw
}

func newProcessor(t *testing.T, publisher Publisher, outputTopic string) (*Processor, *Metrics, *features.Store, *redis.Client) {
	t.Helper()
	if testRedisAddr == "" {
		t.Skip("set FADE_TEST_REDIS_ADDR or start Docker")
	}
	rdb := redis.NewClient(&redis.Options{Addr: testRedisAddr})
	if err := rdb.FlushDB(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	metrics := NewMetrics(prometheus.NewRegistry())
	store := features.NewStore(rdb, testConfig, time.Minute, 24*time.Hour, features.Hooks{})
	logger := obs.NewLogger("error")
	return NewProcessor(store, publisher, metrics, logger, "txn.dead", outputTopic, "0.3.0"), metrics, store, rdb
}

func TestProcessorValidDuplicateAndLate(t *testing.T) {
	pub := &capturePublisher{}
	processor, metrics, store, _ := newProcessor(t, pub, "features.out")
	valid := eventJSON("01M6ZW06807TFTK1P7CPF6Q3AT", "2026-11-09T10:00:00.000Z", "acct_000001", "acct_000002", 5000, "P2P_SEND")
	rec := &kgo.Record{Topic: "txn.raw", Partition: 0, Offset: 1, Value: valid, Timestamp: time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)}
	if err := processor.Handle(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	if len(pub.records) != 1 || pub.records[0].topic != "features.out" {
		t.Fatalf("feature output: %+v", pub.records)
	}
	if pub.records[0].key != "acct_000001" || len(pub.records[0].headers) != 2 {
		t.Fatalf("feature record: %+v", pub.records[0])
	}
	if testutil.ToFloat64(metrics.Events.WithLabelValues("ok")) != 1 {
		t.Fatal("ok metric")
	}

	rec.Offset = 2
	if err := processor.Handle(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	if len(pub.records) != 1 || testutil.ToFloat64(metrics.Events.WithLabelValues("duplicate")) != 1 {
		t.Fatal("duplicate handling")
	}

	late := eventJSON("01M6ZW9B70CRE2PH59XBCW76TN", "2026-11-09T10:05:00.000Z", "acct_000001", "acct_000003", 900, "P2P_SEND")
	// Move the account watermark beyond the event by processing a newer event.
	newer := eventJSON("01M6ZWJG607JTAB7X0D2AHXGPW", "2026-11-09T10:10:00.000Z", "acct_000001", "acct_000004", 900, "P2P_SEND")
	if _, _, err := store.Compute(context.Background(), mustParse(t, newer)); err != nil {
		t.Fatal(err)
	}
	lateRec := &kgo.Record{Topic: "txn.raw", Partition: 0, Offset: 3, Value: late, Timestamp: time.Date(2026, 11, 9, 10, 5, 0, 0, time.UTC)}
	if err := processor.Handle(context.Background(), lateRec); err != nil {
		t.Fatal(err)
	}
	if testutil.ToFloat64(metrics.Events.WithLabelValues("late")) != 1 {
		t.Fatal("late metric")
	}
	found := false
	for _, record := range pub.records {
		if record.topic == "txn.dead" {
			var payload DLQPayload
			if err := json.Unmarshal(record.value, &payload); err != nil {
				t.Fatal(err)
			}
			if payload.Reason != string(events.ReasonLate) || payload.EventID != "01M6ZW9B70CRE2PH59XBCW76TN" ||
				payload.SourceOffset != 3 || !strings.HasPrefix(payload.Raw, "eyJ") {
				t.Fatalf("late DLQ: %+v", payload)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("late event not dead-lettered")
	}
}

func mustParse(t *testing.T, raw []byte) *events.Event {
	t.Helper()
	ev, verr := events.Parse(raw, time.Time{})
	if verr != nil {
		t.Fatal(verr)
	}
	return ev
}

func TestProcessorInvalidJSONAndPublishFailure(t *testing.T) {
	pub := &capturePublisher{}
	processor, metrics, _, _ := newProcessor(t, pub, "")
	rec := &kgo.Record{Topic: "txn.raw", Partition: 1, Offset: 9, Value: []byte("{not-json"), Timestamp: time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)}
	if err := processor.Handle(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	if testutil.ToFloat64(metrics.ParseFailures) != 1 ||
		testutil.ToFloat64(metrics.Events.WithLabelValues("invalid")) != 1 {
		t.Fatal("parse metrics")
	}
	if len(pub.records) != 1 || pub.records[0].topic != "txn.dead" {
		t.Fatalf("invalid DLQ: %+v", pub.records)
	}
	var payload DLQPayload
	if err := json.Unmarshal(pub.records[0].value, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Reason != string(events.ReasonInvalidJSON) || payload.EventID != "" {
		t.Fatalf("invalid payload: %+v", payload)
	}

	failing := &capturePublisher{err: fmt.Errorf("broker unavailable")}
	processor, metrics, _, _ = newProcessor(t, failing, "features.out")
	valid := eventJSON("01M6ZW06807TFTK1P7CPF6Q3AT", "2026-11-09T10:00:00.000Z", "acct_000001", "acct_000002", 5000, "P2P_SEND")
	if err := processor.Handle(context.Background(), recWithValue(valid)); err == nil {
		t.Fatal("publish failure was swallowed")
	}
	if testutil.ToFloat64(metrics.Events.WithLabelValues("error")) != 1 {
		t.Fatal("error metric")
	}
}

func recWithValue(value []byte) *kgo.Record {
	return &kgo.Record{Topic: "txn.raw", Partition: 0, Offset: 1, Value: value, Timestamp: time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)}
}

func TestConsumerRetryAndPing(t *testing.T) {
	failing := &capturePublisher{err: fmt.Errorf("broker unavailable")}
	processor, metrics, _, _ := newProcessor(t, failing, "features.out")
	client, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	consumer := NewConsumer(client, processor, metrics, obs.NewLogger("error"), 2, 4)
	if err := consumer.Ping(context.Background()); err == nil {
		t.Fatal("ping before Run succeeded")
	}
	raw := eventJSON("01M6ZW06807TFTK1P7CPF6Q3AT", "2026-11-09T10:00:00.000Z", "acct_000001", "acct_000002", 5000, "P2P_SEND")
	records := []*kgo.Record{
		{Topic: "txn.raw", Partition: 0, Offset: 1, Key: []byte("acct_000001"), Value: raw, Timestamp: time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)},
		{Topic: "txn.raw", Partition: 0, Offset: 2, Key: []byte("acct_000002"), Value: raw, Timestamp: time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	if err := consumer.processPartition(ctx, records); err == nil {
		t.Fatal("retry loop returned nil")
	}
	if testutil.ToFloat64(metrics.Retries) == 0 {
		t.Fatal("retry metric not incremented")
	}
	if keyShard(nil, 1) != 0 || keyShard([]byte("x"), 3) != keyShard([]byte("x"), 3) {
		t.Fatal("shard")
	}
}

func TestConsumerRunCommitsAndMetrics(t *testing.T) {
	if testBrokers == "" {
		t.Skip("set FADE_TEST_KAFKA_BROKERS or start Docker")
	}
	pub := &capturePublisher{}
	processor, metrics, _, rdb := newProcessor(t, pub, "")
	topic := fmt.Sprintf("txn.raw.%d", time.Now().UnixNano())
	cfg := &config.Config{
		KafkaBrokers: []string{testBrokers}, KafkaTopic: topic,
		KafkaGroupID:  fmt.Sprintf("fade-test-%d", time.Now().UnixNano()),
		KafkaDLQTopic: "txn.dead", Features: testConfig,
		LateTolerance: time.Minute, DedupeTTL: 24 * time.Hour,
	}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	consumer := NewConsumer(client, processor, metrics, obs.NewLogger("error"), 1, 8)
	defer consumer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	producer, err := kgo.NewClient(kgo.SeedBrokers(testBrokers), kgo.AllowAutoTopicCreation())
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	if _, err := kadm.NewClient(producer).CreateTopic(ctx, 1, 1, nil, topic); err != nil {
		t.Fatal(err)
	}
	runErr := make(chan error, 1)
	go func() { runErr <- consumer.Run(ctx) }()
	records := []*kgo.Record{
		{Key: []byte("acct_000001"), Value: eventJSON("01M6ZW06807TFTK1P7CPF6Q3AT", "2026-11-09T10:00:00.000Z", "acct_000001", "acct_000002", 5000, "P2P_SEND")},
		{Key: []byte("acct_000001"), Value: eventJSON("01M6ZW06807TFTK1P7CPF6Q3AT", "2026-11-09T10:00:00.000Z", "acct_000001", "acct_000002", 5000, "P2P_SEND")},
		{Key: []byte("acct_000001"), Value: eventJSON("01M6ZWJG607JTAB7X0D2AHXGPW", "2026-11-09T10:10:00.000Z", "acct_000001", "acct_000003", 900, "P2P_SEND")},
	}
	eventTimes := []time.Time{
		time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC),
		time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC),
		time.Date(2026, 11, 9, 10, 5, 0, 0, time.UTC),
	}
	for i, record := range records {
		record.Topic = topic
		record.Timestamp = eventTimes[i]
	}
	if err := producer.ProduceSync(ctx, records...).FirstErr(); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		done := testutil.ToFloat64(metrics.Events.WithLabelValues("ok")) +
			testutil.ToFloat64(metrics.Events.WithLabelValues("duplicate"))
		if done >= 3 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := consumer.Ping(context.Background()); err != nil {
		t.Fatalf("ping after start: %v", err)
	}
	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop")
	}
	if testutil.ToFloat64(metrics.Commits.WithLabelValues("ok")) == 0 {
		t.Fatal("no successful commit")
	}
	if testutil.ToFloat64(metrics.Events.WithLabelValues("ok")) < 2 ||
		testutil.ToFloat64(metrics.Events.WithLabelValues("duplicate")) < 1 {
		t.Fatalf("status metrics: ok=%v dup=%v",
			testutil.ToFloat64(metrics.Events.WithLabelValues("ok")),
			testutil.ToFloat64(metrics.Events.WithLabelValues("duplicate")))
	}
	if n := rdb.Exists(context.Background(), "dedupe:01M6ZW06807TFTK1P7CPF6Q3AT").Val(); n != 1 {
		t.Fatal("dedupe key missing")
	}
}

func TestMetricsRegistered(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	if m == nil {
		t.Fatal("nil metrics")
	}
	// Touch every collector so a duplicate registration or nil collector panics.
	m.Events.WithLabelValues("ok").Inc()
	m.Processing.Observe(0.001)
	m.WindowCapped.Add(1)
	m.ConsumerLag.Set(2)
	m.DeadLettered.WithLabelValues("LATE").Inc()
	m.Commits.WithLabelValues("ok").Inc()
	m.FeatureOutput.Inc()
	m.RedisScripts.WithLabelValues("ok").Inc()
	m.RedisDuration.WithLabelValues("script").Observe(0.001)
	m.CycleDuration.Observe(0.001)
	m.DLQDuration.Observe(0.001)
	m.Retries.Inc()
	m.ParseFailures.Inc()
}

func TestKgoPublisherRoundTrip(t *testing.T) {
	if testBrokers == "" {
		t.Skip("set FADE_TEST_KAFKA_BROKERS or start Docker")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	topic := fmt.Sprintf("features.out.%d", time.Now().UnixNano())
	producer, err := kgo.NewClient(kgo.SeedBrokers(testBrokers))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	if _, err := kadm.NewClient(producer).CreateTopic(ctx, 1, 1, nil, topic); err != nil {
		t.Fatal(err)
	}
	headers := []kgo.RecordHeader{{Key: "event_id", Value: []byte("01M6ZW06807TFTK1P7CPF6Q3AT")}}
	if err := NewKgoPublisher(producer).Publish(ctx, topic, []byte("acct_000001"), []byte(`{"x":1}`), headers); err != nil {
		t.Fatal(err)
	}
	consumer, err := kgo.NewClient(kgo.SeedBrokers(testBrokers), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	fetches := consumer.PollFetches(ctx)
	if errs := fetches.Errors(); len(errs) != 0 {
		t.Fatalf("fetch: %v", errs)
	}
	var got *kgo.Record
	fetches.EachRecord(func(record *kgo.Record) { got = record })
	if got == nil || string(got.Key) != "acct_000001" || string(got.Value) != `{"x":1}` ||
		len(got.Headers) != 1 || string(got.Headers[0].Value) != "01M6ZW06807TFTK1P7CPF6Q3AT" {
		t.Fatalf("round trip: %+v", got)
	}
}

func TestNewConsumerDefaultsAndCanceledRun(t *testing.T) {
	client, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	consumer := NewConsumer(client, nil, NewMetrics(prometheus.NewRegistry()), obs.NewLogger("error"), 0, 0)
	if consumer.workers != 1 || consumer.inflight != 1 {
		t.Fatalf("defaults: workers=%d inflight=%d", consumer.workers, consumer.inflight)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := consumer.Run(ctx); err != nil {
		t.Fatalf("canceled Run: %v", err)
	}
}

func TestProcessorDLQPublishFailures(t *testing.T) {
	failing := &capturePublisher{err: fmt.Errorf("broker unavailable")}
	processor, metrics, store, _ := newProcessor(t, failing, "")
	invalid := &kgo.Record{Topic: "txn.raw", Partition: 0, Offset: 1, Value: []byte("{bad"),
		Timestamp: time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)}
	if err := processor.Handle(context.Background(), invalid); err == nil {
		t.Fatal("invalid-event DLQ failure swallowed")
	}
	newer := eventJSON("01M6ZWJG607JTAB7X0D2AHXGPW", "2026-11-09T10:10:00.000Z", "acct_000001", "acct_000002", 900, "P2P_SEND")
	if _, _, err := store.Compute(context.Background(), mustParse(t, newer)); err != nil {
		t.Fatal(err)
	}
	late := eventJSON("01M6ZW9B70CRE2PH59XBCW76TN", "2026-11-09T10:05:00.000Z", "acct_000001", "acct_000002", 900, "P2P_SEND")
	lateRec := &kgo.Record{Topic: "txn.raw", Partition: 0, Offset: 2, Value: late,
		Timestamp: time.Date(2026, 11, 9, 10, 5, 0, 0, time.UTC)}
	if err := processor.Handle(context.Background(), lateRec); err == nil {
		t.Fatal("late-event DLQ failure swallowed")
	}
	if testutil.ToFloat64(metrics.Events.WithLabelValues("error")) < 2 {
		t.Fatal("error metric")
	}
}

func TestProcessPartitionCommitFailure(t *testing.T) {
	processor, metrics, _, _ := newProcessor(t, &capturePublisher{}, "")
	client, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	consumer := NewConsumer(client, processor, metrics, obs.NewLogger("error"), 1, 1)
	raw := eventJSON("01M6ZW06807TFTK1P7CPF6Q3AT", "2026-11-09T10:00:00.000Z", "acct_000001", "acct_000002", 5000, "P2P_SEND")
	rec := &kgo.Record{Topic: "txn.raw", Partition: 0, Offset: 1, Key: []byte("acct_000001"), Value: raw,
		Timestamp: time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := consumer.processPartition(ctx, []*kgo.Record{rec}); err == nil {
		t.Fatal("commit failure not returned")
	}
	if testutil.ToFloat64(metrics.Commits.WithLabelValues("error")) == 0 {
		t.Fatal("commit error metric")
	}
}

func TestPublisherErrorAndCanceledPartition(t *testing.T) {
	producer, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if err := NewKgoPublisher(producer).Publish(ctx, "topic", nil, nil, nil); err == nil {
		t.Fatal("publish to unreachable broker succeeded")
	}

	processor, metrics, _, _ := newProcessor(t, &capturePublisher{}, "")
	consumer := NewConsumer(producer, processor, metrics, obs.NewLogger("error"), 1, 1)
	cancelled, stop := context.WithCancel(context.Background())
	stop()
	raw := eventJSON("01M6ZW06807TFTK1P7CPF6Q3AT", "2026-11-09T10:00:00.000Z", "acct_000001", "acct_000002", 5000, "P2P_SEND")
	rec := &kgo.Record{Topic: "txn.raw", Partition: 0, Offset: 1, Key: []byte("acct_000001"), Value: raw,
		Timestamp: time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)}
	if err := consumer.processPartition(cancelled, []*kgo.Record{rec}); err == nil {
		t.Fatal("canceled partition returned nil")
	}
}

func TestRunFetchCancellation(t *testing.T) {
	client, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	consumer := NewConsumer(client, nil, NewMetrics(prometheus.NewRegistry()), obs.NewLogger("error"), 1, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if err := consumer.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestConsumerEmptyAndCanceledRetry(t *testing.T) {
	processor, metrics, _, _ := newProcessor(t, &capturePublisher{err: fmt.Errorf("down")}, "")
	client, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	consumer := NewConsumer(client, processor, metrics, obs.NewLogger("error"), 1, 1)
	if err := consumer.processPartition(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rec := &kgo.Record{Topic: "txn.raw", Partition: 0, Offset: 1}
	if err := consumer.processWithRetry(ctx, rec); err == nil {
		t.Fatal("canceled retry returned nil")
	}
}
