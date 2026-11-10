package features

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/events"
)

func benchmarkStore(b *testing.B) (*Store, *redis.Client) {
	b.Helper()
	if testRedisAddr == "" {
		b.Skip("set FADE_TEST_REDIS_ADDR or start Docker")
	}
	client := redis.NewClient(&redis.Options{Addr: testRedisAddr})
	if err := client.FlushDB(context.Background()).Err(); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = client.Close() })
	return NewStore(client, testConfig, time.Minute, 24*time.Hour, Hooks{}), client
}

func benchEvent(i int, base time.Time) *events.Event {
	sender := fmt.Sprintf("acct_%06d", i%997)
	recipient := fmt.Sprintf("acct_%06d", (i*7+13)%997)
	if sender == recipient {
		recipient = "acct_999999"
	}
	txn := events.MerchantPay
	switch i % 10 {
	case 7, 8:
		txn = events.P2PSend
	case 9:
		txn = events.CashIn
	case 6:
		txn = events.CashOut
	}
	var agentID *string
	if txn == events.CashIn || txn == events.CashOut {
		recipient = "spec_agent_000"
		agent := recipient
		agentID = &agent
	}
	return &events.Event{
		EventID:     fmt.Sprintf("01M6ZW06807TFTK1P7CPF%06d", i),
		EventTimeMS: base.Add(time.Duration(i) * time.Millisecond).UnixMilli(),
		TxnType:     txn,
		Amount:      1000 + int64(i%9000),
		Sender: events.Sender{
			AccountID: sender, DeviceID: "dev_" + sender, SIMSerialHash: "sim_" + sender,
			Geo: events.Geo{Lat: -1.9, Lon: 30.0}, KYCTier: 2,
		},
		Recipient: events.Recipient{AccountID: recipient, IsAgent: agentID != nil},
		AgentID:   agentID,
	}
}

func BenchmarkFeatureComputeSequential(b *testing.B) {
	store, _ := benchmarkStore(b)
	base := time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)
	ctx := context.Background()
	events := make([]*events.Event, b.N)
	for i := range events {
		events[i] = benchEvent(i, base)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := store.Compute(ctx, events[i]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFeatureComputeParallel(b *testing.B) {
	store, _ := benchmarkStore(b)
	base := time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)
	ctx := context.Background()
	events := make([]*events.Event, b.N)
	for i := range events {
		events[i] = benchEvent(i, base)
	}
	var next atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := int(next.Add(1) - 1)
			if _, _, err := store.Compute(ctx, events[i]); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
