package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Publisher is the small Kafka surface the processor needs. Tests inject an
// in-memory implementation.
type Publisher interface {
	Publish(ctx context.Context, topic string, key, value []byte, headers []kgo.RecordHeader) error
}

type kgoPublisher struct {
	client *kgo.Client
}

func (p kgoPublisher) Publish(ctx context.Context, topic string, key, value []byte, headers []kgo.RecordHeader) error {
	rec := &kgo.Record{
		Topic:     topic,
		Key:       key,
		Value:     value,
		Headers:   headers,
		Timestamp: time.Now(),
	}
	if err := p.client.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("produce to %s: %w", topic, err)
	}
	return nil
}
