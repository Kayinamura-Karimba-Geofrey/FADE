package outbox

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaPublisher publishes relayed records to Redpanda/Kafka. Delivery is
// synchronous so the relay only marks a row published after the broker acked
// it; a failed publish leaves the row for the next pass.
type KafkaPublisher struct {
	client *kgo.Client
}

// NewKafkaPublisher connects a producer. The client is lazy, so an unavailable
// broker surfaces on the first publish rather than at startup.
func NewKafkaPublisher(brokers []string) (*KafkaPublisher, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("outbox publisher needs at least one broker")
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.AllowAutoTopicCreation(),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, err
	}
	return &KafkaPublisher{client: client}, nil
}

// Publish sends one record.
func (p *KafkaPublisher) Publish(ctx context.Context, topic, key string, payload []byte) error {
	res := p.client.ProduceSync(ctx, &kgo.Record{
		Topic: topic, Key: []byte(key), Value: payload,
	})
	return res.FirstErr()
}

// Close flushes and closes the producer.
func (p *KafkaPublisher) Close() { p.client.Close() }
