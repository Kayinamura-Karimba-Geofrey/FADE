package pipeline

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaPublisher publishes decisions to txn.decisions, keyed by account id so
// per-account ordering is preserved within a partition.
type KafkaPublisher struct {
	client *kgo.Client
	topic  string
}

// NewKafkaPublisher connects lazily; Produce is asynchronous.
func NewKafkaPublisher(brokers []string, topic string) (*KafkaPublisher, error) {
	if len(brokers) == 0 || topic == "" {
		return nil, fmt.Errorf("kafka publisher needs brokers and topic")
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.AllowAutoTopicCreation(),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, err
	}
	return &KafkaPublisher{client: client, topic: topic}, nil
}

// PublishDecisions queues one record per decision and waits for acks.
func (p *KafkaPublisher) PublishDecisions(ctx context.Context, batch []*StoredDecision) error {
	records := make([]*kgo.Record, 0, len(batch))
	for _, rec := range batch {
		raw, err := json.Marshal(rec.Decision)
		if err != nil {
			continue
		}
		records = append(records, &kgo.Record{
			Topic: p.topic, Key: []byte(rec.AccountID), Value: raw,
			Headers: []kgo.RecordHeader{
				{Key: "decision_id", Value: []byte(rec.Decision.DecisionID)},
				{Key: "rule_set_version", Value: []byte(fmt.Sprint(rec.Decision.RuleSetVersion))},
			},
		})
	}
	results := p.client.ProduceSync(ctx, records...)
	var first error
	for _, r := range results {
		if r.Err != nil && first == nil {
			first = r.Err
		}
	}
	return first
}

// Close flushes and closes the producer.
func (p *KafkaPublisher) Close() {
	p.client.Flush(context.Background())
	p.client.Close()
}
