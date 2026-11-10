package ingest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/events"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/features"
)

// Processor turns one Kafka record into at most one feature vector. It is
// safe for concurrent use.
type Processor struct {
	store       *features.Store
	publisher   Publisher
	metrics     *Metrics
	log         *slog.Logger
	dlqTopic    string
	outputTopic string
	version     string
}

// NewProcessor constructs a processor. outputTopic may be empty.
func NewProcessor(store *features.Store, publisher Publisher, metrics *Metrics, log *slog.Logger,
	dlqTopic, outputTopic, version string) *Processor {
	return &Processor{
		store: store, publisher: publisher, metrics: metrics, log: log,
		dlqTopic: dlqTopic, outputTopic: outputTopic, version: version,
	}
}

// DLQPayload is the documented dead-letter envelope. Raw is base64 so even
// invalid UTF-8 or malformed JSON is preserved byte-for-byte.
type DLQPayload struct {
	EventID         string `json:"event_id"`
	Reason          string `json:"reason"`
	Detail          string `json:"detail"`
	SourceTopic     string `json:"source_topic"`
	SourcePartition int32  `json:"source_partition"`
	SourceOffset    int64  `json:"source_offset"`
	FailedAt        string `json:"failed_at"`
	Raw             string `json:"raw_base64"`
}

// Handle processes one record. A nil error means the offset is safe to commit.
// Non-nil errors are transient (Redis/Kafka unavailable) and must be retried.
func (p *Processor) Handle(ctx context.Context, rec *kgo.Record) error {
	start := time.Now()
	defer func() { p.metrics.Processing.Observe(time.Since(start).Seconds()) }()

	ingestTime := rec.Timestamp
	if ingestTime.IsZero() {
		ingestTime = time.Now()
	}
	ev, verr := events.Parse(rec.Value, ingestTime)
	if verr != nil {
		p.metrics.ParseFailures.Inc()
		p.metrics.Events.WithLabelValues("invalid").Inc()
		if err := p.deadLetter(ctx, rec, ev, verr.Reason, verr.Detail); err != nil {
			p.metrics.Events.WithLabelValues("error").Inc()
			return err
		}
		return nil
	}

	vector, status, err := p.store.Compute(ctx, ev)
	if err != nil {
		p.metrics.Events.WithLabelValues("error").Inc()
		return fmt.Errorf("event %s: %w", ev.EventID, err)
	}
	switch status {
	case features.StatusDuplicate:
		p.metrics.Events.WithLabelValues("duplicate").Inc()
		return nil
	case features.StatusLate:
		p.metrics.Events.WithLabelValues("late").Inc()
		if err := p.deadLetter(ctx, rec, ev, events.ReasonLate,
			"event is older than the account watermark minus LATE_TOLERANCE_S"); err != nil {
			p.metrics.Events.WithLabelValues("error").Inc()
			return err
		}
		return nil
	}

	if p.outputTopic != "" {
		payload, err := vector.MarshalCanonical()
		if err != nil {
			p.metrics.Events.WithLabelValues("error").Inc()
			return fmt.Errorf("marshal features %s: %w", ev.EventID, err)
		}
		headers := []kgo.RecordHeader{
			{Key: "event_id", Value: []byte(ev.EventID)},
			{Key: "feature_schema_version", Value: []byte(vector.FeatureSchemaVersion)},
		}
		if err := p.publisher.Publish(ctx, p.outputTopic, []byte(ev.Sender.AccountID), payload, headers); err != nil {
			p.metrics.Events.WithLabelValues("error").Inc()
			return fmt.Errorf("publish features %s: %w", ev.EventID, err)
		}
		p.metrics.FeatureOutput.Inc()
	}

	p.metrics.Events.WithLabelValues("ok").Inc()
	return nil
}

func (p *Processor) deadLetter(ctx context.Context, rec *kgo.Record, ev *events.Event, reason events.Reason, detail string) error {
	eventID := ""
	if ev != nil {
		eventID = ev.EventID
	} else {
		var probe struct {
			EventID string `json:"event_id"`
		}
		_ = json.Unmarshal(rec.Value, &probe)
		eventID = probe.EventID
	}
	payload, err := json.Marshal(DLQPayload{
		EventID:         eventID,
		Reason:          string(reason),
		Detail:          detail,
		SourceTopic:     rec.Topic,
		SourcePartition: rec.Partition,
		SourceOffset:    rec.Offset,
		FailedAt:        time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
		Raw:             base64.StdEncoding.EncodeToString(rec.Value),
	})
	if err != nil {
		return fmt.Errorf("marshal DLQ payload: %w", err)
	}
	key := []byte(eventID)
	if eventID == "" {
		key = rec.Key
	}
	headers := []kgo.RecordHeader{
		{Key: "reason", Value: []byte(reason)},
		{Key: "source_topic", Value: []byte(rec.Topic)},
		{Key: "schema_version", Value: []byte(p.version)},
	}
	start := time.Now()
	if err := p.publisher.Publish(ctx, p.dlqTopic, key, payload, headers); err != nil {
		return fmt.Errorf("publish DLQ %s: %w", reason, err)
	}
	p.metrics.DLQDuration.Observe(time.Since(start).Seconds())
	p.metrics.DeadLettered.WithLabelValues(string(reason)).Inc()
	p.log.Warn("dead-lettered event", "event_id", eventID, "reason", reason,
		"partition", rec.Partition, "offset", rec.Offset, "detail", detail)
	return nil
}
