package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/cases"
)

const watermarkKey = "decision_watermark"

// triage turns decisions into cases and enforces the SLA clock.
type triage struct {
	pool   *pgxpool.Pool
	cases  *cases.Store
	log    *slog.Logger
	batch  int
	qaRate float64
}

func (t *triage) run(ctx context.Context, poll, slaEvery time.Duration) error {
	pollTick := time.NewTicker(poll)
	defer pollTick.Stop()
	slaTick := time.NewTicker(slaEvery)
	defer slaTick.Stop()
	for {
		if err := t.sweepDecisions(ctx); err != nil && ctx.Err() == nil {
			t.log.Error("decision sweep failed", "error", err)
		}
		if err := t.sweepSLA(ctx); err != nil && ctx.Err() == nil {
			t.log.Error("sla sweep failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-pollTick.C:
		case <-slaTick.C:
		}
	}
}

// sweepDecisions consumes every decision newer than the stored watermark.
func (t *triage) sweepDecisions(ctx context.Context) error {
	watermark, err := t.loadWatermark(ctx)
	if err != nil {
		return err
	}
	page, err := t.cases.DecisionPage(ctx, watermark, t.batch)
	if err != nil {
		return err
	}
	var opened, linked, sampled int
	now := time.Now().UTC()
	for i := range page {
		d := &page[i]
		source, want := triageOutcome(d, t.qaRate)
		if !want {
			watermark = d.DecidedAt
			continue
		}
		c, created, err := t.cases.OpenFromDecision(ctx, d, source, now)
		if err != nil {
			// Stop the page and retry this decision on the next pass rather than
			// skipping it: the watermark only advances past handled rows.
			if uerr := t.saveWatermark(ctx, watermark); uerr != nil {
				return uerr
			}
			return err
		}
		if created {
			opened++
			if source == cases.SourceQASample {
				sampled++
			}
		} else if c != nil {
			linked++
		}
		watermark = d.DecidedAt
	}
	if err := t.saveWatermark(ctx, watermark); err != nil {
		return err
	}
	if opened+linked > 0 {
		t.log.Info("decisions triaged", "opened", opened, "linked", linked, "qa_sample", sampled,
			"watermark", watermark.Format(time.RFC3339Nano))
	}
	return nil
}

// triageOutcome maps a decision class to the case source and whether a case is
// warranted at all. STEP_UP opens no case until the challenge fails, which the
// step-up-result endpoint reports separately.
func triageOutcome(d *cases.DecisionRef, qaRate float64) (cases.Source, bool) {
	switch d.Decision {
	case "BLOCK", "HOLD":
		return cases.SourceDecision, true
	case "ALLOW":
		if cases.QASampleSelected(d.ID, qaRate) {
			return cases.SourceQASample, true
		}
	}
	return "", false
}

func (t *triage) sweepSLA(ctx context.Context) error {
	breached, err := t.cases.BreachDue(ctx, time.Now().UTC(), 200)
	if err != nil {
		return err
	}
	if len(breached) > 0 {
		t.log.Warn("sla breaches escalated", "count", len(breached))
	}
	return nil
}

func (t *triage) loadWatermark(ctx context.Context) (time.Time, error) {
	var raw []byte
	err := t.pool.QueryRow(ctx,
		`SELECT value FROM case_config WHERE key=$1`, watermarkKey).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Now().UTC().Add(-24 * time.Hour), nil
	}
	if err != nil {
		return time.Time{}, err
	}
	var stored struct {
		At time.Time `json:"at"`
	}
	if err := json.Unmarshal(raw, &stored); err != nil || stored.At.IsZero() {
		return time.Now().UTC().Add(-24 * time.Hour), nil
	}
	return stored.At, nil
}

func (t *triage) saveWatermark(ctx context.Context, at time.Time) error {
	if at.IsZero() {
		return nil
	}
	raw, err := json.Marshal(map[string]time.Time{"at": at.UTC()})
	if err != nil {
		return err
	}
	_, err = t.pool.Exec(ctx, `
		INSERT INTO case_config (key, value) VALUES ($1,$2)
		ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value`, watermarkKey, raw)
	return err
}
