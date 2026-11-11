// Package dataset provides small readers for the generated FADE fixtures used
// by replay-lite and load tools: compressed JSONL events and Parquet labels.
package dataset

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/klauspost/compress/zstd"
	"github.com/parquet-go/parquet-go"
)

// Label is one ground-truth row.
type Label struct {
	EventID  string
	Label    string // FRAUD or LEGIT
	Scenario string
}

type labelRow struct {
	EventID  string  `parquet:"event_id"`
	Label    string  `parquet:"label"`
	Scenario *string `parquet:"scenario,optional"`
}

// ReadLabels loads labels.parquet into a map keyed by event_id.
func ReadLabels(path string) (map[string]Label, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	pf, err := parquet.OpenFile(f, st.Size())
	if err != nil {
		return nil, fmt.Errorf("open labels parquet: %w", err)
	}
	reader := parquet.NewGenericReader[labelRow](pf)
	defer reader.Close()

	out := make(map[string]Label, pf.NumRows())
	rows := make([]labelRow, 512)
	for {
		n, err := reader.Read(rows)
		for i := 0; i < n; i++ {
			scenario := ""
			if rows[i].Scenario != nil {
				scenario = *rows[i].Scenario
			}
			out[rows[i].EventID] = Label{EventID: rows[i].EventID, Label: rows[i].Label, Scenario: scenario}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// ForEachEventJSONLZstd streams events in file (arrival) order. The callback
// receives the raw JSON object; return an error from fn to stop.
func ForEachEventJSONLZstd(path string, fn func(raw []byte) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	dec, err := zstd.NewReader(f, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return err
	}
	defer dec.Close()

	sc := bufio.NewScanner(dec)
	sc.Buffer(make([]byte, 1<<20), 8<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		if err := fn(line); err != nil {
			return err
		}
	}
	return sc.Err()
}

// ProbeEventID extracts event_id without full schema validation.
func ProbeEventID(raw []byte) string {
	var probe struct {
		EventID string `json:"event_id"`
	}
	_ = json.Unmarshal(raw, &probe)
	return probe.EventID
}
