// Package dataset reads replay datasets from data/<set>: arrival-ordered events,
// labels, account segments and the manifest that pins every byte.
package dataset

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/parquet-go/parquet-go"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/events"
)

// Manifest is the generator's dataset description. Only the fields replay
// depends on are decoded; unknown fields are ignored so the generator can add
// metadata without breaking old readers.
type Manifest struct {
	Config struct {
		Accounts int `json:"accounts"`
		Days     int `json:"days"`
	} `json:"config"`
	Counts struct {
		Accounts     int `json:"accounts"`
		Applied      int `json:"applied"`
		Events       int `json:"events"`
		EventsUnique int `json:"events_unique"`
	} `json:"counts"`
	Files map[string]struct {
		Bytes  int64  `json:"bytes"`
		SHA256 string `json:"sha256"`
	} `json:"files"`
}

// Label is one ground-truth row.
type Label struct {
	EventID     string
	Label       string
	Scenario    string
	ScenarioRun string
}

// Dataset is a verified dataset directory.
type Dataset struct {
	Name     string
	Dir      string
	Hash     string
	Manifest Manifest
	// Fraud holds only FRAUD labels; every other event defaults to LEGIT.
	Fraud map[string]Label
	// Accounts maps account id to segment.
	Accounts map[string]string
	// First and Last are the event-time bounds from the manifest filenames or
	// computed lazily by scanning; populated by Load.
	First, Last time.Time
	Events      int
}

type labelRow struct {
	EventID     string  `parquet:"event_id"`
	Label       string  `parquet:"label"`
	Scenario    *string `parquet:"scenario,optional"`
	ScenarioRun *string `parquet:"scenario_run_id,optional"`
}

type accountRow struct {
	AccountID string `parquet:"account_id"`
	Segment   string `parquet:"segment"`
}

// Verify checks the manifest and every file hash. It fails closed: a file that
// is missing, truncated or modified cannot be replayed.
func Verify(dir string) (*Manifest, string, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return nil, "", fmt.Errorf("dataset: read manifest: %w", err)
	}
	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, "", fmt.Errorf("dataset: decode manifest: %w", err)
	}
	if len(m.Files) == 0 {
		return nil, "", fmt.Errorf("dataset: manifest has no files")
	}
	names := make([]string, 0, len(m.Files))
	for name := range m.Files {
		names = append(names, name)
	}
	sort.Strings(names)
	h := sha256.New()
	for _, name := range names {
		want := m.Files[name]
		path := filepath.Join(dir, name)
		st, err := os.Stat(path)
		if err != nil {
			return nil, "", fmt.Errorf("dataset: %s: %w", name, err)
		}
		if want.Bytes != 0 && st.Size() != want.Bytes {
			return nil, "", fmt.Errorf("dataset: %s size %d, manifest %d", name, st.Size(), want.Bytes)
		}
		got, err := fileSHA256(path)
		if err != nil {
			return nil, "", err
		}
		if want.SHA256 != "" && got != want.SHA256 {
			return nil, "", fmt.Errorf("dataset: %s sha256 %s, manifest %s", name, got, want.SHA256)
		}
		fmt.Fprintf(h, "%s:%s\n", name, got)
	}
	return &m, hex.EncodeToString(h.Sum(nil)), nil
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// Load verifies and opens a dataset: labels and account segments are loaded
// into memory (small), events stay on disk.
func Load(dir string) (*Dataset, error) {
	m, hash, err := Verify(dir)
	if err != nil {
		return nil, err
	}
	ds := &Dataset{
		Name:     filepath.Base(filepath.Clean(dir)),
		Dir:      dir,
		Hash:     hash,
		Manifest: *m,
		Fraud:    map[string]Label{},
		Accounts: map[string]string{},
		Events:   m.Counts.Events,
	}
	if err := ds.loadLabels(); err != nil {
		return nil, err
	}
	if err := ds.loadAccounts(); err != nil {
		return nil, err
	}
	return ds, nil
}

func (d *Dataset) loadLabels() error {
	path := filepath.Join(d.Dir, "labels.parquet")
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("dataset: open labels: %w", err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	pf, err := parquet.OpenFile(f, st.Size())
	if err != nil {
		return fmt.Errorf("dataset: labels: %w", err)
	}
	reader := parquet.NewGenericReader[labelRow](pf)
	defer reader.Close()
	rows := make([]labelRow, 2048)
	for {
		n, rerr := reader.Read(rows)
		for i := 0; i < n; i++ {
			row := rows[i]
			if row.Label != "FRAUD" {
				continue
			}
			lab := Label{EventID: row.EventID, Label: row.Label}
			if row.Scenario != nil {
				lab.Scenario = *row.Scenario
			}
			if row.ScenarioRun != nil {
				lab.ScenarioRun = *row.ScenarioRun
			}
			d.Fraud[row.EventID] = lab
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return fmt.Errorf("dataset: read labels: %w", rerr)
		}
	}
	return nil
}

func (d *Dataset) loadAccounts() error {
	path := filepath.Join(d.Dir, "accounts.parquet")
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("dataset: open accounts: %w", err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	pf, err := parquet.OpenFile(f, st.Size())
	if err != nil {
		return fmt.Errorf("dataset: accounts: %w", err)
	}
	reader := parquet.NewGenericReader[accountRow](pf)
	defer reader.Close()
	rows := make([]accountRow, 2048)
	for {
		n, rerr := reader.Read(rows)
		for i := 0; i < n; i++ {
			d.Accounts[rows[i].AccountID] = rows[i].Segment
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return fmt.Errorf("dataset: read accounts: %w", rerr)
		}
	}
	return nil
}

// Segment returns the account segment or "unknown".
func (d *Dataset) Segment(accountID string) string {
	if s, ok := d.Accounts[accountID]; ok && s != "" {
		return s
	}
	return "unknown"
}

// Label returns the label for an event, defaulting to LEGIT.
func (d *Dataset) Label(eventID string) Label {
	if l, ok := d.Fraud[eventID]; ok {
		return l
	}
	return Label{EventID: eventID, Label: "LEGIT"}
}

// Options filter the event stream.
type Options struct {
	From      time.Time
	To        time.Time
	SamplePct float64
	Scenario  string
	Seed      int64
}

type arrivalRow struct {
	EventID     string    `parquet:"event_id"`
	ArrivalTime time.Time `parquet:"arrival_time"`
	Seq         int64     `parquet:"seq"`
}

// arrivalStream pairs the JSONL event stream with arrivals.parquet. The two
// files are produced in the same order; a mismatch is a hard dataset error.
type arrivalStream struct {
	reader *parquet.GenericReader[arrivalRow]
	buf    []arrivalRow
	n      int
	eof    bool
}

func openArrivals(dir string) (*arrivalStream, error) {
	f, err := os.Open(filepath.Join(dir, "arrivals.parquet"))
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	pf, err := parquet.OpenFile(f, st.Size())
	if err != nil {
		f.Close()
		return nil, err
	}
	return &arrivalStream{reader: parquet.NewGenericReader[arrivalRow](pf)}, nil
}

func (a *arrivalStream) next() (arrivalRow, bool, error) {
	if a.n >= len(a.buf) {
		if a.eof {
			return arrivalRow{}, false, nil
		}
		a.buf = make([]arrivalRow, 512)
		n, err := a.reader.Read(a.buf)
		a.buf = a.buf[:n]
		a.n = 0
		if err == io.EOF {
			a.eof = true
		}
		if err != nil && err != io.EOF {
			return arrivalRow{}, false, err
		}
		if n == 0 {
			return arrivalRow{}, false, nil
		}
	}
	row := a.buf[a.n]
	a.n++
	return row, true, nil
}

// Stream reads events in arrival order and calls fn. arrival is the event's
// arrival time when arrivals.parquet is present, else its event time.
func (d *Dataset) Stream(ctx context.Context, fn func(ev *events.Event) error) error {
	path := filepath.Join(d.Dir, "events.jsonl.zst")
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("dataset: open events: %w", err)
	}
	defer f.Close()
	dec, err := zstd.NewReader(f, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return err
	}
	defer dec.Close()
	arrivals, aerr := openArrivals(d.Dir)
	if aerr == nil {
		defer arrivals.reader.Close()
	}
	sc := bufio.NewScanner(dec)
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	for sc.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		ev, verr := events.Parse(line, time.Time{})
		if verr != nil {
			// Damage in a frozen dataset is a hard error; silently skipping it
			// would make every downstream metric suspect.
			return fmt.Errorf("dataset: invalid event: %w", verr)
		}
		if aerr == nil {
			row, ok, rerr := arrivals.next()
			if rerr != nil {
				return fmt.Errorf("dataset: read arrivals: %w", rerr)
			}
			if !ok {
				return fmt.Errorf("dataset: arrivals shorter than events")
			}
			if row.EventID != ev.EventID {
				return fmt.Errorf("dataset: arrivals out of order at %s (got %s)", ev.EventID, row.EventID)
			}
		}
		if err := fn(ev); err != nil {
			return err
		}
	}
	return sc.Err()
}

// InScope applies time, scenario and deterministic account-sampling filters.
func (d *Dataset) InScope(ev *events.Event, opts Options) bool {
	if !opts.From.IsZero() && ev.EventTimeMS < opts.From.UnixMilli() {
		return false
	}
	if !opts.To.IsZero() && ev.EventTimeMS > opts.To.UnixMilli() {
		return false
	}
	if opts.Scenario != "" {
		lab, ok := d.Fraud[ev.EventID]
		if !ok || lab.Scenario != opts.Scenario {
			return false
		}
	}
	if opts.SamplePct > 0 && opts.SamplePct < 100 {
		h := fnv.New64a()
		_, _ = h.Write([]byte(ev.Sender.AccountID))
		bucket := float64(h.Sum64()%10000) / 100.0
		if bucket >= opts.SamplePct {
			return false
		}
	}
	return true
}

// AccountHash returns a stable shard id for an account.
func AccountHash(accountID string, shards int) int {
	if shards <= 1 {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(strings.TrimSpace(accountID)))
	return int(h.Sum64() % uint64(shards))
}
