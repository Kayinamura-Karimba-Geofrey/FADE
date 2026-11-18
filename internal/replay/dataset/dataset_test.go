package dataset

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/events"
)

func TestLoadGoldenAndStream(t *testing.T) {
	ds, err := Load(filepath.Join("..", "..", "..", "data", "golden"))
	if err != nil {
		t.Fatal(err)
	}
	if ds.Hash == "" || ds.Events == 0 {
		t.Fatalf("dataset = %+v", ds)
	}
	if len(ds.Fraud) == 0 {
		t.Fatal("no fraud labels loaded")
	}
	if ds.Segment("acct_000000") == "" {
		t.Fatal("segments not loaded")
	}
	n := 0
	err = ds.Stream(context.Background(), func(ev *events.Event) error {
		n++
		if ev.EventID == "" || ev.EventTimeMS == 0 {
			t.Fatal("event not parsed")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != ds.Events {
		t.Fatalf("streamed %d events, manifest %d", n, ds.Events)
	}
}

func TestManifestMismatchFailsClosed(t *testing.T) {
	src := filepath.Join("..", "..", "..", "data", "golden")
	dst := t.TempDir()
	for _, name := range []string{"manifest.json", "events.jsonl.zst", "arrivals.parquet", "labels.parquet", "accounts.parquet"} {
		raw, err := os.ReadFile(filepath.Join(src, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, name), raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	f, err := os.OpenFile(filepath.Join(dst, "labels.parquet"), os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte("tamper"))
	f.Close()
	if _, _, err := Verify(dst); err == nil {
		t.Fatal("tampered dataset verified")
	}
}

func TestDeterministicAccountSampling(t *testing.T) {
	ds, err := Load(filepath.Join("..", "..", "..", "data", "golden"))
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	_ = ds.Stream(context.Background(), func(ev *events.Event) error {
		opts := Options{SamplePct: 10}
		a := ds.InScope(ev, opts)
		b := ds.InScope(ev, opts)
		if a != b {
			t.Fatalf("sampling is not deterministic for %s", ev.EventID)
		}
		if a {
			seen[ev.Sender.AccountID] = true
		}
		return nil
	})
	if len(seen) == 0 {
		t.Fatal("10% sample selected no accounts")
	}
}
