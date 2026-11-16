package audit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"pgregory.net/rapid"
)

func row(i int) Row {
	return Row{
		Actor: "admin@fade", Action: string(ActionRuleUpdate),
		ObjectType: "rule", ObjectID: fmt.Sprintf("R-VEL-%03d", i),
		Before: json.RawMessage(fmt.Sprintf(`{"weight":%d}`, i)),
		After:  json.RawMessage(fmt.Sprintf(`{"weight":%d}`, i+1)),
		At:     NormalizeAt(time.Date(2026, 11, 16, 0, 0, i%60, 0, time.UTC)),
	}
}

func appendRows(t *testing.T, s Store, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := s.Append(context.Background(), row(i)); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
}

func TestGenesisHashIsDocumented(t *testing.T) {
	sum := sha256.Sum256([]byte("FADE-AUDIT-GENESIS-v1"))
	want := hex.EncodeToString(sum[:])
	if GenesisHash != want {
		t.Fatalf("genesis hash = %s want %s", GenesisHash, want)
	}
	if len(GenesisHash) != 64 {
		t.Fatalf("genesis hash is not a sha256 digest: %s", GenesisHash)
	}
}

func TestChainIsVerifiable(t *testing.T) {
	s := NewMemoryStore()
	appendRows(t, s, 50)
	res, err := VerifyChain(context.Background(), s, VerifyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !res.OK || res.Checked != 50 {
		t.Fatalf("verify: ok=%v checked=%d break=%+v", res.OK, res.Checked, res.FirstBreak)
	}
	entries := s.Entries()
	if entries[0].PrevHash != GenesisHash {
		t.Fatalf("first prev_hash=%q", entries[0].PrevHash)
	}
	for i := 1; i < len(entries); i++ {
		if entries[i].PrevHash != entries[i-1].Hash {
			t.Fatalf("link %d broken", i)
		}
		if entries[i].Seq != entries[i-1].Seq+1 {
			t.Fatalf("seq %d not contiguous", entries[i].Seq)
		}
	}
}

func TestHashIsStableAcrossFormatting(t *testing.T) {
	a := Row{Actor: "a", Action: "X", ObjectType: "rule", ObjectID: "1",
		Before: json.RawMessage(`{"b":2,"a":1}`), After: nil, At: "2026-11-16T00:00:00.000000Z"}
	b := a
	b.Before = json.RawMessage(`{ "a" : 1 , "b" : 2 }`)
	ha, err := ChainHash(GenesisHash, a)
	if err != nil {
		t.Fatal(err)
	}
	hb, err := ChainHash(GenesisHash, b)
	if err != nil {
		t.Fatal(err)
	}
	if ha != hb {
		t.Fatalf("hash depends on key order: %s != %s", ha, hb)
	}
}

func TestTamperEditDetected(t *testing.T) {
	s := NewMemoryStore()
	appendRows(t, s, 10)
	s.entries[5].Row.After = json.RawMessage(`{"weight":9999}`)
	res, err := VerifyChain(context.Background(), s, VerifyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if res.OK || res.FirstBreak == nil || res.FirstBreak.Seq != 6 || res.FirstBreak.Kind != BreakRowHash {
		t.Fatalf("edit not detected: %+v", res)
	}
}

func TestTamperDeleteMiddleDetected(t *testing.T) {
	s := NewMemoryStore()
	appendRows(t, s, 10)
	s.entries = append(s.entries[:4], s.entries[5:]...)
	res, _ := VerifyChain(context.Background(), s, VerifyOptions{})
	if res.OK || res.FirstBreak == nil || res.FirstBreak.Kind != BreakSeqGap {
		t.Fatalf("middle delete not detected: %+v", res)
	}
}

func TestTamperRebuildForwardDetectedByCheckpoint(t *testing.T) {
	s := NewMemoryStore()
	appendRows(t, s, 20)
	dir := t.TempDir()
	key := []byte("test-key")
	head, _ := s.Head(context.Background())
	count, _ := s.CountTo(context.Background(), head.Seq)
	cp := Checkpoint{Seq: head.Seq, Hash: head.Hash, RowCount: count,
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	if err := cp.Sign(key); err != nil {
		t.Fatal(err)
	}
	if _, err := WriteCheckpointFile(dir, cp); err != nil {
		t.Fatal(err)
	}

	// Delete the last 5 rows (including the checkpointed head) and rebuild a
	// perfectly valid chain forward with recomputed hashes.
	s.entries = s.entries[:15]
	for i := 0; i < 5; i++ {
		if _, err := s.Append(context.Background(), row(100+i)); err != nil {
			t.Fatal(err)
		}
	}
	res, err := VerifyChain(context.Background(), s, VerifyOptions{CheckpointDir: dir, HMACKey: key})
	if err != nil {
		t.Fatal(err)
	}
	if res.OK {
		t.Fatal("rebuilt chain passed verification")
	}
	if res.FirstBreak == nil || res.FirstBreak.Kind != BreakCheckpoint {
		t.Fatalf("expected checkpoint break, got %+v", res.FirstBreak)
	}
}

func TestCheckpointSignature(t *testing.T) {
	cp := Checkpoint{Seq: 7, Hash: "abc", RowCount: 7, CreatedAt: "2026-11-16T00:00:00Z"}
	if err := cp.Sign([]byte("k")); err != nil {
		t.Fatal(err)
	}
	if err := cp.VerifySignature([]byte("k")); err != nil {
		t.Fatal(err)
	}
	if err := cp.VerifySignature([]byte("other")); err == nil {
		t.Fatal("wrong key accepted")
	}
	cp.RowCount++
	if err := cp.VerifySignature([]byte("k")); err == nil {
		t.Fatal("tampered checkpoint accepted")
	}
	var zero Checkpoint
	if err := zero.Sign(nil); err != ErrNoHMACKey {
		t.Fatalf("unsigned checkpoint allowed: %v", err)
	}
}

func TestCheckpointServiceWritesFileAndRow(t *testing.T) {
	s := NewMemoryStore()
	appendRows(t, s, 5)
	dir := t.TempDir()
	svc := NewCheckpointService(s, []byte("key"), dir, Hooks{})
	cp, err := svc.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if cp.RowCount != 5 || cp.Seq != 5 {
		t.Fatalf("checkpoint = %+v", cp)
	}
	files, err := LoadCheckpointFiles(dir)
	if err != nil || len(files) != 1 {
		t.Fatalf("files=%d err=%v", len(files), err)
	}
	if files[0].Digest != cp.Digest {
		t.Fatal("file digest differs")
	}
	// The checkpoint row itself extends the chain to seq 6.
	head, _ := s.Head(context.Background())
	if head.Seq != 6 || head.Row.Action != string(ActionAuditCheckpoint) {
		t.Fatalf("checkpoint row not appended: %+v", head)
	}
	res, err := VerifyChain(context.Background(), s, VerifyOptions{CheckpointDir: dir, HMACKey: []byte("key")})
	if err != nil {
		t.Fatal(err)
	}
	if !res.OK {
		t.Fatalf("verify after checkpoint: %+v", res.FirstBreak)
	}
}

func TestWriterRedactsDisallowedFields(t *testing.T) {
	s := NewMemoryStore()
	w := NewWriter(s, WriterConfig{Clock: func() time.Time {
		return time.Date(2026, 11, 16, 1, 2, 3, 4, time.UTC)
	}})
	entry, err := w.Record(context.Background(), Input{
		Actor: "admin", Action: ActionListAdd, ObjectType: "list_entry", ObjectID: "12",
		After: map[string]any{
			"id": 12, "list_type": "BLOCK", "subject": "ACCOUNT",
			"value": "acct_000123", "reason": "called +250788123456",
			"raw_msisdn": "+250788123456", "internal_note": "secret",
		},
		RequestID: "req-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	var after map[string]any
	if err := json.Unmarshal(entry.Row.After, &after); err != nil {
		t.Fatal(err)
	}
	if _, ok := after["raw_msisdn"]; ok {
		t.Fatal("disallowed field persisted")
	}
	if _, ok := after["internal_note"]; ok {
		t.Fatal("disallowed field persisted")
	}
	reason, _ := after["reason"].(string)
	if reason == "" || reason == "called +250788123456" {
		t.Fatalf("allowlisted free text was not redacted: %q", reason)
	}
	if !json.Valid(entry.Row.After) {
		t.Fatalf("redacted payload is not valid JSON: %s", entry.Row.After)
	}
}

func TestWriterRejectsUnknownActionAndObject(t *testing.T) {
	w := NewWriter(NewMemoryStore(), WriterConfig{})
	if _, err := w.Record(context.Background(), Input{
		Actor: "a", Action: "NOT_REAL", ObjectType: "rule", ObjectID: "1",
	}); err == nil {
		t.Fatal("unknown action accepted")
	}
	if _, err := w.Record(context.Background(), Input{
		Actor: "a", Action: ActionRuleUpdate, ObjectType: "mystery", ObjectID: "1",
		After: map[string]any{"x": 1},
	}); err == nil {
		t.Fatal("unknown object type accepted")
	}
	if _, err := w.Record(context.Background(), Input{
		Actor: "a", Action: ActionRuleUpdate, ObjectType: "rule", ObjectID: "1",
		SourceIPHash: "203.0.113.7", After: map[string]any{"id": "1"},
	}); err == nil {
		t.Fatal("raw IP accepted")
	}
}

func TestConcurrentWritersKeepChainValid(t *testing.T) {
	s := NewMemoryStore()
	w := NewWriter(s, WriterConfig{})
	const writers, perWriter = 50, 40
	var wg sync.WaitGroup
	errCh := make(chan error, writers)
	for wkr := 0; wkr < writers; wkr++ {
		wg.Add(1)
		go func(wkr int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				if _, err := w.Record(context.Background(), Input{
					Actor: fmt.Sprintf("actor-%d", wkr), Action: ActionConfigChange,
					ObjectType: "config", ObjectID: fmt.Sprintf("%d-%d", wkr, i),
					After: map[string]any{"key": "k", "value": i},
				}); err != nil {
					errCh <- err
					return
				}
			}
		}(wkr)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	entries := s.Entries()
	if len(entries) != writers*perWriter {
		t.Fatalf("rows = %d", len(entries))
	}
	for i, e := range entries {
		if e.Seq != int64(i+1) {
			t.Fatalf("seq %d at index %d", e.Seq, i)
		}
	}
	res, err := VerifyChain(context.Background(), s, VerifyOptions{})
	if err != nil || !res.OK {
		t.Fatalf("chain invalid: %+v %v", res.FirstBreak, err)
	}
}

// TestSingleBitMutationBreaksVerify is the rapid property: flipping any single
// bit in a persisted payload or hash must make verification fail.
func TestSingleBitMutationBreaksVerify(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		s := NewMemoryStore()
		n := rapid.IntRange(2, 6).Draw(t, "rows")
		for i := 0; i < n; i++ {
			if _, err := s.Append(context.Background(), row(i)); err != nil {
				t.Fatalf("append: %v", err)
			}
		}
		idx := rapid.IntRange(0, n-1).Draw(t, "index")
		field := rapid.IntRange(0, 2).Draw(t, "field")
		bit := rapid.IntRange(0, 7).Draw(t, "bit")
		switch field {
		case 0:
			raw := []byte(s.entries[idx].Row.After)
			if len(raw) == 0 {
				return
			}
			pos := rapid.IntRange(0, len(raw)-1).Draw(t, "pos")
			raw[pos] ^= 1 << bit
			s.entries[idx].Row.After = raw
		case 1:
			raw := []byte(s.entries[idx].Row.Actor)
			if len(raw) == 0 {
				return
			}
			pos := rapid.IntRange(0, len(raw)-1).Draw(t, "pos")
			raw[pos] ^= 1 << bit
			s.entries[idx].Row.Actor = string(raw)
		default:
			raw := []byte(s.entries[idx].Hash)
			pos := rapid.IntRange(0, len(raw)-1).Draw(t, "pos")
			if raw[pos] == 'f' {
				raw[pos] = '0'
			} else {
				raw[pos] = 'f'
			}
			s.entries[idx].Hash = string(raw)
		}
		res, err := VerifyChain(context.Background(), s, VerifyOptions{})
		if err != nil {
			t.Fatalf("verify error: %v", err)
		}
		if res.OK {
			t.Fatalf("mutation of row %d field %d was not detected", idx, field)
		}
	})
}

func TestVerifyFromSequence(t *testing.T) {
	s := NewMemoryStore()
	appendRows(t, s, 10)
	res, err := VerifyChain(context.Background(), s, VerifyOptions{From: 5})
	if err != nil {
		t.Fatal(err)
	}
	if !res.OK || res.Checked != 6 {
		t.Fatalf("from=5 ok=%v checked=%d", res.OK, res.Checked)
	}
}

func TestVerifyThroughputMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
	s := NewMemoryStore()
	// 20k rows keeps the unit test fast; the Postgres integration test covers
	// the 100k rows/s target at 1M rows.
	appendRows(t, s, 20000)
	start := time.Now()
	res, err := VerifyChain(context.Background(), s, VerifyOptions{})
	if err != nil || !res.OK {
		t.Fatalf("verify: %v %+v", err, res.FirstBreak)
	}
	rate := float64(res.Checked) / time.Since(start).Seconds()
	t.Logf("verified %d rows at %.0f rows/s", res.Checked, rate)
}

func BenchmarkChainHash(b *testing.B) {
	r := row(1)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := ChainHash(GenesisHash, r); err != nil {
			b.Fatal(err)
		}
	}
}
