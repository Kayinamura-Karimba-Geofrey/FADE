package audit

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/canonical"
)

// ErrNoHMACKey is returned when a checkpoint is requested without an audit HMAC
// key. Checkpoints are not written unsigned: their whole purpose is to survive
// a rewrite of Postgres.
var ErrNoHMACKey = errors.New("audit: AUDIT_HMAC_KEY is required to write a checkpoint")

// ChainName is the single logical chain in a database.
const ChainName = "fade-audit"

// Checkpoint is a signed statement about the chain head. The digest is an
// HMAC-SHA256 over the canonical JSON of every other field, so it can be
// verified without the database.
type Checkpoint struct {
	SchemaVersion int    `json:"schema_version"`
	Chain         string `json:"chain"`
	Seq           int64  `json:"seq"`
	Hash          string `json:"hash"`
	RowCount      int64  `json:"row_count"`
	CreatedAt     string `json:"created_at"`
	Digest        string `json:"digest"`
}

// checkpointDigestInput is the HMAC pre-image (Digest excluded).
type checkpointDigestInput struct {
	SchemaVersion int    `json:"schema_version"`
	Chain         string `json:"chain"`
	Seq           int64  `json:"seq"`
	Hash          string `json:"hash"`
	RowCount      int64  `json:"row_count"`
	CreatedAt     string `json:"created_at"`
}

// Digest computes the checkpoint HMAC.
func (c Checkpoint) computeDigest(key []byte) (string, error) {
	raw, err := canonical.Marshal(checkpointDigestInput{
		SchemaVersion: c.SchemaVersion, Chain: c.Chain, Seq: c.Seq,
		Hash: c.Hash, RowCount: c.RowCount, CreatedAt: c.CreatedAt,
	})
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, key)
	mac.Write(raw)
	return hex.EncodeToString(mac.Sum(nil)), nil
}

// Sign fills SchemaVersion, Chain and Digest.
func (c *Checkpoint) Sign(key []byte) error {
	if len(key) == 0 {
		return ErrNoHMACKey
	}
	if c.SchemaVersion == 0 {
		c.SchemaVersion = SchemaVersion
	}
	if c.Chain == "" {
		c.Chain = ChainName
	}
	digest, err := c.computeDigest(key)
	if err != nil {
		return err
	}
	c.Digest = digest
	return nil
}

// VerifySignature checks the HMAC in constant time.
func (c Checkpoint) VerifySignature(key []byte) error {
	if len(key) == 0 {
		return ErrNoHMACKey
	}
	want, err := c.computeDigest(key)
	if err != nil {
		return err
	}
	got, err := hex.DecodeString(c.Digest)
	if err != nil {
		return fmt.Errorf("audit: checkpoint digest is not hex: %w", err)
	}
	expected, err := hex.DecodeString(want)
	if err != nil {
		return err
	}
	if !hmac.Equal(got, expected) {
		return fmt.Errorf("audit: checkpoint at seq %d has an invalid HMAC signature", c.Seq)
	}
	return nil
}

// CheckpointCheck is the result of comparing one checkpoint file with the chain.
type CheckpointCheck struct {
	Path     string `json:"path"`
	Seq      int64  `json:"seq"`
	Hash     string `json:"hash"`
	RowCount int64  `json:"row_count"`
	OK       bool   `json:"ok"`
	Detail   string `json:"detail,omitempty"`
}

// CheckpointFileName returns the append-only export path for a day.
func CheckpointFileName(dir string, t time.Time) string {
	return filepath.Join(dir, t.UTC().Format("2006-01-02")+".json")
}

// WriteCheckpointFile atomically writes a checkpoint to the export directory.
// The file is fsynced and renamed, so a crash leaves either the old or the new
// document, never a partial one.
func WriteCheckpointFile(dir string, cp Checkpoint) (string, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", fmt.Errorf("audit: create checkpoint dir: %w", err)
	}
	path := CheckpointFileName(dir, mustParseAt(cp.CreatedAt))
	raw, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return "", err
	}
	raw = append(raw, '\n')
	tmp, err := os.CreateTemp(dir, ".checkpoint-*.tmp")
	if err != nil {
		return "", err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		return "", err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return "", err
	}
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return path, nil
}

func mustParseAt(at string) time.Time {
	t, err := time.Parse(AtFormat, at)
	if err != nil {
		if t2, err2 := time.Parse(time.RFC3339Nano, at); err2 == nil {
			return t2
		}
		return time.Unix(0, 0).UTC()
	}
	return t
}

// LoadCheckpointFiles reads every checkpoint export in dir, sorted by sequence.
func LoadCheckpointFiles(dir string) ([]Checkpoint, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var out []Checkpoint
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, err
		}
		var cp Checkpoint
		if err := json.Unmarshal(raw, &cp); err != nil {
			return nil, fmt.Errorf("audit: parse checkpoint %s: %w", e.Name(), err)
		}
		out = append(out, cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, nil
}

// VerifyCheckpointFiles checks every exported checkpoint against the chain and
// the audit key. A rewritten chain whose rows no longer hash to the checkpointed
// digest fails here even when the rows form a perfectly valid chain.
func VerifyCheckpointFiles(ctx context.Context, store Store, dir string, key []byte) ([]CheckpointCheck, error) {
	files, err := LoadCheckpointFiles(dir)
	if err != nil {
		return nil, err
	}
	out := make([]CheckpointCheck, 0, len(files))
	for _, cp := range files {
		check := CheckpointCheck{Path: CheckpointFileName(dir, mustParseAt(cp.CreatedAt)),
			Seq: cp.Seq, Hash: cp.Hash, RowCount: cp.RowCount}
		if err := cp.VerifySignature(key); err != nil {
			check.Detail = err.Error()
			out = append(out, check)
			continue
		}
		row, err := store.AtSeq(ctx, cp.Seq)
		if err != nil {
			check.Detail = fmt.Sprintf("checkpointed seq %d is missing from the chain: %v", cp.Seq, err)
			out = append(out, check)
			continue
		}
		if row.Hash != cp.Hash {
			check.Detail = fmt.Sprintf("chain hash at seq %d is %s, checkpoint says %s", cp.Seq, row.Hash, cp.Hash)
			out = append(out, check)
			continue
		}
		count, err := store.CountTo(ctx, cp.Seq)
		if err != nil {
			check.Detail = err.Error()
			out = append(out, check)
			continue
		}
		if count != cp.RowCount {
			check.Detail = fmt.Sprintf("chain has %d rows up to seq %d, checkpoint says %d", count, cp.Seq, cp.RowCount)
			out = append(out, check)
			continue
		}
		check.OK = true
		out = append(out, check)
	}
	return out, nil
}

// CheckpointService creates daily checkpoints: it reads the current head,
// signs it, exports it outside Postgres, then appends an AUDIT_CHECKPOINT row.
type CheckpointService struct {
	Store Store
	Key   []byte
	Dir   string
	Clock func() time.Time
	Hooks Hooks
}

// NewCheckpointService builds a service. Dir defaults to audit/checkpoints.
func NewCheckpointService(store Store, key []byte, dir string, hooks Hooks) *CheckpointService {
	if dir == "" {
		dir = "audit/checkpoints"
	}
	return &CheckpointService{Store: store, Key: key, Dir: dir, Clock: time.Now, Hooks: hooks}
}

// Run writes a checkpoint row and export file.
func (s *CheckpointService) Run(ctx context.Context) (Checkpoint, error) {
	if len(s.Key) == 0 {
		return Checkpoint{}, ErrNoHMACKey
	}
	now := time.Now
	if s.Clock != nil {
		now = s.Clock
	}
	head, err := s.Store.Head(ctx)
	if err != nil {
		return Checkpoint{}, err
	}
	count, err := s.Store.CountTo(ctx, head.Seq)
	if err != nil {
		return Checkpoint{}, err
	}
	cp := Checkpoint{
		SchemaVersion: SchemaVersion, Chain: ChainName,
		Seq: head.Seq, Hash: head.Hash, RowCount: count,
		CreatedAt: now().UTC().Format(time.RFC3339Nano),
	}
	if err := cp.Sign(s.Key); err != nil {
		return Checkpoint{}, err
	}
	path, err := WriteCheckpointFile(s.Dir, cp)
	if err != nil {
		return Checkpoint{}, err
	}
	// The checkpoint row itself extends the chain; it records the signed head
	// as it was before this row was appended.
	if err := s.Store.InsertCheckpoint(ctx, cp, path); err != nil {
		return Checkpoint{}, err
	}
	_, err = s.Store.Append(ctx, Row{
		Actor: "system:scheduler", Action: string(ActionAuditCheckpoint),
		ObjectType: "audit_log", ObjectID: fmt.Sprintf("%d", cp.Seq),
		After: mustRaw(cp), At: NormalizeAt(now()),
	})
	if err != nil {
		return Checkpoint{}, err
	}
	if s.Hooks.OnCheckpoint != nil {
		s.Hooks.OnCheckpoint(0)
	}
	return cp, nil
}

func mustRaw(v any) json.RawMessage {
	raw, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage("null")
	}
	return raw
}
