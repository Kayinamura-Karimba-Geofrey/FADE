// Package audit implements FADE's tamper-evident governance log.
//
// Every row is a link in a single, database-wide SHA-256 hash chain:
//
//	hash = SHA-256( prev_hash || canonical_json({ actor, action, object_type,
//	              object_id, before, after, at, request_id, source_ip_hash }) )
//
// The pre-image is canonicalised with RFC 8785 (internal/canonical), so the same
// row always hashes identically regardless of key order or formatting. Rows are
// append-only: the application role has INSERT/SELECT only, and a trigger
// rejects UPDATE, DELETE and TRUNCATE even for the owner. Daily checkpoints
// (with an HMAC-signed digest exported outside Postgres) make truncation and
// chain rebuilding detectable.
package audit

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/canonical"
)

// SchemaVersion is the audit row contract version. It is recorded in every
// checkpoint so a verifier can detect an algorithm change.
const SchemaVersion = 1

// GenesisSeed is the documented genesis pre-image for a new chain.
const GenesisSeed = "FADE-AUDIT-GENESIS-v1"

// GenesisHash is SHA-256("FADE-AUDIT-GENESIS-v1"), the prev_hash of the first
// row of a chain created by this version of FADE.
var GenesisHash = func() string {
	sum := sha256.Sum256([]byte(GenesisSeed))
	return hex.EncodeToString(sum[:])
}()

// AtFormat is the fixed timestamp format used in the hash pre-image. Postgres
// stores TIMESTAMPTZ at microsecond precision, so the format is fixed to
// microseconds to keep the recomputed hash byte-identical after a round trip.
const AtFormat = "2006-01-02T15:04:05.000000Z"

// NormalizeAt renders t in the chain's fixed UTC microsecond format.
func NormalizeAt(t time.Time) string { return t.UTC().Truncate(time.Microsecond).Format(AtFormat) }

// Action is one audit action code from the governance catalog.
type Action string

// The audit event catalog. Adding an action requires updating docs/audit.md.
const (
	ActionRulesetActivate      Action = "RULESET_ACTIVATE"
	ActionRulesetReject        Action = "RULESET_REJECT"
	ActionRuleUpdate           Action = "RULE_UPDATE"
	ActionCombinerConfigUpdate Action = "COMBINER_CONFIG_UPDATE"
	ActionModelPromote         Action = "MODEL_PROMOTE"
	ActionModelRetire          Action = "MODEL_RETIRE"
	ActionListAdd              Action = "LIST_ADD"
	ActionListRemove           Action = "LIST_REMOVE"
	ActionCaseAssign           Action = "CASE_ASSIGN"
	ActionCaseResolve          Action = "CASE_RESOLVE"
	ActionLabelWrite           Action = "LABEL_WRITE"
	ActionUserRoleChange       Action = "USER_ROLE_CHANGE"
	ActionDecisionOverride     Action = "DECISION_OVERRIDE_MANUAL"
	ActionRetentionPurge       Action = "RETENTION_PURGE"
	ActionAuditVerify          Action = "AUDIT_VERIFY"
	ActionAuthFailureBurst     Action = "AUTH_FAILURE_BURST"
	ActionConfigChange         Action = "CONFIG_CHANGE"
	ActionAuditCheckpoint      Action = "AUDIT_CHECKPOINT"
)

// Catalog is the closed set of action codes accepted by the writer.
var Catalog = map[Action]bool{
	ActionRulesetActivate: true, ActionRulesetReject: true, ActionRuleUpdate: true,
	ActionCombinerConfigUpdate: true, ActionModelPromote: true, ActionModelRetire: true,
	ActionListAdd: true, ActionListRemove: true, ActionCaseAssign: true,
	ActionCaseResolve: true, ActionLabelWrite: true, ActionUserRoleChange: true,
	ActionDecisionOverride: true, ActionRetentionPurge: true, ActionAuditVerify: true,
	ActionAuthFailureBurst: true, ActionConfigChange: true, ActionAuditCheckpoint: true,
}

// Row is one audit entry. The field set is the hash pre-image contract: do not
// add or remove fields without bumping SchemaVersion and documenting a verifier
// upgrade. Prev_hash is stored alongside the row but is not part of the
// canonical pre-image; it is the ASCII prefix.
type Row struct {
	Actor        string          `json:"actor"`
	Action       string          `json:"action"`
	ObjectType   string          `json:"object_type"`
	ObjectID     string          `json:"object_id"`
	Before       json.RawMessage `json:"before"`
	After        json.RawMessage `json:"after"`
	At           string          `json:"at"`
	RequestID    string          `json:"request_id"`
	SourceIPHash string          `json:"source_ip_hash"`
}

// NewRow builds a row stamped at t. The timestamp is normalized to the fixed
// UTC microsecond layout so a Postgres round-trip reproduces the same hash.
func NewRow(actor, action, objectType, objectID string, before, after []byte, t time.Time) Row {
	return Row{
		Actor: actor, Action: action, ObjectType: objectType, ObjectID: objectID,
		Before: before, After: after, At: NormalizeAt(t),
	}
}

// Entry is a row with its chain position.
type Entry struct {
	Seq      int64  `json:"seq"`
	PrevHash string `json:"prev_hash"`
	Hash     string `json:"hash"`
	Row      Row    `json:"row"`
}

// Input is the caller-facing form of an audit entry. Before/after are arbitrary
// values; the writer redacts them against the object_type allowlist before they
// are canonicalised.
type Input struct {
	Actor        string
	Action       Action
	ObjectType   string
	ObjectID     string
	Before       any
	After        any
	RequestID    string
	SourceIPHash string
	At           time.Time
}

// Hooks expose writer and verifier telemetry without importing internal/obs.
type Hooks struct {
	OnAppend     func(action string, seconds float64, err error)
	OnVerify     func(ok bool, seconds float64)
	OnCheckpoint func(age time.Duration)
	OnChainState func(ok bool)
}

// JSON serialises the row for storage and API responses. It is deliberately
// stable: the stored representation uses the same field names as the pre-image.
func (r Row) JSON() ([]byte, error) {
	type wire struct {
		Actor        string          `json:"actor"`
		Action       string          `json:"action"`
		ObjectType   string          `json:"object_type"`
		ObjectID     string          `json:"object_id"`
		Before       json.RawMessage `json:"before"`
		After        json.RawMessage `json:"after"`
		At           string          `json:"at"`
		RequestID    string          `json:"request_id"`
		SourceIPHash string          `json:"source_ip_hash"`
	}
	return json.Marshal(wire{
		Actor: r.Actor, Action: r.Action, ObjectType: r.ObjectType, ObjectID: r.ObjectID,
		Before: nullRaw(r.Before), After: nullRaw(r.After), At: r.At,
		RequestID: r.RequestID, SourceIPHash: r.SourceIPHash,
	})
}

func nullRaw(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage("null")
	}
	return raw
}

// PreImage returns the canonical JSON bytes that are hashed.
func (r Row) PreImage() ([]byte, error) {
	if r.At == "" {
		return nil, fmt.Errorf("audit: row at is required")
	}
	raw, err := r.JSON()
	if err != nil {
		return nil, err
	}
	return canonical.JSON(raw)
}

// ChainHash computes SHA-256(prev_hash || canonical(row)).
func ChainHash(prev string, row Row) (string, error) {
	pre, err := row.PreImage()
	if err != nil {
		return "", err
	}
	h := sha256.New()
	h.Write([]byte(prev))
	h.Write(pre)
	return hex.EncodeToString(h.Sum(nil)), nil
}

// VerifyLink checks prev linkage and the recomputed hash for one entry.
func VerifyLink(expectedPrev string, e Entry) error {
	if e.PrevHash != expectedPrev {
		return &BreakError{Seq: e.Seq, Kind: BreakPrevHash,
			Detail: fmt.Sprintf("prev_hash %q does not match predecessor %q", e.PrevHash, expectedPrev)}
	}
	want, err := ChainHash(expectedPrev, e.Row)
	if err != nil {
		return err
	}
	if e.Hash != want {
		return &BreakError{Seq: e.Seq, Kind: BreakRowHash,
			Detail: fmt.Sprintf("stored hash %s does not match recomputed %s", e.Hash, want)}
	}
	return nil
}

// BreakKind classifies the first failure in a chain.
type BreakKind string

// Break kinds.
const (
	BreakSeqGap     BreakKind = "SEQ_GAP"
	BreakPrevHash   BreakKind = "PREV_HASH_MISMATCH"
	BreakRowHash    BreakKind = "ROW_HASH_MISMATCH"
	BreakCheckpoint BreakKind = "CHECKPOINT_MISMATCH"
	BreakMissingRow BreakKind = "SEQ_MISSING"
)

// BreakError is the first detected chain break.
type BreakError struct {
	Seq    int64     `json:"seq"`
	Kind   BreakKind `json:"kind"`
	Detail string    `json:"detail"`
}

func (e *BreakError) Error() string {
	return fmt.Sprintf("audit chain broken at seq %d (%s): %s", e.Seq, e.Kind, e.Detail)
}

// VerifyResult reports a verification run.
type VerifyResult struct {
	From          int64             `json:"from_seq"`
	To            int64             `json:"to_seq"`
	Checked       int64             `json:"checked"`
	OK            bool              `json:"ok"`
	LegacyPrelude bool              `json:"legacy_prelude,omitempty"`
	FirstBreak    *BreakError       `json:"first_break,omitempty"`
	Context       []Entry           `json:"context,omitempty"`
	Checkpoints   []CheckpointCheck `json:"checkpoints,omitempty"`
	Seconds       float64           `json:"seconds"`
	RowsPerSec    float64           `json:"rows_per_sec"`
}

// Validate checks an Input against the catalog and object allowlist.
func (in Input) Validate() error {
	if strings.TrimSpace(in.Actor) == "" {
		return fmt.Errorf("audit: actor is required")
	}
	if !Catalog[in.Action] {
		return fmt.Errorf("audit: action %q is not in the audit catalog", in.Action)
	}
	if strings.TrimSpace(in.ObjectType) == "" || strings.TrimSpace(in.ObjectID) == "" {
		return fmt.Errorf("audit: object_type and object_id are required")
	}
	if !KnownObjectType(in.ObjectType) {
		return fmt.Errorf("audit: object_type %q has no redaction allowlist", in.ObjectType)
	}
	if in.SourceIPHash != "" && !looksHashed(in.SourceIPHash) {
		return fmt.Errorf("audit: source_ip_hash must be a hex digest, never a raw IP")
	}
	return nil
}

func looksHashed(s string) bool {
	if len(s) < 16 {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return false
		}
	}
	return true
}
