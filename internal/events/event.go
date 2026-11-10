// Package events defines the immutable Phase 1 transaction contract and the
// strict validator used by the Phase 2 ingestion path.
package events

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Transaction types.
const (
	P2PSend      = "P2P_SEND"
	CashIn       = "CASH_IN"
	CashOut      = "CASH_OUT"
	Airtime      = "AIRTIME"
	BillPay      = "BILL_PAY"
	MerchantPay  = "MERCHANT_PAY"
	BankTransfer = "BANK_TRANSFER"
)

// Channels.
const (
	ChannelUSSD  = "USSD"
	ChannelApp   = "APP"
	ChannelAgent = "AGENT"
	ChannelAPI   = "API"
)

// Geo is the sender position at event time.
type Geo struct {
	Lat    float64 `json:"lat"`
	Lon    float64 `json:"lon"`
	CellID string  `json:"cell_id"`
}

// Sender is the wallet that initiates the transaction.
type Sender struct {
	AccountID      string `json:"account_id"`
	MSISDNHash     string `json:"msisdn_hash"`
	DeviceID       string `json:"device_id"`
	SIMSerialHash  string `json:"sim_serial_hash"`
	Geo            Geo    `json:"geo"`
	AccountAgeDays int    `json:"account_age_days"`
	KYCTier        int    `json:"kyc_tier"`
	BalanceBefore  int64  `json:"balance_before"`
}

// Recipient is the counterparty.
type Recipient struct {
	AccountID         string `json:"account_id"`
	MSISDNHash        string `json:"msisdn_hash"`
	IsAgent           bool   `json:"is_agent"`
	FirstSeenBySender bool   `json:"first_seen_by_sender"`
}

// Event is exactly the Phase 1 JSON contract. Unknown fields are rejected by
// Parse; do not add tags without updating docs/dataset-contract.md.
type Event struct {
	EventID   string    `json:"event_id"`
	EventTime string    `json:"event_time"`
	TxnType   string    `json:"txn_type"`
	Amount    int64     `json:"amount"`
	Currency  string    `json:"currency"`
	Fee       int64     `json:"fee"`
	Sender    Sender    `json:"sender"`
	Recipient Recipient `json:"recipient"`
	Channel   string    `json:"channel"`
	AgentID   *string   `json:"agent_id"`
	SessionID string    `json:"session_id"`

	// EventTimeMS is derived during Parse and is never serialised.
	EventTimeMS int64 `json:"-"`
}

// TxnTypes is the closed set from the frozen contract.
var TxnTypes = map[string]bool{
	P2PSend: true, CashIn: true, CashOut: true, Airtime: true,
	BillPay: true, MerchantPay: true, BankTransfer: true,
}

var Channels = map[string]bool{
	ChannelUSSD: true, ChannelApp: true, ChannelAgent: true, ChannelAPI: true,
}

var (
	reULID    = regexp.MustCompile(`^[0-9A-HJKMNP-TV-Z]{26}$`)
	reTime    = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$`)
	reMSISDN  = regexp.MustCompile(`^h_[0-9a-f]{32}$`)
	reSIM     = regexp.MustCompile(`^sim_[0-9a-f]{32}$`)
	reDevice  = regexp.MustCompile(`^dev_\d{6}_\d$`)
	reCell    = regexp.MustCompile(`^[A-Z]{3}-\d{2}$`)
	reAccount = regexp.MustCompile(`^(acct_\d{6}|spec_[a-z]+_\d{3})$`)
	reSession = regexp.MustCompile(`^(p2p|agt|top|bil|pos|bnk)-[0-9A-Z]{8}$`)
)

// IsOutbound reports whether the sender's wallet is debited.
func (e *Event) IsOutbound() bool { return e.TxnType != CashIn }

// CreditsRecipient reports whether the recipient wallet is credited.
func (e *Event) CreditsRecipient() bool { return e.TxnType != CashOut }

// AgentAccountID returns the agent involved, if any.
func (e *Event) AgentAccountID() string {
	if e.AgentID != nil {
		return *e.AgentID
	}
	if e.Recipient.IsAgent {
		return e.Recipient.AccountID
	}
	return ""
}

// FeeForAmount is the Phase 1 fee schedule (charged to the sender).
func FeeForAmount(amount int64) int64 {
	switch {
	case amount < 1_000:
		return 0
	case amount < 5_000:
		return 50
	case amount < 50_000:
		return 100
	case amount < 500_000:
		return 500
	default:
		return 1_000
	}
}

// Reason is a dead-letter reason. The values are part of the topic contract.
type Reason string

const (
	ReasonInvalidJSON   Reason = "INVALID_JSON"
	ReasonInvalidSchema Reason = "INVALID_SCHEMA"
	ReasonInvalidTime   Reason = "INVALID_TIMESTAMP"
	ReasonTooOld        Reason = "EVENT_TOO_OLD"
	ReasonFromFuture    Reason = "EVENT_FROM_FUTURE"
	ReasonLate          Reason = "LATE"
	ReasonDuplicate     Reason = "DUPLICATE"
)

// ValidationError carries a machine-readable reason and a safe detail string.
type ValidationError struct {
	Reason Reason
	Detail string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Reason, e.Detail)
}

func invalid(reason Reason, format string, args ...any) *ValidationError {
	return &ValidationError{Reason: reason, Detail: fmt.Sprintf(format, args...)}
}

// Parse decodes one event with unknown-field rejection and validates every
// invariant that does not need Redis state.
func Parse(data []byte, ingestTime time.Time) (*Event, *ValidationError) {
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	var ev Event
	if err := dec.Decode(&ev); err != nil {
		return nil, invalid(ReasonInvalidJSON, "decode: %v", err)
	}
	if dec.More() {
		return nil, invalid(ReasonInvalidJSON, "trailing JSON value")
	}
	if err := ev.Validate(ingestTime); err != nil {
		return nil, err
	}
	return &ev, nil
}

// Validate checks the frozen schema and ingest-time bounds.
func (e *Event) Validate(ingestTime time.Time) *ValidationError {
	if !reULID.MatchString(e.EventID) {
		return invalid(ReasonInvalidSchema, "event_id is not a 26-char Crockford ULID")
	}
	if !reTime.MatchString(e.EventTime) {
		return invalid(ReasonInvalidTime, "event_time must be YYYY-MM-DDTHH:MM:SS.mmmZ")
	}
	ts, err := time.Parse("2006-01-02T15:04:05.000Z", e.EventTime)
	if err != nil {
		return invalid(ReasonInvalidTime, "parse event_time: %v", err)
	}
	e.EventTimeMS = ts.UnixMilli()
	if ulidMS, ok := ULIDTimestampMS(e.EventID); !ok || ulidMS != e.EventTimeMS {
		return invalid(ReasonInvalidTime, "ULID timestamp does not equal event_time")
	}
	if !TxnTypes[e.TxnType] {
		return invalid(ReasonInvalidSchema, "unknown txn_type %q", e.TxnType)
	}
	if !Channels[e.Channel] {
		return invalid(ReasonInvalidSchema, "unknown channel %q", e.Channel)
	}
	if e.Amount <= 0 || e.Currency != "RWF" || e.Fee < 0 {
		return invalid(ReasonInvalidSchema, "amount > 0, currency RWF and fee >= 0 required")
	}
	if e.Fee != FeeForAmount(e.Amount) {
		return invalid(ReasonInvalidSchema, "fee %d does not match fee schedule for amount %d", e.Fee, e.Amount)
	}
	if e.SessionID == "" || !reSession.MatchString(e.SessionID) {
		return invalid(ReasonInvalidSchema, "invalid session_id")
	}
	if !reAccount.MatchString(e.Sender.AccountID) || !reAccount.MatchString(e.Recipient.AccountID) {
		return invalid(ReasonInvalidSchema, "invalid account_id")
	}
	if !reMSISDN.MatchString(e.Sender.MSISDNHash) || !reMSISDN.MatchString(e.Recipient.MSISDNHash) {
		return invalid(ReasonInvalidSchema, "invalid msisdn_hash")
	}
	if !reDevice.MatchString(e.Sender.DeviceID) {
		return invalid(ReasonInvalidSchema, "invalid device_id")
	}
	if !reSIM.MatchString(e.Sender.SIMSerialHash) {
		return invalid(ReasonInvalidSchema, "invalid sim_serial_hash")
	}
	if !reCell.MatchString(e.Sender.Geo.CellID) {
		return invalid(ReasonInvalidSchema, "invalid cell_id")
	}
	if e.Sender.Geo.Lat < -2.90 || e.Sender.Geo.Lat > -1.00 ||
		e.Sender.Geo.Lon < 28.80 || e.Sender.Geo.Lon > 30.60 {
		return invalid(ReasonInvalidSchema, "sender geo outside Rwanda bounding box")
	}
	if e.Sender.AccountAgeDays < 0 || e.Sender.KYCTier < 1 || e.Sender.KYCTier > 3 || e.Sender.BalanceBefore < 0 {
		return invalid(ReasonInvalidSchema, "invalid sender account_age_days/kyc_tier/balance_before")
	}
	if err := e.validateAgentRules(); err != nil {
		return err
	}

	if !ingestTime.IsZero() {
		ingestMS := ingestTime.UTC().UnixMilli()
		if e.EventTimeMS < ingestMS-24*60*60*1000 {
			return invalid(ReasonTooOld, "event_time is more than 24h before ingest_time")
		}
		if e.EventTimeMS > ingestMS+5*60*1000 {
			return invalid(ReasonFromFuture, "event_time is more than 5m after ingest_time")
		}
	}
	return nil
}

func (e *Event) validateAgentRules() *ValidationError {
	cash := e.TxnType == CashIn || e.TxnType == CashOut
	if cash {
		if e.Channel != ChannelAgent {
			return invalid(ReasonInvalidSchema, "CASH_IN/CASH_OUT must use channel AGENT")
		}
		if e.AgentID == nil || *e.AgentID != e.Recipient.AccountID || !e.Recipient.IsAgent {
			return invalid(ReasonInvalidSchema, "agent_id must equal an agent recipient for CASH_IN/CASH_OUT")
		}
		return nil
	}
	if e.AgentID != nil {
		return invalid(ReasonInvalidSchema, "agent_id must be null for non-agent transactions")
	}
	if e.Recipient.IsAgent && e.TxnType != BankTransfer {
		return invalid(ReasonInvalidSchema, "recipient.is_agent is only valid for agent transactions or bank-transfer float credits")
	}
	return nil
}

// CheckLate applies the per-account watermark rule. It is called by the
// worker after the atomic script has read the watermark, so the same rule is
// also enforced inside Redis.
func CheckLate(eventMS, watermarkMS int64, tolerance time.Duration) bool {
	return eventMS < watermarkMS-tolerance.Milliseconds()
}

// ULIDTimestampMS decodes the 48-bit millisecond timestamp from a ULID.
func ULIDTimestampMS(id string) (int64, bool) {
	if len(id) != 26 {
		return 0, false
	}
	const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	var v uint64
	for i := 0; i < 10; i++ {
		idx := strings.IndexByte(alphabet, id[i])
		if idx < 0 {
			return 0, false
		}
		v = v<<5 | uint64(idx)
	}
	// 10 Crockford characters carry 50 bits; the top two are padding.
	return int64(v & 0xFFFFFFFFFFFF), true
}
