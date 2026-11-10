package events

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func goldenEventJSON() []byte {
	ev := map[string]any{
		"event_id":   "01KDWCY10Q768RM6TMTHZS5X82",
		"event_time": "2026-01-01T09:06:31.575Z",
		"txn_type":   "P2P_SEND",
		"amount":     5000,
		"currency":   "RWF",
		"fee":        100,
		"sender": map[string]any{
			"account_id": "acct_001160", "msisdn_hash": "h_72f288c3b5d3bf73876cf9477decd11b",
			"device_id": "dev_001160_0", "sim_serial_hash": "sim_33118b02606d997bc86e39011659742c",
			"geo":              map[string]any{"lat": -1.51, "lon": 29.654, "cell_id": "MSZ-04"},
			"account_age_days": 648, "kyc_tier": 2, "balance_before": 298700,
		},
		"recipient": map[string]any{
			"account_id": "spec_telecom_000", "msisdn_hash": "h_1349fa8dbd76f209b7d64891e86ff70c",
			"is_agent": false, "first_seen_by_sender": true,
		},
		"channel": "USSD", "agent_id": nil, "session_id": "top-VEN9RZED",
	}
	raw, _ := json.Marshal(ev)
	return raw
}

func TestParseValidEvent(t *testing.T) {
	ev, err := Parse(goldenEventJSON(), time.Date(2026, 1, 1, 9, 6, 31, 0, time.UTC))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if ev.Amount != 5000 || ev.EventTimeMS != 1767258391575 {
		t.Fatalf("bad parse: %+v", ev)
	}
	if ev.IsOutbound() != true || ev.CreditsRecipient() != true {
		t.Fatal("direction flags")
	}
	if ev.AgentAccountID() != "" {
		t.Fatal("unexpected agent")
	}
}

func TestParseRejectsUnknownField(t *testing.T) {
	raw := strings.Replace(string(goldenEventJSON()), `"channel":"USSD"`, `"channel":"USSD","label":"FRAUD"`, 1)
	_, err := Parse([]byte(raw), time.Time{})
	if err == nil || err.Reason != ReasonInvalidJSON {
		t.Fatalf("want INVALID_JSON, got %v", err)
	}
}

func TestValidationRejectsBadFields(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(map[string]any)
		want   Reason
	}{
		{"ulid", func(m map[string]any) { m["event_id"] = "bad" }, ReasonInvalidSchema},
		{"time_format", func(m map[string]any) { m["event_time"] = "2026-01-01T09:06:31Z" }, ReasonInvalidTime},
		{"ulid_time", func(m map[string]any) {
			m["event_id"] = "01KDWCY10Q768RM6TMTHZS5X82"
			m["event_time"] = "2026-01-01T09:06:31.576Z"
		}, ReasonInvalidTime},
		{"type", func(m map[string]any) { m["txn_type"] = "NOPE" }, ReasonInvalidSchema},
		{"fee", func(m map[string]any) { m["fee"] = 1 }, ReasonInvalidSchema},
		{"amount", func(m map[string]any) { m["amount"] = 0 }, ReasonInvalidSchema},
		{"geo", func(m map[string]any) {
			m["sender"].(map[string]any)["geo"].(map[string]any)["lat"] = 0.0
		}, ReasonInvalidSchema},
		{"agent", func(m map[string]any) {
			m["recipient"].(map[string]any)["is_agent"] = true
		}, ReasonInvalidSchema},
		{"too_old", func(m map[string]any) {}, ReasonTooOld},
		{"future", func(m map[string]any) {}, ReasonFromFuture},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var m map[string]any
			if err := json.Unmarshal(goldenEventJSON(), &m); err != nil {
				t.Fatal(err)
			}
			tc.mutate(m)
			raw, _ := json.Marshal(m)
			ingest := time.Date(2026, 1, 1, 9, 6, 31, 0, time.UTC)
			if tc.name == "too_old" {
				ingest = ingest.Add(25 * time.Hour)
			}
			if tc.name == "future" {
				ingest = ingest.Add(-6 * time.Minute)
			}
			_, err := Parse(raw, ingest)
			if err == nil || err.Reason != tc.want {
				t.Fatalf("want %s, got %v", tc.want, err)
			}
		})
	}
}

func TestCashRulesAndAgent(t *testing.T) {
	var m map[string]any
	_ = json.Unmarshal(goldenEventJSON(), &m)
	m["txn_type"] = "CASH_IN"
	m["channel"] = "AGENT"
	agent := "spec_agent_000"
	m["agent_id"] = agent
	m["recipient"].(map[string]any)["account_id"] = agent
	m["recipient"].(map[string]any)["is_agent"] = true
	raw, _ := json.Marshal(m)
	ev, err := Parse(raw, time.Time{})
	if err != nil {
		t.Fatalf("cash-in: %v", err)
	}
	if ev.IsOutbound() || !ev.CreditsRecipient() || ev.AgentAccountID() != agent {
		t.Fatal("cash-in direction/agent")
	}
}

func TestFeeSchedule(t *testing.T) {
	cases := map[int64]int64{999: 0, 1000: 50, 4999: 50, 5000: 100, 49999: 100, 50000: 500, 499999: 500, 500000: 1000}
	for amount, want := range cases {
		if got := FeeForAmount(amount); got != want {
			t.Errorf("fee(%d)=%d want %d", amount, got, want)
		}
	}
}

func TestCheckLate(t *testing.T) {
	if CheckLate(1_000_000, 1_060_000, time.Minute) {
		t.Fatal("exactly 60s late must be accepted")
	}
	if !CheckLate(999_999, 1_060_000, time.Minute) {
		t.Fatal("more than 60s late must be rejected")
	}
}

func TestValidationRejectsEveryContractField(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(map[string]any)
		want   Reason
	}{
		{"channel", func(m map[string]any) { m["channel"] = "SMS" }, ReasonInvalidSchema},
		{"currency", func(m map[string]any) { m["currency"] = "USD" }, ReasonInvalidSchema},
		{"fee_negative", func(m map[string]any) { m["fee"] = -1 }, ReasonInvalidSchema},
		{"session", func(m map[string]any) { m["session_id"] = "nope" }, ReasonInvalidSchema},
		{"sender_account", func(m map[string]any) { m["sender"].(map[string]any)["account_id"] = "x" }, ReasonInvalidSchema},
		{"recipient_account", func(m map[string]any) { m["recipient"].(map[string]any)["account_id"] = "x" }, ReasonInvalidSchema},
		{"sender_msisdn", func(m map[string]any) { m["sender"].(map[string]any)["msisdn_hash"] = "x" }, ReasonInvalidSchema},
		{"recipient_msisdn", func(m map[string]any) { m["recipient"].(map[string]any)["msisdn_hash"] = "x" }, ReasonInvalidSchema},
		{"device", func(m map[string]any) { m["sender"].(map[string]any)["device_id"] = "x" }, ReasonInvalidSchema},
		{"sim", func(m map[string]any) { m["sender"].(map[string]any)["sim_serial_hash"] = "x" }, ReasonInvalidSchema},
		{"cell", func(m map[string]any) { m["sender"].(map[string]any)["geo"].(map[string]any)["cell_id"] = "x" }, ReasonInvalidSchema},
		{"age", func(m map[string]any) { m["sender"].(map[string]any)["account_age_days"] = -1 }, ReasonInvalidSchema},
		{"kyc", func(m map[string]any) { m["sender"].(map[string]any)["kyc_tier"] = 4 }, ReasonInvalidSchema},
		{"balance", func(m map[string]any) { m["sender"].(map[string]any)["balance_before"] = -1 }, ReasonInvalidSchema},
		{"agent_id_non_agent", func(m map[string]any) { m["agent_id"] = "spec_agent_000" }, ReasonInvalidSchema},
		{"cash_channel", func(m map[string]any) {
			m["txn_type"] = "CASH_IN"
			m["channel"] = "APP"
			m["recipient"].(map[string]any)["is_agent"] = true
		}, ReasonInvalidSchema},
		{"cash_agent_mismatch", func(m map[string]any) {
			m["txn_type"] = "CASH_OUT"
			m["channel"] = "AGENT"
			m["recipient"].(map[string]any)["is_agent"] = true
			m["agent_id"] = "spec_agent_000"
		}, ReasonInvalidSchema},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var m map[string]any
			if err := json.Unmarshal(goldenEventJSON(), &m); err != nil {
				t.Fatal(err)
			}
			tc.mutate(m)
			raw, _ := json.Marshal(m)
			if _, err := Parse(raw, time.Time{}); err == nil || err.Reason != tc.want {
				t.Fatalf("want %s, got %v", tc.want, err)
			}
		})
	}
}

func TestParseRejectsTrailingJSON(t *testing.T) {
	raw := append(goldenEventJSON(), []byte(` {}`)...)
	if _, err := Parse(raw, time.Time{}); err == nil || err.Reason != ReasonInvalidJSON {
		t.Fatalf("want INVALID_JSON, got %v", err)
	}
}

func TestAgentAccountIDFromRecipient(t *testing.T) {
	ev := &Event{TxnType: BankTransfer, Recipient: Recipient{AccountID: "spec_agent_001", IsAgent: true}}
	if got := ev.AgentAccountID(); got != "spec_agent_001" {
		t.Fatalf("agent = %q", got)
	}
	if (&ValidationError{Reason: ReasonLate, Detail: "x"}).Error() != "LATE: x" {
		t.Fatal("validation error string")
	}
}

func TestULIDTimestampMSRejectsInvalid(t *testing.T) {
	if _, ok := ULIDTimestampMS("bad"); ok {
		t.Fatal("invalid ULID accepted")
	}
	if _, ok := ULIDTimestampMS("01KDWCY10Q768RM6TMTHZS5X82"); !ok {
		t.Fatal("valid ULID rejected")
	}
}
