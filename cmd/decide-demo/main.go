// Command decide-demo sends one valid P2P event to a running decision-api.
// It mints a service token from JWT_SECRET unless -token is supplied.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/api"
)

const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

func ulid(ms int64) string {
	var ts [10]byte
	v := uint64(ms)
	for i := 9; i >= 0; i-- {
		ts[i] = alphabet[v&31]
		v >>= 5
	}
	rest := make([]byte, 16)
	seed := uint64(time.Now().UnixNano())
	for i := range rest {
		seed = seed*6364136223846793005 + 1442695040888963407
		rest[i] = alphabet[(seed>>33)&31]
	}
	return string(ts[:]) + string(rest)
}

func main() {
	base := flag.String("url", "http://localhost:8080", "decision-api base URL")
	tokenFlag := flag.String("token", "", "bearer token (default: mint from JWT_SECRET)")
	secret := flag.String("secret", os.Getenv("JWT_SECRET"), "JWT secret for the demo token")
	amount := flag.Int64("amount", 70000, "transaction amount")
	flag.Parse()

	token := *tokenFlag
	if token == "" {
		auth := api.NewAuth(api.AuthConfig{Secret: *secret, Issuer: os.Getenv("JWT_ISSUER")})
		var err error
		token, err = auth.SignToken("decide-demo", "service", time.Hour)
		if err != nil {
			fmt.Fprintln(os.Stderr, "mint token:", err)
			os.Exit(2)
		}
	}

	now := time.Now().UTC().Truncate(time.Millisecond)
	fee := int64(1000)
	switch {
	case *amount < 1_000:
		fee = 0
	case *amount < 5_000:
		fee = 50
	case *amount < 50_000:
		fee = 100
	case *amount < 500_000:
		fee = 500
	}
	body, _ := json.Marshal(map[string]any{
		"event_id":   ulid(now.UnixMilli()),
		"event_time": now.Format("2006-01-02T15:04:05.000Z"),
		"txn_type":   "P2P_SEND", "amount": *amount, "currency": "RWF", "fee": fee,
		"sender": map[string]any{
			"account_id": "acct_000123", "msisdn_hash": "h_0123456789abcdef0123456789abcdef",
			"device_id": "dev_000123_1", "sim_serial_hash": "sim_0123456789abcdef0123456789abcdef",
			"geo":              map[string]any{"lat": -1.95, "lon": 30.06, "cell_id": "KGL-01"},
			"account_age_days": 120, "kyc_tier": 2, "balance_before": 100000,
		},
		"recipient": map[string]any{
			"account_id": "acct_000456", "msisdn_hash": "h_fedcba9876543210fedcba9876543210",
			"is_agent": false, "first_seen_by_sender": false,
		},
		"channel": "APP", "session_id": "p2p-1234ABCD",
	})

	req, err := http.NewRequest(http.MethodPost, *base+"/v1/decide", bytes.NewReader(body))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	fmt.Printf("HTTP %d\n%s\n", res.StatusCode, raw)
	if res.StatusCode != http.StatusOK {
		os.Exit(1)
	}
}
