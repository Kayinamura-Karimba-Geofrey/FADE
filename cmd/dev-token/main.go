// Command dev-token mints a short-lived JWT for local decision-api testing.
// It is never deployed: the API only needs JWT_SECRET to validate tokens.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/api"
)

func main() {
	role := flag.String("role", "service", "role: service, analyst or admin")
	subject := flag.String("sub", "local-dev", "token subject")
	ttl := flag.Duration("ttl", time.Hour, "token lifetime")
	secret := flag.String("secret", os.Getenv("JWT_SECRET"), "HS256 secret (default JWT_SECRET)")
	issuer := flag.String("issuer", os.Getenv("JWT_ISSUER"), "issuer claim")
	flag.Parse()

	if *secret == "" {
		fmt.Fprintln(os.Stderr, "JWT_SECRET is empty: pass -secret or export JWT_SECRET")
		os.Exit(2)
	}
	auth := api.NewAuth(api.AuthConfig{Secret: *secret, Issuer: *issuer})
	token, err := auth.SignToken(*subject, *role, *ttl)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	fmt.Println(token)
}
