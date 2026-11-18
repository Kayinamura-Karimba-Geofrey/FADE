package auth

import (
	"strings"
	"testing"
	"time"
)

func TestPasswordHashRoundTrip(t *testing.T) {
	hash, err := HashPassword("correct horse battery staple")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(hash, "$argon2id$v=19$m=65536,t=2,p=4$") {
		t.Fatalf("unexpected hash format %q", hash)
	}
	if !VerifyPassword("correct horse battery staple", hash) {
		t.Fatal("the correct password must verify")
	}
	if VerifyPassword("wrong password", hash) {
		t.Fatal("a wrong password must not verify")
	}
	// Two hashes of the same password differ because the salt is random.
	second, err := HashPassword("correct horse battery staple")
	if err != nil {
		t.Fatal(err)
	}
	if second == hash {
		t.Fatal("salts must be random")
	}
}

func TestVerifyPasswordRejectsMalformedHashes(t *testing.T) {
	for _, hash := range []string{
		"",
		"plain-text",
		"$argon2id$v=19$m=65536,t=2,p=4$onlysalt",
		"$argon2id$v=19$m=65536,t=2,p=4$c2FsdA$!!!notbase64",
		"$bcrypt$v=19$m=65536,t=2,p=4$c2FsdA$c2FsdA",
		"$argon2id$v=19$m=0,t=0,p=0$c2FsdA$c2FsdA",
	} {
		if VerifyPassword("anything", hash) {
			t.Errorf("hash %q must not verify", hash)
		}
	}
}

func TestAccessTokenRoundTrip(t *testing.T) {
	secret := []byte("test-secret")
	tok, err := IssueAccessToken(secret, "user-1", RoleAnalyst, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := ParseAccessToken(secret, tok)
	if err != nil {
		t.Fatal(err)
	}
	if claims.UserID != "user-1" || claims.Role != RoleAnalyst {
		t.Fatalf("claims = %+v", claims)
	}
	if _, err := ParseAccessToken([]byte("other-secret"), tok); err == nil {
		t.Fatal("a token signed with another secret must not parse")
	}
	expired, err := IssueAccessToken(secret, "user-1", RoleAnalyst, -time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseAccessToken(secret, expired); err == nil {
		t.Fatal("an expired token must not parse")
	}
}

func TestRefreshTokenHash(t *testing.T) {
	raw, hash, err := NewRefreshToken()
	if err != nil {
		t.Fatal(err)
	}
	if raw == hash || len(raw) != 64 {
		t.Fatalf("raw=%q hash=%q", raw, hash)
	}
	if HashRefreshToken(raw) != hash {
		t.Fatal("hash must be derivable from the raw token")
	}
	if HashRefreshToken("another") == hash {
		t.Fatal("distinct tokens must not share a hash")
	}
}
