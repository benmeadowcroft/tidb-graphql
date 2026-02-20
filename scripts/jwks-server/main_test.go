package main

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestBuildServerMux_DevTokenDisabledReturnsNotFound(t *testing.T) {
	mux, err := buildServerMux(serverConfig{
		Issuer:   "https://jwks:9000",
		Audience: []string{"tidb-graphql"},
		KID:      "local-key",
		JWKSPem:  []byte(`{"keys":[]}`),
		DevToken: devTokenConfig{
			Enabled: false,
		},
	})
	if err != nil {
		t.Fatalf("buildServerMux returned error: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/dev/token", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d", http.StatusNotFound, rec.Code)
	}
}

func TestDevTokenHandler_MissingHeaderUnauthorized(t *testing.T) {
	handler, _ := testDevTokenHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/dev/token", strings.NewReader(`{"db_role":"app_viewer"}`))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected status %d, got %d", http.StatusUnauthorized, rec.Code)
	}
	assertJSONError(t, rec.Body.String(), "unauthorized")
}

func TestDevTokenHandler_WrongHeaderUnauthorized(t *testing.T) {
	handler, _ := testDevTokenHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/dev/token", strings.NewReader(`{"db_role":"app_viewer"}`))
	req.Header.Set(adminTokenHeader, "wrong-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected status %d, got %d", http.StatusUnauthorized, rec.Code)
	}
	assertJSONError(t, rec.Body.String(), "unauthorized")
}

func TestDevTokenHandler_ValidRequestReturnsSignedJWT(t *testing.T) {
	handler, key := testDevTokenHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/dev/token", strings.NewReader(`{"subject":"alice","db_role":"app_viewer"}`))
	req.Header.Set(adminTokenHeader, "secret-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d (%s)", http.StatusOK, rec.Code, rec.Body.String())
	}

	var payload devTokenResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode response JSON: %v", err)
	}
	if payload.Token == "" {
		t.Fatalf("expected token in response")
	}
	if payload.ExpiresInSeconds != int64((24 * time.Hour).Seconds()) {
		t.Fatalf("unexpected expires_in_seconds: %d", payload.ExpiresInSeconds)
	}

	claims := parseClaims(t, payload.Token, &key.PublicKey)
	if got := claims["iss"]; got != "https://jwks:9000" {
		t.Fatalf("expected iss https://jwks:9000, got %v", got)
	}
	if got := claims["sub"]; got != "alice" {
		t.Fatalf("expected sub alice, got %v", got)
	}
	if got := claims["db_role"]; got != "app_viewer" {
		t.Fatalf("expected db_role app_viewer, got %v", got)
	}

	aud, ok := claims["aud"].([]interface{})
	if !ok || len(aud) != 1 || aud[0] != "tidb-graphql" {
		t.Fatalf("expected aud [tidb-graphql], got %#v", claims["aud"])
	}
}

func TestDevTokenHandler_CustomTTLRejectedWhenAboveMax(t *testing.T) {
	handler, _ := testDevTokenHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/dev/token", strings.NewReader(`{"expires_in":"240h"}`))
	req.Header.Set(adminTokenHeader, "secret-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}
	assertJSONErrorContains(t, rec.Body.String(), "exceeds maximum")
}

func TestDevTokenHandler_InvalidTTLRejected(t *testing.T) {
	handler, _ := testDevTokenHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/dev/token", strings.NewReader(`{"expires_in":"soon"}`))
	req.Header.Set(adminTokenHeader, "secret-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}
	assertJSONErrorContains(t, rec.Body.String(), "valid duration")
}

func TestDevTokenHandler_AcceptTextPlainReturnsRawToken(t *testing.T) {
	handler, key := testDevTokenHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/dev/token", strings.NewReader(`{"db_role":"app_admin"}`))
	req.Header.Set(adminTokenHeader, "secret-token")
	req.Header.Set("Accept", "text/plain")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d (%s)", http.StatusOK, rec.Code, rec.Body.String())
	}
	body := strings.TrimSpace(rec.Body.String())
	if body == "" {
		t.Fatalf("expected raw JWT in response body")
	}

	claims := parseClaims(t, body, &key.PublicKey)
	if got := claims["sub"]; got != defaultDevTokenSub {
		t.Fatalf("expected default subject %q, got %v", defaultDevTokenSub, got)
	}
	if got := claims["db_role"]; got != "app_admin" {
		t.Fatalf("expected db_role app_admin, got %v", got)
	}
}

func testDevTokenHandler(t *testing.T) (http.Handler, *rsa.PrivateKey) {
	t.Helper()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate private key: %v", err)
	}

	handler, err := newDevTokenHandler(serverConfig{
		Issuer:   "https://jwks:9000",
		Audience: []string{"tidb-graphql"},
		KID:      "local-key",
		DevToken: devTokenConfig{
			Enabled:    true,
			AdminToken: "secret-token",
			PrivateKey: privateKey,
			DefaultTTL: 24 * time.Hour,
			MaxTTL:     7 * 24 * time.Hour,
		},
	})
	if err != nil {
		t.Fatalf("newDevTokenHandler returned error: %v", err)
	}
	return handler, privateKey
}

func parseClaims(t *testing.T, tokenString string, pub *rsa.PublicKey) jwt.MapClaims {
	t.Helper()
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		return pub, nil
	}, jwt.WithValidMethods([]string{"RS256"}))
	if err != nil {
		t.Fatalf("failed to parse token: %v", err)
	}
	if !token.Valid {
		t.Fatalf("expected token to be valid")
	}
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		t.Fatalf("expected map claims, got %T", token.Claims)
	}
	return claims
}

func assertJSONError(t *testing.T, raw string, want string) {
	t.Helper()
	var payload jsonError
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("failed to decode JSON body %q: %v", raw, err)
	}
	if payload.Error != want {
		t.Fatalf("expected error %q, got %q", want, payload.Error)
	}
}

func assertJSONErrorContains(t *testing.T, raw string, wantSubstr string) {
	t.Helper()
	var payload jsonError
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("failed to decode JSON body %q: %v", raw, err)
	}
	if !strings.Contains(payload.Error, wantSubstr) {
		t.Fatalf("expected error containing %q, got %q", wantSubstr, payload.Error)
	}
}
