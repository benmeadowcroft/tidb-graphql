//go:build integration
// +build integration

package integration

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"

	"tidb-graphql/internal/middleware"
)

func TestOIDCAuthMiddleware(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	privateKey, publicPath := generateKeypair(t)
	jwksServer := newJWKSServer(t, publicPath, "test-key")
	defer jwksServer.Close()

	cfg := middleware.OIDCAuthConfig{
		Enabled:       true,
		IssuerURL:     jwksServer.URL,
		Audience:      "tidb-graphql",
		ClockSkew:     time.Minute,
		SkipTLSVerify: true,
	}

	authMiddleware, err := middleware.OIDCAuthMiddleware(cfg, nil)
	require.NoError(t, err)

	handler := authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	server := httptest.NewServer(handler)
	defer server.Close()

	t.Run("missing token", func(t *testing.T) {
		resp, err := http.Get(server.URL)
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})

	t.Run("valid token", func(t *testing.T) {
		token := mintToken(t, privateKey, jwksServer.URL, cfg.Audience, "test-key", time.Now().Add(time.Hour))
		req, err := http.NewRequest(http.MethodGet, server.URL, nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+token)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("invalid issuer", func(t *testing.T) {
		token := mintToken(t, privateKey, "other-issuer", cfg.Audience, "test-key", time.Now().Add(time.Hour))
		req, err := http.NewRequest(http.MethodGet, server.URL, nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+token)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})
}

func generateKeypair(t *testing.T) (*rsa.PrivateKey, string) {
	t.Helper()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	publicBytes, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	require.NoError(t, err)

	dir := t.TempDir()
	publicPath := dir + "/jwt_public.pem"

	block := &pem.Block{Type: "PUBLIC KEY", Bytes: publicBytes}
	require.NoError(t, os.WriteFile(publicPath, pem.EncodeToMemory(block), 0o600))

	return privateKey, publicPath
}

func mintToken(t *testing.T, privateKey *rsa.PrivateKey, issuer, audience, kid string, expiresAt time.Time) string {
	t.Helper()

	claims := jwt.RegisteredClaims{
		Issuer:    issuer,
		Subject:   "user-1",
		Audience:  jwt.ClaimStrings{audience},
		IssuedAt:  jwt.NewNumericDate(time.Now()),
		ExpiresAt: jwt.NewNumericDate(expiresAt),
		NotBefore: jwt.NewNumericDate(time.Now().Add(-time.Minute)),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = kid
	signed, err := token.SignedString(privateKey)
	require.NoError(t, err)
	return signed
}

func newJWKSServer(t *testing.T, publicPath, kid string) *httptest.Server {
	t.Helper()

	jwksPayload := buildJWKS(t, publicPath, kid)
	mux := http.NewServeMux()
	var server *httptest.Server

	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		issuer := server.URL
		resp := map[string]string{
			"issuer":   issuer,
			"jwks_uri": issuer + "/jwks",
		}
		require.NoError(t, json.NewEncoder(w).Encode(resp))
	})

	mux.HandleFunc("/jwks", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(jwksPayload)
	})

	server = httptest.NewTLSServer(mux)
	return server
}

func buildJWKS(t *testing.T, publicPath, kid string) []byte {
	t.Helper()

	data, err := os.ReadFile(publicPath)
	require.NoError(t, err)

	block, _ := pem.Decode(data)
	require.NotNil(t, block)

	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	require.NoError(t, err)

	key, ok := parsed.(*rsa.PublicKey)
	require.True(t, ok)

	jwk := map[string]string{
		"kty": "RSA",
		"use": "sig",
		"alg": "RS256",
		"kid": kid,
		"n":   base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
		"e":   base64.RawURLEncoding.EncodeToString(intToBytes(key.E)),
	}

	payload := map[string][]map[string]string{
		"keys": {jwk},
	}

	encoded, err := json.Marshal(payload)
	require.NoError(t, err)
	return encoded
}

func intToBytes(value int) []byte {
	if value == 0 {
		return []byte{0}
	}
	var buf [8]byte
	i := len(buf)
	for value > 0 {
		i--
		buf[i] = byte(value & 0xff)
		value >>= 8
	}
	return buf[i:]
}
