package middleware

import (
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestNewOIDCHTTPClient_TrustsProvidedCA(t *testing.T) {
	tlsServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer tlsServer.Close()

	caPath := filepath.Join(t.TempDir(), "root_ca.crt")
	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: tlsServer.Certificate().Raw,
	})
	if err := os.WriteFile(caPath, certPEM, 0o600); err != nil {
		t.Fatalf("failed to write CA file: %v", err)
	}

	client, err := newOIDCHTTPClient(OIDCAuthConfig{CAFile: caPath})
	if err != nil {
		t.Fatalf("unexpected client build error: %v", err)
	}

	resp, err := client.Get(tlsServer.URL)
	if err != nil {
		t.Fatalf("expected request to succeed with custom CA, got error: %v", err)
	}
	_ = resp.Body.Close()
}

func TestNewOIDCHTTPClient_FailsWithoutCAForSelfSignedServer(t *testing.T) {
	tlsServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer tlsServer.Close()

	client, err := newOIDCHTTPClient(OIDCAuthConfig{})
	if err != nil {
		t.Fatalf("unexpected client build error: %v", err)
	}

	if _, err := client.Get(tlsServer.URL); err == nil {
		t.Fatal("expected TLS verification error without CA file")
	}
}

func TestNewOIDCHTTPClient_RejectsInvalidCAFile(t *testing.T) {
	caPath := filepath.Join(t.TempDir(), "invalid_ca.crt")
	if err := os.WriteFile(caPath, []byte("not a certificate"), 0o600); err != nil {
		t.Fatalf("failed to write CA file: %v", err)
	}

	if _, err := newOIDCHTTPClient(OIDCAuthConfig{CAFile: caPath}); err == nil {
		t.Fatal("expected error for invalid CA file")
	}
}
