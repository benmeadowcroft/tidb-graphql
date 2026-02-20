package main

import (
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewHTTPClient_TrustsProvidedCA(t *testing.T) {
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

	client, err := newHTTPClient(3*time.Second, caPath)
	if err != nil {
		t.Fatalf("unexpected client error: %v", err)
	}

	resp, err := client.Get(tlsServer.URL)
	if err != nil {
		t.Fatalf("expected request success with custom CA, got: %v", err)
	}
	_ = resp.Body.Close()
}

func TestNewHTTPClient_RejectsInvalidCAFile(t *testing.T) {
	caPath := filepath.Join(t.TempDir(), "invalid_ca.crt")
	if err := os.WriteFile(caPath, []byte("invalid"), 0o600); err != nil {
		t.Fatalf("failed to write CA file: %v", err)
	}

	if _, err := newHTTPClient(3*time.Second, caPath); err == nil {
		t.Fatal("expected error for invalid CA file")
	}
}
