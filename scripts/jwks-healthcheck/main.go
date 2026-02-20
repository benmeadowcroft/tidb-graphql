package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

type oidcDiscoveryDocument struct {
	Issuer  string `json:"issuer"`
	JWKSURI string `json:"jwks_uri"`
}

func main() {
	url := flag.String("url", "https://localhost:9000/.well-known/openid-configuration", "OIDC discovery URL to probe")
	caFile := flag.String("ca-file", "", "Optional CA certificate file for TLS verification")
	timeout := flag.Duration("timeout", 3*time.Second, "HTTP request timeout")
	expectedIssuer := flag.String("expected-issuer", "", "Optional expected issuer value")
	flag.Parse()

	client, err := newHTTPClient(*timeout, *caFile)
	if err != nil {
		exitErr(err)
	}

	resp, err := client.Get(*url)
	if err != nil {
		exitErr(fmt.Errorf("healthcheck request failed: %w", err))
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		exitErr(fmt.Errorf("unexpected status code %d", resp.StatusCode))
	}

	var doc oidcDiscoveryDocument
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		exitErr(fmt.Errorf("failed to decode discovery document: %w", err))
	}

	if strings.TrimSpace(doc.Issuer) == "" {
		exitErr(fmt.Errorf("discovery document missing issuer"))
	}
	if strings.TrimSpace(doc.JWKSURI) == "" {
		exitErr(fmt.Errorf("discovery document missing jwks_uri"))
	}
	if *expectedIssuer != "" && doc.Issuer != *expectedIssuer {
		exitErr(fmt.Errorf("issuer mismatch: got %q want %q", doc.Issuer, *expectedIssuer))
	}
}

func newHTTPClient(timeout time.Duration, caFile string) (*http.Client, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	caFile = strings.TrimSpace(caFile)
	if caFile != "" {
		pemData, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA file %q: %w", caFile, err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if ok := pool.AppendCertsFromPEM(pemData); !ok {
			return nil, fmt.Errorf("failed to parse CA file %q", caFile)
		}
		tlsConfig.RootCAs = pool
	}

	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}, nil
}

func exitErr(err error) {
	fmt.Fprintln(os.Stderr, err.Error())
	os.Exit(1)
}
