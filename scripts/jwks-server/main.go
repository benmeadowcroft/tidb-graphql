package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

type jwk struct {
	Kty string `json:"kty"`
	Use string `json:"use"`
	Alg string `json:"alg"`
	Kid string `json:"kid"`
	N   string `json:"n"`
	E   string `json:"e"`
}

type jwks struct {
	Keys []jwk `json:"keys"`
}

func main() {
	addr := flag.String("addr", ":9000", "Listen address")
	issuer := flag.String("issuer", "https://localhost:9000", "OIDC issuer URL")
	publicKeyPath := flag.String("public-key", ".auth/jwt_public.pem", "Path to RSA public key (PEM)")
	kid := flag.String("kid", "local-key", "Key ID to advertise")
	enableTLS := flag.Bool("tls", true, "Enable HTTPS with a self-signed certificate")
	tlsCertPath := flag.String("tls-cert", ".auth/jwks_tls.crt", "Path to TLS certificate (PEM)")
	tlsKeyPath := flag.String("tls-key", ".auth/jwks_tls.key", "Path to TLS private key (PEM)")
	flag.Parse()

	key, err := loadPublicKey(*publicKeyPath)
	if err != nil {
		exitErr(err)
	}

	jwksPayload, err := buildJWKS(key, *kid)
	if err != nil {
		exitErr(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"issuer":"%s","jwks_uri":"%s/jwks"}`, *issuer, *issuer)
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(jwksPayload)
	})

	if *enableTLS {
		if err := ensureTLSFiles(*tlsCertPath, *tlsKeyPath); err != nil {
			exitErr(err)
		}
		fmt.Printf("JWKS server listening on https://%s\n", *addr)
		exitErr(http.ListenAndServeTLS(*addr, *tlsCertPath, *tlsKeyPath, mux))
		return
	}

	fmt.Fprintln(os.Stderr, "warning: JWKS server running without TLS (dev only)")
	fmt.Printf("JWKS server listening on http://%s\n", *addr)
	exitErr(http.ListenAndServe(*addr, mux))
}

func loadPublicKey(path string) (*rsa.PublicKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read public key: %w", err)
	}

	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("failed to decode public key PEM")
	}

	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse public key: %w", err)
	}

	key, ok := parsed.(*rsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("public key is not RSA")
	}

	return key, nil
}

func buildJWKS(key *rsa.PublicKey, kid string) ([]byte, error) {
	n := base64.RawURLEncoding.EncodeToString(key.N.Bytes())
	e := base64.RawURLEncoding.EncodeToString(intToBytes(key.E))

	payload := jwks{
		Keys: []jwk{{
			Kty: "RSA",
			Use: "sig",
			Alg: "RS256",
			Kid: kid,
			N:   n,
			E:   e,
		}},
	}

	return json.Marshal(payload)
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

func ensureTLSFiles(certPath, keyPath string) error {
	if fileExists(certPath) && fileExists(keyPath) {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(certPath), 0o700); err != nil {
		return fmt.Errorf("failed to create tls cert directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(keyPath), 0o700); err != nil {
		return fmt.Errorf("failed to create tls key directory: %w", err)
	}

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return fmt.Errorf("failed to generate tls key: %w", err)
	}

	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		return fmt.Errorf("failed to generate tls serial: %w", err)
	}

	template := x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: "localhost",
		},
		NotBefore: time.Now().Add(-1 * time.Minute),
		NotAfter:  time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:  x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
		},
		DNSNames: []string{"localhost"},
		IPAddresses: []net.IP{
			net.ParseIP("127.0.0.1"),
			net.ParseIP("::1"),
		},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return fmt.Errorf("failed to create tls certificate: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})

	if err := os.WriteFile(certPath, certPEM, 0o644); err != nil {
		return fmt.Errorf("failed to write tls cert: %w", err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		return fmt.Errorf("failed to write tls key: %w", err)
	}

	return nil
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return !info.IsDir()
}

func exitErr(err error) {
	if err == nil {
		return
	}
	fmt.Fprintln(os.Stderr, err.Error())
	os.Exit(1)
}
