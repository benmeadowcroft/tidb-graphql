package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
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

type serverConfig struct {
	Issuer   string
	Audience []string
	KID      string
	JWKSPem  []byte
	DevToken devTokenConfig
}

type devTokenConfig struct {
	Enabled        bool
	AdminToken     string
	PrivateKeyPath string
	PrivateKey     *rsa.PrivateKey
	DefaultTTL     time.Duration
	MaxTTL         time.Duration
}

type devTokenRequest struct {
	Subject   string   `json:"subject"`
	DBRole    string   `json:"db_role"`
	Roles     []string `json:"roles"`
	ExpiresIn string   `json:"expires_in"`
}

type devTokenResponse struct {
	Token            string `json:"token"`
	TokenType        string `json:"token_type"`
	ExpiresInSeconds int64  `json:"expires_in_seconds"`
	ExpiresAt        string `json:"expires_at"`
}

type jsonError struct {
	Error string `json:"error"`
}

const (
	adminTokenHeader      = "X-Admin-Token"
	defaultDevTokenSub    = "dev-user"
	maxRequestBodyBytes   = 1 << 20
	defaultTokenAudience  = "tidb-graphql"
	defaultDevTokenMaxTTL = 7 * 24 * time.Hour
)

func main() {
	addr := flag.String("addr", ":9000", "Listen address")
	issuer := flag.String("issuer", "https://localhost:9000", "OIDC issuer URL")
	audience := flag.String("audience", defaultTokenAudience, "Expected JWT audience value(s), comma-separated")
	publicKeyPath := flag.String("public-key", ".auth/jwt_public.pem", "Path to RSA public key (PEM)")
	kid := flag.String("kid", "local-key", "Key ID to advertise")
	enableTLS := flag.Bool("tls", true, "Enable HTTPS with a self-signed certificate")
	tlsCertPath := flag.String("tls-cert", ".auth/jwks_tls.crt", "Path to TLS certificate (PEM)")
	tlsKeyPath := flag.String("tls-key", ".auth/jwks_tls.key", "Path to TLS private key (PEM)")
	devTokenEnabled := flag.Bool("dev-token-enabled", false, "Enable dev-only token vending endpoint (/dev/token)")
	devTokenAdminToken := flag.String("dev-token-admin-token", "", "Shared admin token required by /dev/token")
	devTokenPrivateKey := flag.String("dev-token-private-key", ".auth/jwt_private.pem", "Path to RSA private key used by /dev/token")
	devTokenDefaultTTL := flag.Duration("dev-token-default-ttl", 24*time.Hour, "Default token lifetime for /dev/token")
	devTokenMaxTTL := flag.Duration("dev-token-max-ttl", defaultDevTokenMaxTTL, "Maximum allowed token lifetime for /dev/token")
	flag.Parse()

	key, err := loadPublicKey(*publicKeyPath)
	if err != nil {
		exitErr(err)
	}

	jwksPayload, err := buildJWKS(key, *kid)
	if err != nil {
		exitErr(err)
	}

	devCfg := devTokenConfig{
		Enabled:        *devTokenEnabled,
		AdminToken:     strings.TrimSpace(*devTokenAdminToken),
		PrivateKeyPath: strings.TrimSpace(*devTokenPrivateKey),
		DefaultTTL:     *devTokenDefaultTTL,
		MaxTTL:         *devTokenMaxTTL,
	}
	if err := validateAndLoadDevTokenConfig(&devCfg); err != nil {
		exitErr(err)
	}

	audienceValues := splitList(*audience)
	if len(audienceValues) == 0 {
		exitErr(errors.New("audience is required"))
	}

	mux, err := buildServerMux(serverConfig{
		Issuer:   *issuer,
		Audience: audienceValues,
		KID:      *kid,
		JWKSPem:  jwksPayload,
		DevToken: devCfg,
	})
	if err != nil {
		exitErr(err)
	}

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

func buildServerMux(cfg serverConfig) (*http.ServeMux, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{
			"issuer":   cfg.Issuer,
			"jwks_uri": cfg.Issuer + "/jwks",
		})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(cfg.JWKSPem)
	})
	if cfg.DevToken.Enabled {
		handler, err := newDevTokenHandler(cfg)
		if err != nil {
			return nil, err
		}
		mux.Handle("/dev/token", handler)
	}
	return mux, nil
}

func validateAndLoadDevTokenConfig(cfg *devTokenConfig) error {
	if cfg == nil || !cfg.Enabled {
		return nil
	}
	if strings.TrimSpace(cfg.AdminToken) == "" {
		return errors.New("dev-token-enabled requires --dev-token-admin-token")
	}
	if cfg.DefaultTTL <= 0 {
		return errors.New("dev-token-default-ttl must be greater than 0")
	}
	if cfg.MaxTTL <= 0 {
		return errors.New("dev-token-max-ttl must be greater than 0")
	}
	if cfg.DefaultTTL > cfg.MaxTTL {
		return errors.New("dev-token-default-ttl cannot exceed dev-token-max-ttl")
	}
	privateKey, err := loadPrivateKey(cfg.PrivateKeyPath)
	if err != nil {
		return fmt.Errorf("failed to load dev token private key: %w", err)
	}
	cfg.PrivateKey = privateKey
	return nil
}

func newDevTokenHandler(cfg serverConfig) (http.Handler, error) {
	if !cfg.DevToken.Enabled {
		return nil, nil
	}
	if cfg.DevToken.PrivateKey == nil {
		return nil, errors.New("dev token private key is required")
	}
	adminToken := strings.TrimSpace(cfg.DevToken.AdminToken)
	if adminToken == "" {
		return nil, errors.New("dev token admin token is required")
	}
	if len(cfg.Audience) == 0 {
		return nil, errors.New("audience is required for dev token endpoint")
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, jsonError{Error: "method not allowed"})
			return
		}
		provided := strings.TrimSpace(r.Header.Get(adminTokenHeader))
		if !constantTimeTokenMatch(provided, adminToken) {
			writeJSON(w, http.StatusUnauthorized, jsonError{Error: "unauthorized"})
			return
		}

		req, err := decodeDevTokenRequest(r)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, jsonError{Error: "invalid request body"})
			return
		}
		now := time.Now()
		ttl, err := resolveTokenTTL(cfg.DevToken, req.ExpiresIn)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, jsonError{Error: err.Error()})
			return
		}

		subject := strings.TrimSpace(req.Subject)
		if subject == "" {
			subject = defaultDevTokenSub
		}
		expiresAt := now.Add(ttl)
		claims := jwt.MapClaims{
			"iss": cfg.Issuer,
			"sub": subject,
			"aud": cfg.Audience,
			"iat": now.Unix(),
			"exp": expiresAt.Unix(),
			"nbf": now.Add(-1 * time.Minute).Unix(),
		}
		if role := strings.TrimSpace(req.DBRole); role != "" {
			claims["db_role"] = role
		}
		if roles := normalizeRoles(req.Roles); len(roles) > 0 {
			claims["roles"] = roles
		}

		token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
		token.Header["kid"] = cfg.KID
		signed, err := token.SignedString(cfg.DevToken.PrivateKey)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, jsonError{Error: "failed to sign token"})
			return
		}

		if strings.Contains(strings.ToLower(r.Header.Get("Accept")), "text/plain") {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, signed)
			return
		}

		writeJSON(w, http.StatusOK, devTokenResponse{
			Token:            signed,
			TokenType:        "Bearer",
			ExpiresInSeconds: int64(ttl.Seconds()),
			ExpiresAt:        expiresAt.UTC().Format(time.RFC3339),
		})
	}), nil
}

func decodeDevTokenRequest(r *http.Request) (devTokenRequest, error) {
	if r == nil || r.Body == nil {
		return devTokenRequest{}, nil
	}
	defer func() {
		_ = r.Body.Close()
	}()

	var req devTokenRequest
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxRequestBodyBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		if errors.Is(err, io.EOF) {
			return devTokenRequest{}, nil
		}
		return devTokenRequest{}, err
	}
	var extra struct{}
	if err := decoder.Decode(&extra); err != io.EOF {
		return devTokenRequest{}, errors.New("request body must contain a single JSON object")
	}
	return req, nil
}

func resolveTokenTTL(cfg devTokenConfig, requested string) (time.Duration, error) {
	ttl := cfg.DefaultTTL
	if raw := strings.TrimSpace(requested); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			return 0, errors.New("expires_in must be a valid duration")
		}
		ttl = parsed
	}
	if ttl <= 0 {
		return 0, errors.New("expires_in must be greater than 0")
	}
	if ttl > cfg.MaxTTL {
		return 0, fmt.Errorf("expires_in exceeds maximum of %s", cfg.MaxTTL)
	}
	return ttl, nil
}

func normalizeRoles(raw []string) []string {
	roles := make([]string, 0, len(raw))
	for _, role := range raw {
		if trimmed := strings.TrimSpace(role); trimmed != "" {
			roles = append(roles, trimmed)
		}
	}
	return roles
}

func writeJSON(w http.ResponseWriter, statusCode int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}

func constantTimeTokenMatch(provided string, expected string) bool {
	providedDigest := sha256.Sum256([]byte(provided))
	expectedDigest := sha256.Sum256([]byte(expected))
	return subtle.ConstantTimeCompare(providedDigest[:], expectedDigest[:]) == 1
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

func loadPrivateKey(path string) (*rsa.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read private key: %w", err)
	}

	block, _ := pem.Decode(data)
	if block == nil {
		return nil, errors.New("failed to decode private key PEM")
	}

	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %w", err)
	}

	rsaKey, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("private key is not RSA")
	}
	return rsaKey, nil
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

func splitList(value string) []string {
	raw := strings.Split(value, ",")
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		if trimmed := strings.TrimSpace(item); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
