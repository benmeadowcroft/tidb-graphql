package tlscert

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

type selfSignedManager struct {
	cfg      Config
	logger   *slog.Logger
	certPath string
	keyPath  string
}

func newSelfSignedManager(cfg Config, logger *slog.Logger) (Manager, error) {
	// Default hosts for self-signed cert
	if len(cfg.SelfSignedHosts) == 0 {
		cfg.SelfSignedHosts = []string{"localhost", "127.0.0.1", "::1"}
	}

	// Ensure certificate directory exists
	if err := os.MkdirAll(cfg.SelfSignedCertDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create certificate directory: %w", err)
	}

	certPath := filepath.Join(cfg.SelfSignedCertDir, "server.crt")
	keyPath := filepath.Join(cfg.SelfSignedCertDir, "server.key")

	// Ensure a valid self-signed certificate exists.
	needsCert := !fileExists(certPath) || !fileExists(keyPath)
	if !needsCert {
		valid, err := validateSelfSignedCert(certPath, keyPath, cfg.SelfSignedHosts)
		if err != nil {
			return nil, err
		}
		needsCert = !valid
	}

	if needsCert {
		logger.Info("generating self-signed certificate",
			slog.String("cert_path", certPath),
			slog.String("key_path", keyPath),
			slog.Any("hosts", cfg.SelfSignedHosts))

		if err := generateSelfSignedCert(certPath, keyPath, cfg.SelfSignedHosts); err != nil {
			return nil, fmt.Errorf("failed to generate self-signed certificate: %w", err)
		}

		logger.Warn("self-signed certificate generated - not suitable for production",
			slog.String("cert_path", certPath))
	} else {
		logger.Info("using existing self-signed certificate",
			slog.String("cert_path", certPath))
	}

	return &selfSignedManager{
		cfg:      cfg,
		logger:   logger,
		certPath: certPath,
		keyPath:  keyPath,
	}, nil
}

func (m *selfSignedManager) GetTLSConfig() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(m.certPath, m.keyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load self-signed certificate: %w", err)
	}

	return &tls.Config{
		MinVersion:   MinTLSVersion,
		Certificates: []tls.Certificate{cert},
	}, nil
}

func (m *selfSignedManager) Description() string {
	return fmt.Sprintf("self-signed (cert=%s) - DEV ONLY", m.certPath)
}

func (m *selfSignedManager) Shutdown() error {
	return nil
}

func generateSelfSignedCert(certPath, keyPath string, hosts []string) error {
	// Generate RSA private key (2048-bit)
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return fmt.Errorf("failed to generate RSA key: %w", err)
	}

	// Generate serial number
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return fmt.Errorf("failed to generate serial number: %w", err)
	}

	// Create certificate template
	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"TiDB GraphQL (Self-Signed)"},
			CommonName:   "localhost",
		},
		NotBefore:             time.Now().Add(-5 * time.Minute),     // 5min grace period
		NotAfter:              time.Now().Add(365 * 24 * time.Hour), // 1 year
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	// Add DNS names and IP addresses from hosts
	for _, host := range hosts {
		if ip := net.ParseIP(host); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, host)
		}
	}

	// Create self-signed certificate
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return fmt.Errorf("failed to create certificate: %w", err)
	}

	// Write certificate file
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	if err := os.WriteFile(certPath, certPEM, 0644); err != nil {
		return fmt.Errorf("failed to write certificate: %w", err)
	}

	// Write private key file with restrictive permissions
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	if err := os.WriteFile(keyPath, keyPEM, 0600); err != nil {
		return fmt.Errorf("failed to write private key: %w", err)
	}

	return nil
}

func validateSelfSignedCert(certPath, keyPath string, hosts []string) (bool, error) {
	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		return false, fmt.Errorf("failed to read self-signed certificate: %w", err)
	}

	block, _ := pem.Decode(certPEM)
	if block == nil || block.Type != "CERTIFICATE" {
		return false, fmt.Errorf("invalid certificate PEM in %s", certPath)
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return false, fmt.Errorf("failed to parse self-signed certificate: %w", err)
	}

	now := time.Now()
	if now.Before(cert.NotBefore) || now.After(cert.NotAfter) {
		return false, nil
	}

	if !hostsMatchCertificate(cert, hosts) {
		return false, nil
	}

	if _, err := tls.LoadX509KeyPair(certPath, keyPath); err != nil {
		return false, nil
	}

	return true, nil
}

func hostsMatchCertificate(cert *x509.Certificate, hosts []string) bool {
	expectedDNS := make(map[string]struct{})
	expectedIPs := make(map[string]struct{})

	for _, host := range hosts {
		if ip := net.ParseIP(host); ip != nil {
			expectedIPs[ip.String()] = struct{}{}
			continue
		}
		expectedDNS[host] = struct{}{}
	}

	actualDNS := make(map[string]struct{}, len(cert.DNSNames))
	for _, name := range cert.DNSNames {
		actualDNS[name] = struct{}{}
	}
	actualIPs := make(map[string]struct{}, len(cert.IPAddresses))
	for _, ip := range cert.IPAddresses {
		actualIPs[ip.String()] = struct{}{}
	}

	if len(expectedDNS) != len(actualDNS) || len(expectedIPs) != len(actualIPs) {
		return false
	}

	for name := range expectedDNS {
		if _, ok := actualDNS[name]; !ok {
			return false
		}
	}
	for ip := range expectedIPs {
		if _, ok := actualIPs[ip]; !ok {
			return false
		}
	}

	return true
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}
