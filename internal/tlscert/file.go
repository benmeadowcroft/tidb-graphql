package tlscert

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"os"
)

type fileManager struct {
	cfg    Config
	logger *slog.Logger
}

func newFileManager(cfg Config, logger *slog.Logger) (Manager, error) {
	// Validate required fields
	if cfg.CertFile == "" {
		return nil, fmt.Errorf("tls_cert_file is required when tls_cert_mode=file")
	}
	if cfg.KeyFile == "" {
		return nil, fmt.Errorf("tls_key_file is required when tls_cert_mode=file")
	}

	// Validate files exist and are readable
	if err := validateFile(cfg.CertFile); err != nil {
		return nil, fmt.Errorf("invalid certificate file: %w", err)
	}
	if err := validateFile(cfg.KeyFile); err != nil {
		return nil, fmt.Errorf("invalid key file: %w", err)
	}

	// Validate key file permissions (should be 0600 or 0400)
	if err := checkKeyFilePermissions(cfg.KeyFile); err != nil {
		return nil, fmt.Errorf("insecure key file permissions: %w", err)
	}

	// Test load certificate to validate it
	if _, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile); err != nil {
		return nil, fmt.Errorf("failed to load certificate: %w", err)
	}

	return &fileManager{
		cfg:    cfg,
		logger: logger,
	}, nil
}

func (m *fileManager) GetTLSConfig() (*tls.Config, error) {
	tlsConfig := &tls.Config{
		MinVersion: MinTLSVersion,
		// Let Go handle cipher suite selection for TLS 1.3
		// Go's defaults are secure and follow best practices
		CipherSuites: nil,
	}

	// Load certificate on each connection (allows cert rotation)
	tlsConfig.GetCertificate = func(clientHello *tls.ClientHelloInfo) (*tls.Certificate, error) {
		cert, err := tls.LoadX509KeyPair(m.cfg.CertFile, m.cfg.KeyFile)
		if err != nil {
			m.logger.Error("failed to reload certificate",
				slog.String("cert_file", m.cfg.CertFile),
				slog.String("error", err.Error()))
			return nil, err
		}
		return &cert, nil
	}

	return tlsConfig, nil
}

func (m *fileManager) Description() string {
	return fmt.Sprintf("file-based (cert=%s, key=%s)", m.cfg.CertFile, m.cfg.KeyFile)
}

func (m *fileManager) Shutdown() error {
	return nil
}

func validateFile(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("file not accessible: %w", err)
	}
	if info.IsDir() {
		return fmt.Errorf("path is a directory, not a file")
	}
	if info.Size() == 0 {
		return fmt.Errorf("file is empty")
	}
	return nil
}

func checkKeyFilePermissions(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}

	mode := info.Mode().Perm()
	// Check if file is readable by group or others
	if mode&0077 != 0 {
		return fmt.Errorf("key file has insecure permissions %o (should be 0600 or 0400)", mode)
	}
	return nil
}
