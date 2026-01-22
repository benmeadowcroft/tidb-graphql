// Package tlscert manages TLS certificates for the HTTPS server, including self-signed certs.
package tlscert

import (
	"crypto/tls"
	"fmt"
	"log/slog"
)

// CertMode represents the certificate management mode
type CertMode string

const (
	CertModeFile       CertMode = "file"
	CertModeSelfSigned CertMode = "selfsigned"
)

// Config holds TLS certificate configuration
type Config struct {
	Mode CertMode

	// File mode
	CertFile string
	KeyFile  string

	// Self-signed mode
	SelfSignedCertDir string
	SelfSignedHosts   []string // "localhost", "127.0.0.1", etc.
}

// Manager provides TLS certificate management
type Manager interface {
	// GetTLSConfig returns a tls.Config ready for use with http.Server
	GetTLSConfig() (*tls.Config, error)

	// Description returns a human-readable description of the cert source
	Description() string

	// Shutdown performs cleanup (if needed)
	Shutdown() error
}

// NewManager creates a certificate manager based on configuration
func NewManager(cfg Config, logger *slog.Logger) (Manager, error) {
	switch cfg.Mode {
	case CertModeFile:
		return newFileManager(cfg, logger)
	case CertModeSelfSigned:
		return newSelfSignedManager(cfg, logger)
	default:
		return nil, fmt.Errorf("unsupported TLS certificate mode: %s (valid modes: file, selfsigned)", cfg.Mode)
	}
}

// MinTLSVersion is the minimum supported TLS version for the server.
const MinTLSVersion = tls.VersionTLS13
