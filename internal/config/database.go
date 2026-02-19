package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"github.com/go-sql-driver/mysql"
)

// tlsConfigName is the name used to register custom TLS configs with the MySQL driver.
const tlsConfigName = "tidb-graphql-custom"

// DSN returns a MySQL-compatible data source name.
// If ConnectionString is set, it is used directly (with TLS config applied).
// Otherwise, builds DSN from discrete fields.
func (d *DatabaseConfig) DSN() string {
	var dsn string

	if d.ConnectionString != "" {
		dsn = d.ConnectionString
		// Ensure parseTime is set
		if !strings.Contains(dsn, "parseTime") {
			if strings.Contains(dsn, "?") {
				dsn += "&parseTime=true"
			} else {
				dsn += "?parseTime=true"
			}
		}
		if !strings.Contains(dsn, "loc=") {
			dsn += "&loc=UTC"
		}
	} else {
		dsn = fmt.Sprintf(
			"%s:%s@tcp(%s:%d)/%s?parseTime=true&loc=UTC",
			d.User,
			d.Password,
			d.Host,
			d.Port,
			d.Database,
		)
	}

	// Add TLS parameter
	tlsParam := d.effectiveTLSParam()
	if tlsParam != "" && !strings.Contains(dsn, "tls=") {
		dsn += fmt.Sprintf("&tls=%s", tlsParam)
	}

	return dsn
}

// DSNWithoutDatabase returns a DSN that omits the default database.
// Useful for role-based auth where database access is granted via SET ROLE.
func (d *DatabaseConfig) DSNWithoutDatabase() string {
	var dsn string

	if d.ConnectionString != "" {
		// Parse the connection string and remove the database part
		// This is a simplification; the ConnectionString may not have a database
		dsn = d.ConnectionString
		// Ensure parseTime is set
		if !strings.Contains(dsn, "parseTime") {
			if strings.Contains(dsn, "?") {
				dsn += "&parseTime=true"
			} else {
				dsn += "?parseTime=true"
			}
		}
		if !strings.Contains(dsn, "loc=") {
			dsn += "&loc=UTC"
		}
	} else {
		dsn = fmt.Sprintf(
			"%s:%s@tcp(%s:%d)/?parseTime=true&loc=UTC",
			d.User,
			d.Password,
			d.Host,
			d.Port,
		)
	}

	// Add TLS parameter
	tlsParam := d.effectiveTLSParam()
	if tlsParam != "" && !strings.Contains(dsn, "tls=") {
		dsn += fmt.Sprintf("&tls=%s", tlsParam)
	}

	return dsn
}

// EffectiveDatabaseName returns the canonical database name used for schema introspection
// and role-aware query execution.
func (d *DatabaseConfig) EffectiveDatabaseName() (name string, source string, err error) {
	return resolveEffectiveDatabaseName(d.Database, d.ConnectionString, d.MyCnfFile)
}

func resolveEffectiveDatabaseName(databaseName string, connectionString string, myCnfFile string) (name string, source string, err error) {
	configDatabase := strings.TrimSpace(databaseName)
	dsn := strings.TrimSpace(connectionString)
	myCnfPath := strings.TrimSpace(myCnfFile)
	dsnDatabase, parseErr := parseDSNDatabaseName(dsn)
	if parseErr != nil {
		return "", "", parseErr
	}

	if configDatabase != "" {
		if dsnDatabase != "" && configDatabase != dsnDatabase {
			return "", "", fmt.Errorf(
				"database mismatch: database.database=%q but database.dsn targets %q",
				configDatabase,
				dsnDatabase,
			)
		}
		if myCnfPath != "" && dsn == "" {
			return configDatabase, "mycnf", nil
		}
		return configDatabase, "database.database", nil
	}

	if dsnDatabase != "" {
		return dsnDatabase, "dsn", nil
	}

	if myCnfPath != "" {
		return "", "", fmt.Errorf(
			"database.mycnf_file does not provide a database name and database.database is not set",
		)
	}

	return "", "", fmt.Errorf(
		"no effective database name configured: set database.database or include /<database> in database.dsn/database.dsn_file or database.mycnf_file",
	)
}

func parseDSNDatabaseName(connectionString string) (string, error) {
	dsn := strings.TrimSpace(connectionString)
	if dsn == "" {
		return "", nil
	}

	parsed, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", fmt.Errorf("database.dsn is invalid: %w", err)
	}
	return strings.TrimSpace(parsed.DBName), nil
}

// effectiveTLSParam returns the TLS parameter value for the DSN.
// Returns the registered config name for custom TLS, or empty string if no TLS is configured.
func (d *DatabaseConfig) effectiveTLSParam() string {
	// Check if we have new TLS configuration
	mode := d.TLS.Mode

	// If new TLS mode is set, use it
	if mode != "" {
		switch mode {
		case "off":
			return "false"
		case "skip-verify":
			return "skip-verify"
		case "verify-ca", "verify-full":
			// Custom TLS config required
			return tlsConfigName
		default:
			// Unknown mode, let the driver handle it
			return mode
		}
	}

	// Empty mode means no TLS parameter should be added.
	return ""
}

// RegisterTLS registers a custom TLS configuration with the MySQL driver.
// Must be called before opening the database connection when using verify-ca or verify-full modes.
// Returns nil if no custom TLS configuration is needed.
func (d *DatabaseConfig) RegisterTLS() error {
	mode := d.TLS.Mode

	// Only register custom config for modes that need it
	if mode != "verify-ca" && mode != "verify-full" {
		return nil
	}

	tlsCfg, err := d.buildTLSConfig()
	if err != nil {
		return fmt.Errorf("failed to build TLS config: %w", err)
	}

	if err := mysql.RegisterTLSConfig(tlsConfigName, tlsCfg); err != nil {
		return fmt.Errorf("failed to register TLS config: %w", err)
	}

	return nil
}

// buildTLSConfig creates a tls.Config based on the DatabaseTLSConfig settings.
func (d *DatabaseConfig) buildTLSConfig() (*tls.Config, error) {
	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	mode := d.TLS.Mode

	// Resolve file paths from env var indirection if specified
	caFile := d.TLS.resolveCAFile()
	certFile := d.TLS.resolveCertFile()
	keyFile := d.TLS.resolveKeyFile()

	// Load CA certificate for server verification
	if caFile != "" {
		caCert, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA file %q: %w", caFile, err)
		}

		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate from %q", caFile)
		}
		tlsCfg.RootCAs = certPool
	}

	// Load client certificate for mTLS
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	} else if certFile != "" || keyFile != "" {
		return nil, fmt.Errorf("both cert_file and key_file must be specified for client certificate authentication")
	}

	// Configure verification mode
	switch mode {
	case "verify-ca":
		// Verify the server certificate against CA, but don't check hostname
		tlsCfg.InsecureSkipVerify = false
		// Note: go-sql-driver/mysql handles the CA verification
	case "verify-full":
		tlsCfg.InsecureSkipVerify = false
		if d.TLS.ServerName != "" {
			tlsCfg.ServerName = d.TLS.ServerName
		}
	}

	return tlsCfg, nil
}

// resolveCAFile returns the effective CA file path, checking env var indirection.
func (t *DatabaseTLSConfig) resolveCAFile() string {
	if t.CAFileEnv != "" {
		if path := os.Getenv(t.CAFileEnv); path != "" {
			return path
		}
	}
	return t.CAFile
}

// resolveCertFile returns the effective client cert file path, checking env var indirection.
func (t *DatabaseTLSConfig) resolveCertFile() string {
	if t.CertFileEnv != "" {
		if path := os.Getenv(t.CertFileEnv); path != "" {
			return path
		}
	}
	return t.CertFile
}

// resolveKeyFile returns the effective client key file path, checking env var indirection.
func (t *DatabaseTLSConfig) resolveKeyFile() string {
	if t.KeyFileEnv != "" {
		if path := os.Getenv(t.KeyFileEnv); path != "" {
			return path
		}
	}
	return t.KeyFile
}
