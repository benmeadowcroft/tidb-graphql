// Package config loads configuration from files, env vars, and flags, and validates it.
// See docs/reference/configuration.md and docs/how-to/config-precedence.md.
package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"golang.org/x/term"

	"tidb-graphql/internal/naming"
	"tidb-graphql/internal/schemafilter"
)

// Config holds the application configuration.
type Config struct {
	Database      DatabaseConfig      `mapstructure:"database"`
	Server        ServerConfig        `mapstructure:"server"`
	Observability ObservabilityConfig `mapstructure:"observability"`
	SchemaFilters schemafilter.Config `mapstructure:"schema_filters"`
	Naming        naming.Config       `mapstructure:"naming"`
}

// PoolConfig holds connection pool parameters.
type PoolConfig struct {
	MaxOpen     int           `mapstructure:"max_open"`
	MaxIdle     int           `mapstructure:"max_idle"`
	MaxLifetime time.Duration `mapstructure:"max_lifetime"`
}

// DatabaseTLSConfig holds TLS/SSL configuration for database connections.
// Supports both server verification and client certificate authentication (mTLS).
type DatabaseTLSConfig struct {
	// Mode controls TLS behavior:
	//   - "off": No TLS (plaintext connection)
	//   - "skip-verify": TLS without server certificate verification (insecure)
	//   - "verify-ca": TLS with CA verification but no hostname check
	//   - "verify-full": TLS with full verification including hostname
	Mode string `mapstructure:"mode"`

	// CAFile is the path to the CA certificate for server verification.
	// Required for verify-ca and verify-full modes.
	CAFile string `mapstructure:"ca_file"`
	// CAFileEnv is an environment variable name containing the CA file path.
	// Useful for Kubernetes ConfigMap/Secret separation.
	CAFileEnv string `mapstructure:"ca_file_env"`

	// CertFile is the path to the client certificate for mTLS authentication.
	CertFile string `mapstructure:"cert_file"`
	// CertFileEnv is an environment variable name containing the client cert path.
	CertFileEnv string `mapstructure:"cert_file_env"`

	// KeyFile is the path to the client private key for mTLS authentication.
	KeyFile string `mapstructure:"key_file"`
	// KeyFileEnv is an environment variable name containing the client key path.
	KeyFileEnv string `mapstructure:"key_file_env"`

	// ServerName overrides the server name used for TLS verification.
	// If empty, the database host is used.
	ServerName string `mapstructure:"server_name"`
}

// DatabaseConfig holds database connection parameters.
type DatabaseConfig struct {
	// ConnectionString is a complete go-sql-driver/mysql Data Source Name.
	// Format: user:password@tcp(host:port)/database?params
	// When set, overrides Host/Port/User/Password/Database fields.
	// Configured via "dsn" in YAML or TIGQL_DATABASE_DSN env var.
	ConnectionString string `mapstructure:"dsn"`
	// ConnectionStringFile is a path to a file containing the DSN (for secrets management).
	// Supports "@-" to read from stdin.
	// Configured via "dsn_file" in YAML or TIGQL_DATABASE_DSN_FILE env var.
	ConnectionStringFile string `mapstructure:"dsn_file"`

	// Discrete connection fields (used when DSN is not set)
	Host                    string `mapstructure:"host"`
	Port                    int    `mapstructure:"port"`
	User                    string `mapstructure:"user"`
	Password                string `mapstructure:"password"`
	PasswordFile            string `mapstructure:"password_file"`
	PasswordPrompt          bool   `mapstructure:"password_prompt"`
	Database                string `mapstructure:"database"`

	// TLS holds the TLS/SSL configuration for database connections.
	TLS DatabaseTLSConfig `mapstructure:"tls"`

	// Connection pool settings
	Pool PoolConfig `mapstructure:"pool"`

	// ConnectionTimeout is the max time to wait for DB on startup.
	ConnectionTimeout time.Duration `mapstructure:"connection_timeout"`
	// ConnectionRetryInterval is the initial interval between connection retries.
	ConnectionRetryInterval time.Duration `mapstructure:"connection_retry_interval"`
}

// AuthConfig holds authentication and authorization parameters.
type AuthConfig struct {
	OIDCEnabled              bool          `mapstructure:"oidc_enabled"`
	OIDCIssuerURL            string        `mapstructure:"oidc_issuer_url"`
	OIDCAudience             string        `mapstructure:"oidc_audience"`
	OIDCClockSkew            time.Duration `mapstructure:"oidc_clock_skew"`
	OIDCSkipTLSVerify        bool          `mapstructure:"oidc_skip_tls_verify"`
	DBRoleEnabled            bool          `mapstructure:"db_role_enabled"`
	DBRoleClaimName          string        `mapstructure:"db_role_claim_name"`
	DBRoleValidationEnabled  bool          `mapstructure:"db_role_validation_enabled"`
	DBRoleIntrospectionRole  string        `mapstructure:"db_role_introspection_role"`
}

// ServerConfig holds HTTP server parameters.
type ServerConfig struct {
	Port                     int           `mapstructure:"port"`
	GraphQLMaxDepth          int           `mapstructure:"graphql_max_depth"`
	GraphQLMaxComplexity     int           `mapstructure:"graphql_max_complexity"`
	GraphQLMaxRows           int           `mapstructure:"graphql_max_rows"`
	GraphQLDefaultLimit      int           `mapstructure:"graphql_default_limit"`
	SchemaRefreshMinInterval time.Duration `mapstructure:"schema_refresh_min_interval"`
	SchemaRefreshMaxInterval time.Duration `mapstructure:"schema_refresh_max_interval"`
	GraphiQLEnabled          bool          `mapstructure:"graphiql_enabled"`
	Auth                     AuthConfig    `mapstructure:"auth"`
	RateLimitEnabled         bool          `mapstructure:"rate_limit_enabled"`
	RateLimitRPS             float64       `mapstructure:"rate_limit_rps"`
	RateLimitBurst           int           `mapstructure:"rate_limit_burst"`
	CORSEnabled              bool          `mapstructure:"cors_enabled"`
	CORSAllowedOrigins       []string      `mapstructure:"cors_allowed_origins"`
	CORSAllowedMethods       []string      `mapstructure:"cors_allowed_methods"`
	CORSAllowedHeaders       []string      `mapstructure:"cors_allowed_headers"`
	CORSExposeHeaders        []string      `mapstructure:"cors_expose_headers"`
	CORSAllowCredentials     bool          `mapstructure:"cors_allow_credentials"`
	CORSMaxAge               int           `mapstructure:"cors_max_age"`
	ReadTimeout              time.Duration `mapstructure:"read_timeout"`
	WriteTimeout             time.Duration `mapstructure:"write_timeout"`
	IdleTimeout              time.Duration `mapstructure:"idle_timeout"`
	ShutdownTimeout          time.Duration `mapstructure:"shutdown_timeout"`
	HealthCheckTimeout       time.Duration `mapstructure:"health_check_timeout"`

	// TLS Configuration
	TLSMode       string `mapstructure:"tls_mode"`         // "off", "auto", or "file" (default: "off")
	TLSCertFile   string `mapstructure:"tls_cert_file"`    // Path to certificate file (for "file" mode)
	TLSKeyFile    string `mapstructure:"tls_key_file"`     // Path to private key file (for "file" mode)
	TLSAutoCertDir string `mapstructure:"tls_auto_cert_dir"` // Directory for auto-generated certs (default: ".tls")
}

// LoggingConfig holds logging parameters.
type LoggingConfig struct {
	Level          string `mapstructure:"level"`           // debug, info, warn, error
	Format         string `mapstructure:"format"`          // json, text
	ExportsEnabled bool   `mapstructure:"exports_enabled"` // Enable OTLP log export
}

// ObservabilityConfig holds observability parameters.
type ObservabilityConfig struct {
	ServiceName         string        `mapstructure:"service_name"`
	ServiceVersion      string        `mapstructure:"service_version"`
	Environment         string        `mapstructure:"environment"`
	MetricsEnabled      bool          `mapstructure:"metrics_enabled"`
	TracingEnabled      bool          `mapstructure:"tracing_enabled"`
	SQLCommenterEnabled bool          `mapstructure:"sqlcommenter_enabled"` // Inject trace context into SQL queries
	Logging             LoggingConfig `mapstructure:"logging"`

	// Global OTLP settings (defaults for all signals)
	OTLP OTLPConfig `mapstructure:"otlp"`

	// Signal-specific overrides (optional)
	Traces  *OTLPConfig `mapstructure:"traces,omitempty"`
	Logs    *OTLPConfig `mapstructure:"logs,omitempty"`
	Metrics *OTLPConfig `mapstructure:"metrics,omitempty"`
}

// OTLPConfig holds OTLP exporter configuration
type OTLPConfig struct {
	Endpoint          string            `mapstructure:"endpoint"`
	Protocol          string            `mapstructure:"protocol"` // "grpc", "http/protobuf"
	Insecure          bool              `mapstructure:"insecure"`
	TLSCertFile       string            `mapstructure:"tls_cert_file"`
	TLSClientCertFile string            `mapstructure:"tls_client_cert_file"`
	TLSClientKeyFile  string            `mapstructure:"tls_client_key_file"`
	Headers           map[string]string `mapstructure:"headers"`
	Timeout           time.Duration     `mapstructure:"timeout"`
	Compression       string            `mapstructure:"compression"` // "none", "gzip"
	RetryEnabled      bool              `mapstructure:"retry_enabled"`
	RetryMaxAttempts  int               `mapstructure:"retry_max_attempts"`
}

// GetTracesConfig returns the effective OTLP config for traces
func (c *ObservabilityConfig) GetTracesConfig() OTLPConfig {
	if c.Traces != nil {
		return mergeOTLPConfigs(c.OTLP, *c.Traces)
	}
	return c.OTLP
}

// GetLogsConfig returns the effective OTLP config for logs
func (c *ObservabilityConfig) GetLogsConfig() OTLPConfig {
	if c.Logs != nil {
		return mergeOTLPConfigs(c.OTLP, *c.Logs)
	}
	return c.OTLP
}

// GetMetricsConfig returns the effective OTLP config for metrics
func (c *ObservabilityConfig) GetMetricsConfig() OTLPConfig {
	if c.Metrics != nil {
		return mergeOTLPConfigs(c.OTLP, *c.Metrics)
	}
	return c.OTLP
}

// mergeOTLPConfigs merges signal-specific config over global defaults
func mergeOTLPConfigs(base OTLPConfig, override OTLPConfig) OTLPConfig {
	result := base // Start with base

	// Override non-zero/non-empty values
	if override.Endpoint != "" {
		result.Endpoint = override.Endpoint
	}
	if override.Protocol != "" {
		result.Protocol = override.Protocol
	}
	// Note: Insecure is a bool, so we can't detect if it was explicitly set to false.
	// We assume if the override struct exists, the user wants to use its Insecure value.
	result.Insecure = override.Insecure

	if override.TLSCertFile != "" {
		result.TLSCertFile = override.TLSCertFile
	}
	if override.TLSClientCertFile != "" {
		result.TLSClientCertFile = override.TLSClientCertFile
	}
	if override.TLSClientKeyFile != "" {
		result.TLSClientKeyFile = override.TLSClientKeyFile
	}

	// Merge headers (signal-specific headers override global)
	if override.Headers != nil {
		result.Headers = make(map[string]string)
		for k, v := range base.Headers {
			result.Headers[k] = v
		}
		for k, v := range override.Headers {
			result.Headers[k] = v
		}
	}

	if override.Timeout != 0 {
		result.Timeout = override.Timeout
	}
	if override.Compression != "" {
		result.Compression = override.Compression
	}
	if override.RetryMaxAttempts != 0 {
		result.RetryEnabled = override.RetryEnabled
		result.RetryMaxAttempts = override.RetryMaxAttempts
	}

	return result
}

var defineFlagsOnce sync.Once

// Load loads configuration from multiple sources with the following precedence:
// 1. Explicit overrides (v.Set) – used only for interactive password prompt
// 2. Command line flags
// 3. Environment variables
// 4. Config file
// 5. Default values
func Load() (*Config, error) {
	v := viper.New()

	// Defaults (lowest priority)
	setDefaults(v)

	// --- Flags ---
	defineFlags()
	if !pflag.Parsed() {
		pflag.Parse()
	}

	// --- Config file ---
	cfgPath, _ := pflag.CommandLine.GetString("config")
	if cfgPath != "" {
		v.SetConfigFile(cfgPath)
	} else {
		v.SetConfigName("tidb-graphql")
		v.SetConfigType("yaml")
		v.AddConfigPath("/etc/tidb-graphql/")
		v.AddConfigPath("$HOME/.tidb-graphql")
		v.AddConfigPath(".")
	}

	if err := v.ReadInConfig(); err != nil {
		if cfgPath != "" {
			return nil, fmt.Errorf("failed to read config file %q: %w", cfgPath, err)
		}
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
	}

	// --- Environment variables ---
	// Canonical keys: dot + snake_case
	// Env vars: TIGQL_DATABASE_MAX_OPEN_CONNS
	v.SetEnvPrefix("TIGQL")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	v.AutomaticEnv()

	// --- Flags binding (highest normal priority) ---
	bindChangedFlagsToViper(v)

	// --- DSN from file (explicit override) ---
	if v.GetString("database.dsn") == "" && v.GetString("database.dsn_file") != "" {
		if dsn, err := readPasswordFile(v.GetString("database.dsn_file")); err != nil {
			return nil, fmt.Errorf("failed to read database DSN file: %w", err)
		} else {
			v.Set("database.dsn", dsn)
		}
	}

	// --- Secure password input (explicit override) ---
	if v.GetString("database.password") == "" && v.GetString("database.password_file") != "" {
		if pwd, err := readPasswordFile(v.GetString("database.password_file")); err != nil {
			return nil, fmt.Errorf("failed to read database password file: %w", err)
		} else {
			v.Set("database.password", pwd)
		}
	}
	if v.GetString("database.password") == "" && v.GetBool("database.password_prompt") {
		pwd, err := promptPassword()
		if err != nil {
			return nil, fmt.Errorf("failed to read password: %w", err)
		}
		v.Set("database.password", pwd)
	}

	// --- Unmarshal (strict) ---
	var cfg Config
	if err := v.UnmarshalExact(
		&cfg,
		viper.DecodeHook(
			mapstructure.ComposeDecodeHookFunc(
				mapstructure.StringToTimeDurationHookFunc(),
				stringToStringSliceHookFunc(","),
			),
		),
	); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &cfg, nil
}

// bindChangedFlagsToViper copies only explicitly-set flags into Viper,
// preserving precedence: flags > env > file > defaults.
func bindChangedFlagsToViper(v *viper.Viper) {
	pflag.CommandLine.Visit(func(f *pflag.Flag) {
		if f.Name == "config" || f.Name == "version" {
			return
		}

		switch f.Value.Type() {
		case "string":
			val, _ := pflag.CommandLine.GetString(f.Name)
			v.Set(f.Name, val)
		case "int":
			val, _ := pflag.CommandLine.GetInt(f.Name)
			v.Set(f.Name, val)
		case "bool":
			val, _ := pflag.CommandLine.GetBool(f.Name)
			v.Set(f.Name, val)
		case "float64":
			val, _ := pflag.CommandLine.GetFloat64(f.Name)
			v.Set(f.Name, val)
		case "duration":
			val, _ := pflag.CommandLine.GetDuration(f.Name)
			v.Set(f.Name, val)
		case "stringSlice":
			val, _ := pflag.CommandLine.GetStringSlice(f.Name)
			v.Set(f.Name, val)
		default:
			v.Set(f.Name, f.Value.String())
		}
	})
}

// defineFlags defines all command line flags using canonical snake_case keys.
func defineFlags() {
	defineFlagsOnce.Do(func() {
		// Database connection flags
		pflag.String("database.dsn", "", "Complete MySQL DSN (user:pass@tcp(host:port)/db)")
		pflag.String("database.dsn_file", "", "Path to file containing database DSN (use @- for stdin)")

		// Database discrete connection flags (used when DSN is not set)
		pflag.String("database.host", "", "Database host")
		pflag.Int("database.port", 0, "Database port")
		pflag.String("database.user", "", "Database user")
		pflag.String("database.password", "", "Database password")
		pflag.String("database.password_file", "", "Path to file containing database password (use @- for stdin)")
		pflag.Bool("database.password_prompt", false, "Prompt for database password securely")
		pflag.String("database.database", "", "Database name")

		// Database TLS flags
		pflag.String("database.tls.mode", "", "TLS mode (off, skip-verify, verify-ca, verify-full)")
		pflag.String("database.tls.ca_file", "", "Path to CA certificate for server verification")
		pflag.String("database.tls.ca_file_env", "", "Env var containing CA certificate path")
		pflag.String("database.tls.cert_file", "", "Path to client certificate for mTLS")
		pflag.String("database.tls.cert_file_env", "", "Env var containing client certificate path")
		pflag.String("database.tls.key_file", "", "Path to client private key for mTLS")
		pflag.String("database.tls.key_file_env", "", "Env var containing client key path")
		pflag.String("database.tls.server_name", "", "Override TLS server name for verification")

		// Database pool flags
		pflag.Int("database.pool.max_open", 0, "Maximum open database connections")
		pflag.Int("database.pool.max_idle", 0, "Maximum idle connections in pool")
		pflag.Duration("database.pool.max_lifetime", 0, "Connection max lifetime (e.g. 5m, 30s)")
		pflag.Duration("database.connection_timeout", 0, "Max time to wait for database on startup (0 = fail immediately)")
		pflag.Duration("database.connection_retry_interval", 0, "Initial interval between connection retries")

		// Server flags
		pflag.Int("server.port", 0, "HTTP server port")
		pflag.Int("server.graphql_max_depth", 0, "Maximum GraphQL query depth limit")
		pflag.Int("server.graphql_max_complexity", 0, "Maximum GraphQL query complexity limit")
		pflag.Int("server.graphql_max_rows", 0, "Maximum estimated GraphQL rows per request")
		pflag.Int("server.graphql_default_limit", 0, "Default list limit for GraphQL list queries")
		pflag.Duration("server.schema_refresh_min_interval", 0, "Minimum interval between schema refresh checks")
		pflag.Duration("server.schema_refresh_max_interval", 0, "Maximum interval between schema refresh checks")
		pflag.Bool("server.graphiql_enabled", false, "Enable GraphiQL UI for /graphql (dev only)")
		pflag.Bool("server.auth.oidc_enabled", false, "Enable OIDC/JWKS authentication middleware")
		pflag.String("server.auth.oidc_issuer_url", "", "OIDC issuer URL (for discovery and JWKS)")
		pflag.String("server.auth.oidc_audience", "", "Expected JWT audience (client ID)")
		pflag.Duration("server.auth.oidc_clock_skew", 0, "Allowed JWT clock skew (e.g. 2m)")
		pflag.Bool("server.auth.oidc_skip_tls_verify", false, "Skip TLS verification for OIDC provider (dev only)")
		pflag.Bool("server.auth.db_role_enabled", false, "Enable database role-based authorization (SET ROLE)")
		pflag.String("server.auth.db_role_claim_name", "", "JWT claim name containing database role (default: db_role)")
		pflag.Bool("server.auth.db_role_validation_enabled", false, "Validate db_role against discovered database roles")
		pflag.String("server.auth.db_role_introspection_role", "", "Database role used for schema introspection when role auth is enabled")
		pflag.Bool("server.rate_limit_enabled", false, "Enable global rate limiting for all HTTP endpoints")
		pflag.Float64("server.rate_limit_rps", 0, "Global rate limit requests per second")
		pflag.Int("server.rate_limit_burst", 0, "Global rate limit burst size")
		pflag.Bool("server.cors_enabled", false, "Enable CORS (Cross-Origin Resource Sharing)")
		pflag.StringSlice("server.cors_allowed_origins", nil, "Allowed CORS origins (comma-separated or repeated)")
		pflag.StringSlice("server.cors_allowed_methods", nil, "Allowed CORS methods (comma-separated or repeated)")
		pflag.StringSlice("server.cors_allowed_headers", nil, "Allowed CORS headers (comma-separated or repeated)")
		pflag.StringSlice("server.cors_expose_headers", nil, "CORS headers to expose to browser (comma-separated or repeated)")
		pflag.Bool("server.cors_allow_credentials", false, "Allow credentials in CORS requests")
		pflag.Int("server.cors_max_age", 0, "CORS preflight cache duration (seconds)")
		pflag.Duration("server.read_timeout", 0, "HTTP server read timeout")
		pflag.Duration("server.write_timeout", 0, "HTTP server write timeout")
		pflag.Duration("server.idle_timeout", 0, "HTTP server idle timeout")
		pflag.Duration("server.shutdown_timeout", 0, "HTTP server graceful shutdown timeout")
		pflag.Duration("server.health_check_timeout", 0, "Health check timeout")

		// TLS flags
		pflag.String("server.tls_mode", "", "TLS mode: off, auto (self-signed), file (default: off)")
		pflag.String("server.tls_cert_file", "", "Path to TLS certificate file (for file mode)")
		pflag.String("server.tls_key_file", "", "Path to TLS private key file (for file mode)")
		pflag.String("server.tls_auto_cert_dir", "", "Directory for auto-generated certificates (default: .tls)")

		// Observability flags
		pflag.String("observability.service_name", "", "Service name for observability")
		pflag.String("observability.service_version", "", "Service version for observability")
		pflag.String("observability.environment", "", "Environment name (dev, staging, prod)")
		pflag.Bool("observability.metrics_enabled", false, "Enable metrics collection")
		pflag.Bool("observability.tracing_enabled", false, "Enable distributed tracing")
		pflag.Bool("observability.sqlcommenter_enabled", false, "Inject trace context into SQL queries")

		// Logging flags (under observability)
		pflag.String("observability.logging.level", "", "Log level (debug, info, warn, error)")
		pflag.String("observability.logging.format", "", "Log format (json, text)")
		pflag.Bool("observability.logging.exports_enabled", false, "Enable OTLP log export")

		// Global OTLP flags
		pflag.String("observability.otlp.endpoint", "", "OTLP endpoint for all signals (e.g., localhost:4317)")
		pflag.String("observability.otlp.protocol", "", "OTLP protocol for all signals (grpc, http/protobuf)")
		pflag.Bool("observability.otlp.insecure", false, "Use insecure connection (no TLS)")
		pflag.String("observability.otlp.tls_cert_file", "", "Path to TLS certificate file for server verification")
		pflag.String("observability.otlp.tls_client_cert_file", "", "Path to client certificate file for mTLS")
		pflag.String("observability.otlp.tls_client_key_file", "", "Path to client key file for mTLS")
		pflag.Duration("observability.otlp.timeout", 0, "OTLP export timeout")
		pflag.String("observability.otlp.compression", "", "OTLP compression (none, gzip)")
		pflag.Bool("observability.otlp.retry_enabled", false, "Enable retry on transient errors")
		pflag.Int("observability.otlp.retry_max_attempts", 0, "Maximum retry attempts")

		// Signal-specific OTLP flags (traces)
		pflag.String("observability.traces.endpoint", "", "OTLP endpoint for traces only")
		pflag.String("observability.traces.protocol", "", "OTLP protocol for traces (grpc, http/protobuf)")
		pflag.Bool("observability.traces.insecure", false, "Use insecure connection for traces")
		pflag.Duration("observability.traces.timeout", 0, "Timeout for trace exports")

		// Signal-specific OTLP flags (logs)
		pflag.String("observability.logs.endpoint", "", "OTLP endpoint for logs only")
		pflag.String("observability.logs.protocol", "", "OTLP protocol for logs (grpc, http/protobuf)")
		pflag.Bool("observability.logs.insecure", false, "Use insecure connection for logs")
		pflag.Duration("observability.logs.timeout", 0, "Timeout for log exports")

		// Signal-specific OTLP flags (metrics)
		pflag.String("observability.metrics.endpoint", "", "OTLP endpoint for metrics only")
		pflag.Bool("observability.metrics.insecure", false, "Use insecure connection for metrics")
		pflag.Duration("observability.metrics.timeout", 0, "Timeout for metric exports")

		// Config file flag
		pflag.StringP("config", "c", "", "Config file path")
	})
}

// setDefaults sets default values (lowest precedence).
func setDefaults(v *viper.Viper) {
	// Database connection defaults
	v.SetDefault("database.dsn", "")
	v.SetDefault("database.dsn_file", "")

	// Database discrete connection defaults
	v.SetDefault("database.host", "localhost")
	v.SetDefault("database.port", 4000)
	v.SetDefault("database.user", "tidb_graphql")
	v.SetDefault("database.password", "")
	v.SetDefault("database.password_file", "")
	v.SetDefault("database.password_prompt", false)
	v.SetDefault("database.database", "test")

	// Database TLS defaults
	v.SetDefault("database.tls.mode", "")
	v.SetDefault("database.tls.ca_file", "")
	v.SetDefault("database.tls.ca_file_env", "")
	v.SetDefault("database.tls.cert_file", "")
	v.SetDefault("database.tls.cert_file_env", "")
	v.SetDefault("database.tls.key_file", "")
	v.SetDefault("database.tls.key_file_env", "")
	v.SetDefault("database.tls.server_name", "")

	// Database pool defaults
	v.SetDefault("database.pool.max_open", 25)
	v.SetDefault("database.pool.max_idle", 5)
	v.SetDefault("database.pool.max_lifetime", 5*time.Minute)
	v.SetDefault("database.connection_timeout", 60*time.Second)
	v.SetDefault("database.connection_retry_interval", 2*time.Second)

	// Server defaults
	v.SetDefault("server.port", 8080)
	v.SetDefault("server.graphql_max_depth", 5)
	v.SetDefault("server.graphql_max_complexity", 0)
	v.SetDefault("server.graphql_max_rows", 0)
	v.SetDefault("server.graphql_default_limit", 100)
	v.SetDefault("server.schema_refresh_min_interval", 30*time.Second)
	v.SetDefault("server.schema_refresh_max_interval", 5*time.Minute)
	v.SetDefault("server.graphiql_enabled", false)
	v.SetDefault("server.auth.oidc_enabled", false)
	v.SetDefault("server.auth.oidc_issuer_url", "")
	v.SetDefault("server.auth.oidc_audience", "")
	v.SetDefault("server.auth.oidc_clock_skew", 2*time.Minute)
	v.SetDefault("server.auth.oidc_skip_tls_verify", false)
	v.SetDefault("server.auth.db_role_enabled", false)
	v.SetDefault("server.auth.db_role_claim_name", "db_role")
	v.SetDefault("server.auth.db_role_validation_enabled", true)
	v.SetDefault("server.auth.db_role_introspection_role", "")
	v.SetDefault("server.rate_limit_enabled", false)
	v.SetDefault("server.rate_limit_rps", 0.0)
	v.SetDefault("server.rate_limit_burst", 0)
	v.SetDefault("server.cors_enabled", false)
	v.SetDefault("server.cors_allowed_origins", []string{})
	v.SetDefault("server.cors_allowed_methods", []string{"GET", "POST", "OPTIONS"})
	v.SetDefault("server.cors_allowed_headers", []string{"Content-Type", "Authorization"})
	v.SetDefault("server.cors_expose_headers", []string{})
	v.SetDefault("server.cors_allow_credentials", false)
	v.SetDefault("server.cors_max_age", 86400)
	v.SetDefault("server.read_timeout", 15*time.Second)
	v.SetDefault("server.write_timeout", 15*time.Second)
	v.SetDefault("server.idle_timeout", 60*time.Second)
	v.SetDefault("server.shutdown_timeout", 30*time.Second)
	v.SetDefault("server.health_check_timeout", 2*time.Second)

	// TLS defaults
	v.SetDefault("server.tls_mode", "off")
	v.SetDefault("server.tls_cert_file", "")
	v.SetDefault("server.tls_key_file", "")
	v.SetDefault("server.tls_auto_cert_dir", ".tls")

	// Observability defaults
	v.SetDefault("observability.service_name", "tidb-graphql")
	v.SetDefault("observability.service_version", "")
	v.SetDefault("observability.environment", "development")
	v.SetDefault("observability.metrics_enabled", true)
	v.SetDefault("observability.tracing_enabled", false)
	v.SetDefault("observability.sqlcommenter_enabled", true) // Enable by default when tracing is on

	// Logging defaults (under observability)
	v.SetDefault("observability.logging.level", "info")
	v.SetDefault("observability.logging.format", "json")
	v.SetDefault("observability.logging.exports_enabled", false)

	// Global OTLP defaults
	v.SetDefault("observability.otlp.endpoint", "localhost:4317")
	v.SetDefault("observability.otlp.protocol", "grpc")
	v.SetDefault("observability.otlp.insecure", false)
	v.SetDefault("observability.otlp.tls_cert_file", "")
	v.SetDefault("observability.otlp.tls_client_cert_file", "")
	v.SetDefault("observability.otlp.tls_client_key_file", "")
	v.SetDefault("observability.otlp.timeout", 10*time.Second)
	v.SetDefault("observability.otlp.compression", "gzip")
	v.SetDefault("observability.otlp.retry_enabled", true)
	v.SetDefault("observability.otlp.retry_max_attempts", 3)

	// Schema filter defaults (allow all)
	v.SetDefault("schema_filters.allow_tables", []string{"*"})
	v.SetDefault("schema_filters.allow_columns", map[string][]string{
		"*": {"*"},
	})
	v.SetDefault("schema_filters.scan_views_enabled", false)
	v.SetDefault("schema_filters.deny_mutation_tables", []string{})
	v.SetDefault("schema_filters.deny_mutation_columns", map[string][]string{})

	// Naming defaults
	v.SetDefault("naming.plural_overrides", map[string]string{})
	v.SetDefault("naming.singular_overrides", map[string]string{})
}

// promptPassword prompts the user for a password without echoing to terminal.
func promptPassword() (string, error) {
	fmt.Print("Enter database password: ")
	bytePassword, err := term.ReadPassword(int(syscall.Stdin))
	fmt.Println()
	if err != nil {
		return "", err
	}
	return string(bytePassword), nil
}

func readPasswordFile(path string) (string, error) {
	var data []byte
	var err error

	if path == "@-" {
		data, err = io.ReadAll(os.Stdin)
	} else {
		data, err = os.ReadFile(path)
	}
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

func stringToStringSliceHookFunc(sep string) mapstructure.DecodeHookFunc {
	return func(from reflect.Type, to reflect.Type, data interface{}) (interface{}, error) {
		if from.Kind() != reflect.String || to != reflect.TypeOf([]string{}) {
			return data, nil
		}

		raw := strings.TrimSpace(data.(string))
		if raw == "" {
			return []string{}, nil
		}

		parts := strings.Split(raw, sep)
		for i := range parts {
			parts[i] = strings.TrimSpace(parts[i])
		}
		return parts, nil
	}
}

// ValidationError represents a configuration validation error with context.
type ValidationError struct {
	Field   string
	Message string
	Hint    string
}

func (e ValidationError) Error() string {
	if e.Hint != "" {
		return fmt.Sprintf("%s: %s (hint: %s)", e.Field, e.Message, e.Hint)
	}
	return fmt.Sprintf("%s: %s", e.Field, e.Message)
}

// ValidationWarning represents a non-fatal configuration issue.
type ValidationWarning struct {
	Field   string
	Message string
	Hint    string
}

// ValidationResult contains the results of configuration validation.
type ValidationResult struct {
	Errors   []ValidationError
	Warnings []ValidationWarning
}

// HasErrors returns true if there are any validation errors.
func (r *ValidationResult) HasErrors() bool {
	return len(r.Errors) > 0
}

// Error returns a combined error message if there are validation errors.
func (r *ValidationResult) Error() string {
	if !r.HasErrors() {
		return ""
	}
	var msgs []string
	for _, e := range r.Errors {
		msgs = append(msgs, e.Error())
	}
	return strings.Join(msgs, "; ")
}

// Validate checks the configuration for errors and returns validation results.
// It returns both errors (fatal) and warnings (non-fatal issues).
func (c *Config) Validate() *ValidationResult {
	result := &ValidationResult{}

	// Validate database config
	c.Database.validate(result)

	// Validate server config
	c.Server.validate(result)

	// Validate observability config
	c.Observability.validate(result)

	return result
}

func (d *DatabaseConfig) validate(result *ValidationResult) {
	// Port range validation (only if not using connection string)
	if d.ConnectionString == "" && (d.Port < 1 || d.Port > 65535) {
		result.Errors = append(result.Errors, ValidationError{
			Field:   "database.port",
			Message: fmt.Sprintf("port %d is out of valid range (1-65535)", d.Port),
		})
	}

	// Validate new TLS configuration
	d.TLS.validate(result)

	// Connection pool validation
	if d.Pool.MaxOpen < 0 {
		result.Errors = append(result.Errors, ValidationError{
			Field:   "database.pool.max_open",
			Message: "max_open cannot be negative",
		})
	}
	if d.Pool.MaxIdle < 0 {
		result.Errors = append(result.Errors, ValidationError{
			Field:   "database.pool.max_idle",
			Message: "max_idle cannot be negative",
		})
	}
	if d.Pool.MaxIdle > d.Pool.MaxOpen && d.Pool.MaxOpen > 0 {
		result.Warnings = append(result.Warnings, ValidationWarning{
			Field:   "database.pool.max_idle",
			Message: "max_idle is greater than max_open",
			Hint:    "idle connections will be limited to max_open",
		})
	}

	// Connection retry validation
	if d.ConnectionTimeout > 0 && d.ConnectionRetryInterval > d.ConnectionTimeout {
		result.Warnings = append(result.Warnings, ValidationWarning{
			Field:   "database.connection_retry_interval",
			Message: "connection_retry_interval is greater than connection_timeout",
			Hint:    "only one connection attempt will be made",
		})
	}
	if d.ConnectionRetryInterval < 0 {
		result.Errors = append(result.Errors, ValidationError{
			Field:   "database.connection_retry_interval",
			Message: "connection_retry_interval cannot be negative",
		})
	}
	if d.ConnectionTimeout > 0 && d.ConnectionRetryInterval == 0 {
		result.Errors = append(result.Errors, ValidationError{
			Field:   "database.connection_retry_interval",
			Message: "connection_retry_interval must be greater than 0 when connection_timeout is set",
			Hint:    "set a retry interval such as 2s, or set connection_timeout to 0 to disable retries",
		})
	}
	if d.ConnectionTimeout < 0 {
		result.Errors = append(result.Errors, ValidationError{
			Field:   "database.connection_timeout",
			Message: "connection_timeout cannot be negative",
		})
	}
}

func (t *DatabaseTLSConfig) validate(result *ValidationResult) {
	// Mode validation
	validModes := map[string]bool{"": true, "off": true, "skip-verify": true, "verify-ca": true, "verify-full": true}
	if !validModes[t.Mode] {
		result.Errors = append(result.Errors, ValidationError{
			Field:   "database.tls.mode",
			Message: fmt.Sprintf("invalid TLS mode %q", t.Mode),
			Hint:    "valid values are: off, skip-verify, verify-ca, verify-full",
		})
	}

	// CA file is required for verify-ca and verify-full
	caFile := t.resolveCAFile()
	if (t.Mode == "verify-ca" || t.Mode == "verify-full") && caFile == "" {
		result.Errors = append(result.Errors, ValidationError{
			Field:   "database.tls.ca_file",
			Message: "CA file is required for verify-ca and verify-full modes",
			Hint:    "set ca_file or ca_file_env to specify the CA certificate",
		})
	}

	// Client cert and key must both be specified or neither
	certFile := t.resolveCertFile()
	keyFile := t.resolveKeyFile()
	if (certFile != "" && keyFile == "") || (certFile == "" && keyFile != "") {
		result.Errors = append(result.Errors, ValidationError{
			Field:   "database.tls.cert_file",
			Message: "both cert_file and key_file must be specified for client certificate authentication",
			Hint:    "provide both cert_file and key_file, or neither",
		})
	}

	// Warn about skip-verify in non-empty mode
	if t.Mode == "skip-verify" {
		result.Warnings = append(result.Warnings, ValidationWarning{
			Field:   "database.tls.mode",
			Message: "skip-verify mode does not verify server certificates",
			Hint:    "use verify-ca or verify-full in production",
		})
	}
}

func (s *ServerConfig) validate(result *ValidationResult) {
	// Port range validation
	if s.Port < 1 || s.Port > 65535 {
		result.Errors = append(result.Errors, ValidationError{
			Field:   "server.port",
			Message: fmt.Sprintf("port %d is out of valid range (1-65535)", s.Port),
		})
	}

	// Rate limit validation
	if s.RateLimitEnabled {
		if s.RateLimitRPS <= 0 {
			result.Errors = append(result.Errors, ValidationError{
				Field:   "server.rate_limit_rps",
				Message: "rate_limit_rps must be greater than 0 when rate limiting is enabled",
			})
		}
		if s.RateLimitBurst <= 0 {
			result.Errors = append(result.Errors, ValidationError{
				Field:   "server.rate_limit_burst",
				Message: "rate_limit_burst must be greater than 0 when rate limiting is enabled",
			})
		}
	}

	if !s.RateLimitEnabled && (s.RateLimitRPS > 0 || s.RateLimitBurst > 0) {
		result.Warnings = append(result.Warnings, ValidationWarning{
			Field:   "server.rate_limit_enabled",
			Message: "rate limit values are set but rate limiting is disabled",
			Hint:    "enable server.rate_limit_enabled to apply rate limits",
		})
	}

	if s.GraphQLMaxDepth < 0 {
		result.Errors = append(result.Errors, ValidationError{
			Field:   "server.graphql_max_depth",
			Message: "graphql_max_depth cannot be negative",
		})
	}
	if s.GraphQLMaxComplexity < 0 {
		result.Errors = append(result.Errors, ValidationError{
			Field:   "server.graphql_max_complexity",
			Message: "graphql_max_complexity cannot be negative",
		})
	}
	if s.GraphQLMaxRows < 0 {
		result.Errors = append(result.Errors, ValidationError{
			Field:   "server.graphql_max_rows",
			Message: "graphql_max_rows cannot be negative",
		})
	}
	if s.GraphQLDefaultLimit < 0 {
		result.Errors = append(result.Errors, ValidationError{
			Field:   "server.graphql_default_limit",
			Message: "graphql_default_limit cannot be negative",
		})
	}

	// CORS validation
	if s.CORSEnabled {
		if len(s.CORSAllowedOrigins) == 0 {
			result.Errors = append(result.Errors, ValidationError{
				Field:   "server.cors_allowed_origins",
				Message: "CORS enabled but no allowed origins configured",
				Hint:    "set cors_allowed_origins or disable CORS",
			})
		}

		hasWildcard := false
		for _, origin := range s.CORSAllowedOrigins {
			if strings.TrimSpace(origin) == "*" {
				hasWildcard = true
				break
			}
		}

		if hasWildcard && s.CORSAllowCredentials {
			result.Errors = append(result.Errors, ValidationError{
				Field:   "server.cors_allowed_origins",
				Message: "wildcard origin (*) cannot be used with credentials",
				Hint:    "use specific origins with credentials, or wildcard without credentials",
			})
		}

		if hasWildcard {
			result.Warnings = append(result.Warnings, ValidationWarning{
				Field:   "server.cors_allowed_origins",
				Message: "CORS wildcard origin enabled",
				Hint:    "use specific origins in production for better security",
			})
		}
	}

	tlsEnabled := s.TLSMode != "" && s.TLSMode != "off"
	if s.CORSEnabled && tlsEnabled && len(s.CORSAllowedOrigins) > 0 {
		onlyHTTP := true
		for _, origin := range s.CORSAllowedOrigins {
			origin = strings.TrimSpace(origin)
			if origin == "" || origin == "*" {
				onlyHTTP = false
				break
			}
			if strings.HasPrefix(origin, "https://") {
				onlyHTTP = false
				break
			}
			if !strings.HasPrefix(origin, "http://") {
				onlyHTTP = false
				break
			}
		}
		if onlyHTTP {
			result.Warnings = append(result.Warnings, ValidationWarning{
				Field:   "server.cors_allowed_origins",
				Message: "CORS allowed origins are http:// only while TLS is enabled",
				Hint:    "use https:// origins when serving over TLS",
			})
		}
	}

	if s.Auth.DBRoleEnabled && !s.Auth.OIDCEnabled {
		result.Errors = append(result.Errors, ValidationError{
			Field:   "server.auth.db_role_enabled",
			Message: "db_role_enabled requires OIDC to be enabled",
			Hint:    "set server.auth.oidc_enabled=true or disable db_role_enabled",
		})
	}

	if s.Auth.DBRoleEnabled && s.Auth.DBRoleIntrospectionRole == "" {
		result.Errors = append(result.Errors, ValidationError{
			Field:   "server.auth.db_role_introspection_role",
			Message: "introspection role is required when db_role_enabled is true",
			Hint:    "set server.auth.db_role_introspection_role to a role with necessary schema read access",
		})
	}

	if s.Auth.OIDCEnabled {
		if s.Auth.OIDCIssuerURL == "" {
			result.Errors = append(result.Errors, ValidationError{
				Field:   "server.auth.oidc_issuer_url",
				Message: "issuer URL is required when OIDC is enabled",
			})
		}
		if s.Auth.OIDCAudience == "" {
			result.Errors = append(result.Errors, ValidationError{
				Field:   "server.auth.oidc_audience",
				Message: "audience is required when OIDC is enabled",
			})
		}
	}

	// TLS validation
	validTLSModes := map[string]bool{"": true, "off": true, "auto": true, "file": true}
	if !validTLSModes[s.TLSMode] {
		result.Errors = append(result.Errors, ValidationError{
			Field:   "server.tls_mode",
			Message: fmt.Sprintf("invalid TLS mode %q", s.TLSMode),
			Hint:    "valid values are: off, auto, file",
		})
	}

	if s.TLSMode == "file" {
		if s.TLSCertFile == "" {
			result.Errors = append(result.Errors, ValidationError{
				Field:   "server.tls_cert_file",
				Message: "TLS cert file required when tls_mode is 'file'",
			})
		}
		if s.TLSKeyFile == "" {
			result.Errors = append(result.Errors, ValidationError{
				Field:   "server.tls_key_file",
				Message: "TLS key file required when tls_mode is 'file'",
			})
		}
	}
}

func (o *ObservabilityConfig) validate(result *ValidationResult) {
	// Log level validation
	validLogLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	if !validLogLevels[o.Logging.Level] {
		result.Errors = append(result.Errors, ValidationError{
			Field:   "observability.logging.level",
			Message: fmt.Sprintf("invalid log level %q", o.Logging.Level),
			Hint:    "valid values are: debug, info, warn, error",
		})
	}

	// Log format validation
	validLogFormats := map[string]bool{"json": true, "text": true}
	if !validLogFormats[o.Logging.Format] {
		result.Errors = append(result.Errors, ValidationError{
			Field:   "observability.logging.format",
			Message: fmt.Sprintf("invalid log format %q", o.Logging.Format),
			Hint:    "valid values are: json, text",
		})
	}

	// OTLP protocol validation
	o.OTLP.validate("observability.otlp", result)

	// Signal-specific OTLP validation
	if o.Traces != nil {
		o.Traces.validate("observability.traces", result)
	}
	if o.Logs != nil {
		o.Logs.validate("observability.logs", result)
	}
	if o.Metrics != nil {
		o.Metrics.validate("observability.metrics", result)
	}
}

func (o *OTLPConfig) validate(prefix string, result *ValidationResult) {
	validProtocols := map[string]bool{"": true, "grpc": true, "http/protobuf": true}
	if !validProtocols[o.Protocol] {
		result.Errors = append(result.Errors, ValidationError{
			Field:   prefix + ".protocol",
			Message: fmt.Sprintf("invalid OTLP protocol %q", o.Protocol),
			Hint:    "valid values are: grpc, http/protobuf",
		})
	}

	if o.Protocol == "http/protobuf" {
		if !validOTLPEndpoint(o.Endpoint) {
			result.Errors = append(result.Errors, ValidationError{
				Field:   prefix + ".endpoint",
				Message: fmt.Sprintf("invalid OTLP endpoint %q for http/protobuf", o.Endpoint),
				Hint:    "use host:port or a full URL",
			})
		}
	}

	validCompressions := map[string]bool{"": true, "none": true, "gzip": true}
	if !validCompressions[o.Compression] {
		result.Errors = append(result.Errors, ValidationError{
			Field:   prefix + ".compression",
			Message: fmt.Sprintf("invalid OTLP compression %q", o.Compression),
			Hint:    "valid values are: none, gzip",
		})
	}

	if o.RetryMaxAttempts < 0 {
		result.Errors = append(result.Errors, ValidationError{
			Field:   prefix + ".retry_max_attempts",
			Message: "retry_max_attempts cannot be negative",
		})
	}
}

func validOTLPEndpoint(endpoint string) bool {
	if endpoint == "" {
		return false
	}
	if strings.Contains(endpoint, "://") {
		parsed, err := url.Parse(endpoint)
		if err != nil {
			return false
		}
		return parsed.Host != ""
	}
	_, _, err := net.SplitHostPort(endpoint)
	return err == nil
}

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
