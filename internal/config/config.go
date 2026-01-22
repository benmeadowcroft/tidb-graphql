// Package config loads configuration from files, env vars, and flags, and validates it.
// See docs/reference/configuration.md and docs/how-to/config-precedence.md.
package config

import (
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

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

// DatabaseConfig holds database connection parameters.
type DatabaseConfig struct {
	Host            string        `mapstructure:"host"`
	Port            int           `mapstructure:"port"`
	User            string        `mapstructure:"user"`
	Password        string        `mapstructure:"password"`
	PasswordFile    string        `mapstructure:"password_file"`
	PasswordPrompt  bool          `mapstructure:"password_prompt"`
	Database        string        `mapstructure:"database"`
	TLSMode         string        `mapstructure:"tls_mode"` // TLS mode: skip-verify, true, or false
	MaxOpenConns    int           `mapstructure:"max_open_conns"`
	MaxIdleConns    int           `mapstructure:"max_idle_conns"`
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`
}

// ServerConfig holds HTTP server parameters.
type ServerConfig struct {
	Port                     int           `mapstructure:"port"`
	GraphQLMaxDepth          int           `mapstructure:"graphql_max_depth"`
	GraphQLMaxComplexity     int           `mapstructure:"graphql_max_complexity"`
	GraphQLMaxRows           int           `mapstructure:"graphql_max_rows"`
	GraphQLDefaultListLimit  int           `mapstructure:"graphql_list_limit_default"`
	SchemaRefreshMinInterval time.Duration `mapstructure:"schema_refresh_min_interval"`
	SchemaRefreshMaxInterval time.Duration `mapstructure:"schema_refresh_max_interval"`
	GraphiQLEnabled          bool          `mapstructure:"graphiql_enabled"`
	OIDCEnabled              bool          `mapstructure:"oidc_enabled"`
	OIDCIssuerURL            string        `mapstructure:"oidc_issuer_url"`
	OIDCAudience             string        `mapstructure:"oidc_audience"`
	OIDCClockSkew            time.Duration `mapstructure:"oidc_clock_skew"`
	OIDCSkipTLSVerify        bool          `mapstructure:"oidc_skip_tls_verify"`
	DBRoleEnabled            bool          `mapstructure:"db_role_enabled"`
	DBRoleClaimName          string        `mapstructure:"db_role_claim_name"`
	DBRoleValidation         bool          `mapstructure:"db_role_validation"`
	DBRoleIntrospectionRole  string        `mapstructure:"db_role_introspection_role"`
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
	TLSEnabled           bool   `mapstructure:"tls_enabled"`             // Enable HTTPS (default: false for backward compat)
	TLSCertMode          string `mapstructure:"tls_cert_mode"`           // "file" or "selfsigned" (default: "file")
	TLSCertFile          string `mapstructure:"tls_cert_file"`           // Path to certificate file (for "file" mode)
	TLSKeyFile           string `mapstructure:"tls_key_file"`            // Path to private key file (for "file" mode)
	TLSSelfSignedCertDir string `mapstructure:"tls_selfsigned_cert_dir"` // Directory for self-signed certs (default: ".tls")
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

	// Normalize database.tls_mode when YAML uses boolean literals.
	if rawTLSMode := v.Get("database.tls_mode"); rawTLSMode != nil {
		if tlsModeBool, ok := rawTLSMode.(bool); ok {
			v.Set("database.tls_mode", strconv.FormatBool(tlsModeBool))
		}
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
		default:
			v.Set(f.Name, f.Value.String())
		}
	})
}

// defineFlags defines all command line flags using canonical snake_case keys.
func defineFlags() {
	defineFlagsOnce.Do(func() {
		// Database flags
		pflag.String("database.host", "", "Database host")
		pflag.Int("database.port", 0, "Database port")
		pflag.String("database.user", "", "Database user")
		pflag.String("database.password", "", "Database password")
		pflag.String("database.password_file", "", "Path to file containing database password (use @- for stdin)")
		pflag.Bool("database.password_prompt", false, "Prompt for database password securely")
		pflag.String("database.database", "", "Database name")
		pflag.String("database.tls_mode", "", "TLS mode (skip-verify, true, false)")
		pflag.Int("database.max_open_conns", 0, "Maximum open database connections")
		pflag.Int("database.max_idle_conns", 0, "Maximum idle connections in pool")
		pflag.Duration("database.conn_max_lifetime", 0, "Connection max lifetime (e.g. 5m, 30s)")

		// Server flags
		pflag.Int("server.port", 0, "HTTP server port")
		pflag.Int("server.graphql_max_depth", 0, "Maximum GraphQL query depth limit")
		pflag.Int("server.graphql_max_complexity", 0, "Maximum GraphQL query complexity limit")
		pflag.Int("server.graphql_max_rows", 0, "Maximum estimated GraphQL rows per request")
		pflag.Int("server.graphql_list_limit_default", 0, "Default list limit for GraphQL list queries")
		pflag.Duration("server.schema_refresh_min_interval", 0, "Minimum interval between schema refresh checks")
		pflag.Duration("server.schema_refresh_max_interval", 0, "Maximum interval between schema refresh checks")
		pflag.Bool("server.graphiql_enabled", false, "Enable GraphiQL UI for /graphql (dev only)")
		pflag.Bool("server.oidc_enabled", false, "Enable OIDC/JWKS authentication middleware")
		pflag.String("server.oidc_issuer_url", "", "OIDC issuer URL (for discovery and JWKS)")
		pflag.String("server.oidc_audience", "", "Expected JWT audience (client ID)")
		pflag.Duration("server.oidc_clock_skew", 0, "Allowed JWT clock skew (e.g. 2m)")
		pflag.Bool("server.oidc_skip_tls_verify", false, "Skip TLS verification for OIDC provider (dev only)")
		pflag.Bool("server.db_role_enabled", false, "Enable database role-based authorization (SET ROLE)")
		pflag.String("server.db_role_claim_name", "", "JWT claim name containing database role (default: db_role)")
		pflag.Bool("server.db_role_validation", false, "Validate db_role against discovered database roles")
		pflag.String("server.db_role_introspection_role", "", "Database role used for schema introspection when role auth is enabled")
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
		pflag.Bool("server.tls_enabled", false, "Enable HTTPS server")
		pflag.String("server.tls_cert_mode", "", "TLS certificate mode: file, selfsigned (default: file)")
		pflag.String("server.tls_cert_file", "", "Path to TLS certificate file (for file mode)")
		pflag.String("server.tls_key_file", "", "Path to TLS private key file (for file mode)")
		pflag.String("server.tls_selfsigned_cert_dir", "", "Directory for self-signed certificates (default: .tls)")

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
	// Database defaults
	v.SetDefault("database.host", "localhost")
	v.SetDefault("database.port", 4000)
	v.SetDefault("database.user", "tidb_graphql")
	v.SetDefault("database.password", "")
	v.SetDefault("database.password_file", "")
	v.SetDefault("database.password_prompt", false)
	v.SetDefault("database.database", "test")
	v.SetDefault("database.tls_mode", "skip-verify") // Default for TiDB Cloud compatibility
	v.SetDefault("database.max_open_conns", 25)
	v.SetDefault("database.max_idle_conns", 5)
	v.SetDefault("database.conn_max_lifetime", 5*time.Minute)

	// Server defaults
	v.SetDefault("server.port", 8080)
	v.SetDefault("server.graphql_max_depth", 5)
	v.SetDefault("server.graphql_max_complexity", 0)
	v.SetDefault("server.graphql_max_rows", 0)
	v.SetDefault("server.graphql_list_limit_default", 100)
	v.SetDefault("server.schema_refresh_min_interval", 30*time.Second)
	v.SetDefault("server.schema_refresh_max_interval", 5*time.Minute)
	v.SetDefault("server.graphiql_enabled", false)
	v.SetDefault("server.oidc_enabled", false)
	v.SetDefault("server.oidc_issuer_url", "")
	v.SetDefault("server.oidc_audience", "")
	v.SetDefault("server.oidc_clock_skew", 2*time.Minute)
	v.SetDefault("server.oidc_skip_tls_verify", false)
	v.SetDefault("server.db_role_enabled", false)
	v.SetDefault("server.db_role_claim_name", "db_role")
	v.SetDefault("server.db_role_validation", true)
	v.SetDefault("server.db_role_introspection_role", "")
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
	v.SetDefault("server.tls_enabled", false)
	v.SetDefault("server.tls_cert_mode", "file")
	v.SetDefault("server.tls_cert_file", "")
	v.SetDefault("server.tls_key_file", "")
	v.SetDefault("server.tls_selfsigned_cert_dir", ".tls")

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
	v.SetDefault("schema_filters.scan_views", false)

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
	// Port range validation
	if d.Port < 1 || d.Port > 65535 {
		result.Errors = append(result.Errors, ValidationError{
			Field:   "database.port",
			Message: fmt.Sprintf("port %d is out of valid range (1-65535)", d.Port),
		})
	}

	// TLS mode validation
	validTLSModes := map[string]bool{"": true, "skip-verify": true, "true": true, "false": true}
	if !validTLSModes[d.TLSMode] {
		result.Errors = append(result.Errors, ValidationError{
			Field:   "database.tls_mode",
			Message: fmt.Sprintf("invalid TLS mode %q", d.TLSMode),
			Hint:    "valid values are: skip-verify, true, false",
		})
	}

	// Connection pool validation
	if d.MaxOpenConns < 0 {
		result.Errors = append(result.Errors, ValidationError{
			Field:   "database.max_open_conns",
			Message: "max_open_conns cannot be negative",
		})
	}
	if d.MaxIdleConns < 0 {
		result.Errors = append(result.Errors, ValidationError{
			Field:   "database.max_idle_conns",
			Message: "max_idle_conns cannot be negative",
		})
	}
	if d.MaxIdleConns > d.MaxOpenConns && d.MaxOpenConns > 0 {
		result.Warnings = append(result.Warnings, ValidationWarning{
			Field:   "database.max_idle_conns",
			Message: "max_idle_conns is greater than max_open_conns",
			Hint:    "idle connections will be limited to max_open_conns",
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
	if s.GraphQLDefaultListLimit < 0 {
		result.Errors = append(result.Errors, ValidationError{
			Field:   "server.graphql_list_limit_default",
			Message: "graphql_list_limit_default cannot be negative",
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

	if s.CORSEnabled && s.TLSEnabled && len(s.CORSAllowedOrigins) > 0 {
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

	if s.DBRoleEnabled && !s.OIDCEnabled {
		result.Errors = append(result.Errors, ValidationError{
			Field:   "server.db_role_enabled",
			Message: "db_role_enabled requires OIDC to be enabled",
			Hint:    "set server.oidc_enabled=true or disable db_role_enabled",
		})
	}

	if s.DBRoleEnabled && s.DBRoleIntrospectionRole == "" {
		result.Errors = append(result.Errors, ValidationError{
			Field:   "server.db_role_introspection_role",
			Message: "introspection role is required when db_role_enabled is true",
			Hint:    "set server.db_role_introspection_role to a role with necessary schema read access",
		})
	}

	if s.OIDCEnabled {
		if s.OIDCIssuerURL == "" {
			result.Errors = append(result.Errors, ValidationError{
				Field:   "server.oidc_issuer_url",
				Message: "issuer URL is required when OIDC is enabled",
			})
		}
		if s.OIDCAudience == "" {
			result.Errors = append(result.Errors, ValidationError{
				Field:   "server.oidc_audience",
				Message: "audience is required when OIDC is enabled",
			})
		}
	}

	// TLS validation
	if s.TLSEnabled {
		validCertModes := map[string]bool{"file": true, "selfsigned": true}
		if !validCertModes[s.TLSCertMode] {
			result.Errors = append(result.Errors, ValidationError{
				Field:   "server.tls_cert_mode",
				Message: fmt.Sprintf("invalid TLS cert mode %q", s.TLSCertMode),
				Hint:    "valid values are: file, selfsigned",
			})
		}

		if s.TLSCertMode == "file" {
			if s.TLSCertFile == "" {
				result.Errors = append(result.Errors, ValidationError{
					Field:   "server.tls_cert_file",
					Message: "TLS cert file required when tls_cert_mode is 'file'",
				})
			}
			if s.TLSKeyFile == "" {
				result.Errors = append(result.Errors, ValidationError{
					Field:   "server.tls_key_file",
					Message: "TLS key file required when tls_cert_mode is 'file'",
				})
			}
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

// DSN returns a MySQL-compatible data source name.
func (d *DatabaseConfig) DSN() string {
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?parseTime=true",
		d.User,
		d.Password,
		d.Host,
		d.Port,
		d.Database,
	)

	// Add TLS parameter if configured
	if d.TLSMode != "" {
		dsn += fmt.Sprintf("&tls=%s", d.TLSMode)
	}

	return dsn
}

// DSNWithoutDatabase returns a DSN that omits the default database.
// Useful for role-based auth where database access is granted via SET ROLE.
func (d *DatabaseConfig) DSNWithoutDatabase() string {
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/?parseTime=true",
		d.User,
		d.Password,
		d.Host,
		d.Port,
	)

	// Add TLS parameter if configured
	if d.TLSMode != "" {
		dsn += fmt.Sprintf("&tls=%s", d.TLSMode)
	}

	return dsn
}
