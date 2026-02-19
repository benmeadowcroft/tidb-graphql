package config

import (
	"time"

	"tidb-graphql/internal/naming"
	"tidb-graphql/internal/schemafilter"
)

// Config holds the application configuration.
type Config struct {
	Database      DatabaseConfig      `mapstructure:"database"`
	Server        ServerConfig        `mapstructure:"server"`
	Observability ObservabilityConfig `mapstructure:"observability"`
	SchemaFilters schemafilter.Config `mapstructure:"schema_filters"`
	TypeMappings  TypeMappingsConfig  `mapstructure:"type_mappings"`
	Naming        naming.Config       `mapstructure:"naming"`
}

// TypeMappingsConfig controls explicit SQL-to-GraphQL type overrides.
type TypeMappingsConfig struct {
	// UUIDColumns maps table glob patterns to column glob patterns that should be treated as UUID.
	UUIDColumns map[string][]string `mapstructure:"uuid_columns"`
	// TinyInt1BooleanColumns maps table glob patterns to tinyint(1) column glob patterns
	// that should be treated as GraphQL Boolean.
	TinyInt1BooleanColumns map[string][]string `mapstructure:"tinyint1_boolean_columns"`
	// TinyInt1IntColumns maps table glob patterns to tinyint(1) column glob patterns
	// that should be treated as GraphQL Int (escape hatch when tinyint(1) is not semantic boolean).
	TinyInt1IntColumns map[string][]string `mapstructure:"tinyint1_int_columns"`
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
	// MyCnfFile points to a MySQL defaults file (.my.cnf style) used as an
	// alternative to DSN/discrete settings.
	// Supported keys are loaded from [client] (and database from [mysql] fallback).
	// Configured via "mycnf_file" in YAML or TIGQL_DATABASE_MYCNF_FILE env var.
	MyCnfFile string `mapstructure:"mycnf_file"`

	// Discrete connection fields (used when DSN is not set)
	Host           string `mapstructure:"host"`
	Port           int    `mapstructure:"port"`
	User           string `mapstructure:"user"`
	Password       string `mapstructure:"password"`
	PasswordFile   string `mapstructure:"password_file"`
	PasswordPrompt bool   `mapstructure:"password_prompt"`
	Database       string `mapstructure:"database"`

	// TLS holds the TLS/SSL configuration for database connections.
	TLS DatabaseTLSConfig `mapstructure:"tls"`

	// Connection pool settings
	Pool PoolConfig `mapstructure:"pool"`

	// ConnectionTimeout is the max time to wait for DB on startup.
	ConnectionTimeout time.Duration `mapstructure:"connection_timeout"`
	// ConnectionRetryInterval is the initial interval between connection retries.
	ConnectionRetryInterval time.Duration `mapstructure:"connection_retry_interval"`
}

const defaultDatabaseName = "test"

type myCnfSettings struct {
	Host      string
	Port      int
	User      string
	Password  string
	Database  string
	TLSMode   string
	HasPort   bool
	HasDBName bool
}

// AuthConfig holds authentication and authorization parameters.
type AuthConfig struct {
	OIDCEnabled             bool          `mapstructure:"oidc_enabled"`
	OIDCIssuerURL           string        `mapstructure:"oidc_issuer_url"`
	OIDCAudience            string        `mapstructure:"oidc_audience"`
	OIDCClockSkew           time.Duration `mapstructure:"oidc_clock_skew"`
	OIDCSkipTLSVerify       bool          `mapstructure:"oidc_skip_tls_verify"`
	DBRoleEnabled           bool          `mapstructure:"db_role_enabled"`
	DBRoleClaimName         string        `mapstructure:"db_role_claim_name"`
	DBRoleIntrospectionRole string        `mapstructure:"db_role_introspection_role"`
	RoleSchemaInclude       []string      `mapstructure:"role_schema_include"`
	RoleSchemaExclude       []string      `mapstructure:"role_schema_exclude"`
	RoleSchemaMaxRoles      int           `mapstructure:"role_schema_max_roles"`
}

// SearchConfig holds vector search configuration.
type SearchConfig struct {
	VectorRequireIndex bool `mapstructure:"vector_require_index"`
	VectorMaxTopK      int  `mapstructure:"vector_max_top_k"`
}

// AdminConfig controls administrative endpoint exposure and authentication.
type AdminConfig struct {
	SchemaReloadEnabled bool   `mapstructure:"schema_reload_enabled"`
	AuthToken           string `mapstructure:"auth_token"`
	AuthTokenFile       string `mapstructure:"auth_token_file"`
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
	Search                   SearchConfig  `mapstructure:"search"`
	Auth                     AuthConfig    `mapstructure:"auth"`
	Admin                    AdminConfig   `mapstructure:"admin"`
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
	TLSMode        string `mapstructure:"tls_mode"`          // "off", "auto", or "file" (default: "off")
	TLSCertFile    string `mapstructure:"tls_cert_file"`     // Path to certificate file (for "file" mode)
	TLSKeyFile     string `mapstructure:"tls_key_file"`      // Path to private key file (for "file" mode)
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
	TraceSampleRatio    float64       `mapstructure:"trace_sample_ratio"`
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
