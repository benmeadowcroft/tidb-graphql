package config

import (
	"fmt"
	"io"
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
)

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
	databaseNameExplicit := databaseNameExplicitlyConfigured(v)
	if err := validateSingleStdinFileSource(v); err != nil {
		return nil, err
	}

	// --- DSN from file (explicit override) ---
	if v.GetString("database.dsn") == "" && v.GetString("database.dsn_file") != "" {
		if dsn, err := readPasswordFile(v.GetString("database.dsn_file")); err != nil {
			return nil, fmt.Errorf("failed to read database DSN file: %w", err)
		} else {
			v.Set("database.dsn", dsn)
		}
	}

	// --- MySQL defaults file (explicit override) ---
	myCnfHasDatabase := false
	if myCnfPath := strings.TrimSpace(v.GetString("database.mycnf_file")); myCnfPath != "" {
		settings, err := parseMyCnfFile(myCnfPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load database my.cnf file: %w", err)
		}

		if settings.Host != "" {
			v.Set("database.host", settings.Host)
		}
		if settings.HasPort {
			v.Set("database.port", settings.Port)
		}
		if settings.User != "" {
			v.Set("database.user", settings.User)
		}
		if settings.Password != "" {
			v.Set("database.password", settings.Password)
		}
		if settings.TLSMode != "" {
			v.Set("database.tls.mode", settings.TLSMode)
		}
		if settings.HasDBName {
			myCnfHasDatabase = true
			if !databaseNameExplicit {
				v.Set("database.database", settings.Database)
			}
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

	// --- Admin auth token from file (explicit override) ---
	if v.GetString("server.admin.auth_token") == "" && v.GetString("server.admin.auth_token_file") != "" {
		tokenPath := v.GetString("server.admin.auth_token_file")
		token, err := readPasswordFile(tokenPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read admin auth token file: %w", err)
		}
		if token == "" {
			return nil, fmt.Errorf("admin auth token file %q is empty", tokenPath)
		}
		v.Set("server.admin.auth_token", token)
	}

	// --- Effective database normalization ---
	// If DSN is set and database.database was not explicitly configured, treat the
	// default placeholder as unset so DSN can become the canonical database target.
	if strings.TrimSpace(v.GetString("database.dsn")) != "" &&
		!databaseNameExplicit &&
		strings.TrimSpace(v.GetString("database.database")) == defaultDatabaseName {
		v.Set("database.database", "")
	}

	// In my.cnf mode, force explicit database when not provided by user nor file.
	if strings.TrimSpace(v.GetString("database.mycnf_file")) != "" &&
		!databaseNameExplicit &&
		!myCnfHasDatabase &&
		strings.TrimSpace(v.GetString("database.database")) == defaultDatabaseName {
		v.Set("database.database", "")
	}

	effectiveDatabase, _, err := resolveEffectiveDatabaseName(
		v.GetString("database.database"),
		v.GetString("database.dsn"),
		v.GetString("database.mycnf_file"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve effective database name: %w", err)
	}
	v.Set("database.database", effectiveDatabase)

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
		pflag.String("database.mycnf_file", "", "Path to MySQL defaults file (.my.cnf format)")

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
		pflag.Int("server.graphql_default_limit", 0, "Default page size for GraphQL connection collection queries")
		pflag.Bool("server.search.vector_require_index", false, "Require vector-search-capable indexes before exposing vector search fields")
		pflag.Int("server.search.vector_max_top_k", 0, "Maximum allowed page size (first) for vector search connection fields")
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
		pflag.String("server.auth.db_role_introspection_role", "", "Database role used for schema introspection when role auth is enabled")
		pflag.StringSlice("server.auth.role_schema_include", nil, "Role glob patterns to include for role-specific schema snapshots (default: [*])")
		pflag.StringSlice("server.auth.role_schema_exclude", nil, "Role glob patterns to exclude from role-specific schema snapshots")
		pflag.Int("server.auth.role_schema_max_roles", 0, "Maximum number of role-specific schemas to build when db_role_enabled is true")
		pflag.Bool("server.admin.schema_reload_enabled", false, "Enable /admin/reload-schema endpoint")
		pflag.String("server.admin.auth_token", "", "Shared secret required in X-Admin-Token header when admin endpoint is enabled without OIDC")
		pflag.String("server.admin.auth_token_file", "", "Path to file containing admin auth token (use @- for stdin)")
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
		pflag.Float64("observability.trace_sample_ratio", 0, "Trace sampling ratio from 0.0 to 1.0")
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
	v.SetDefault("database.mycnf_file", "")

	// Database discrete connection defaults
	v.SetDefault("database.host", "localhost")
	v.SetDefault("database.port", 4000)
	v.SetDefault("database.user", "tidb_graphql")
	v.SetDefault("database.password", "")
	v.SetDefault("database.password_file", "")
	v.SetDefault("database.password_prompt", false)
	v.SetDefault("database.database", defaultDatabaseName)

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
	v.SetDefault("server.search.vector_require_index", true)
	v.SetDefault("server.search.vector_max_top_k", 100)
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
	v.SetDefault("server.auth.db_role_introspection_role", "")
	v.SetDefault("server.auth.role_schema_include", []string{"*"})
	v.SetDefault("server.auth.role_schema_exclude", []string{})
	v.SetDefault("server.auth.role_schema_max_roles", 64)
	v.SetDefault("server.admin.schema_reload_enabled", false)
	v.SetDefault("server.admin.auth_token", "")
	v.SetDefault("server.admin.auth_token_file", "")
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
	v.SetDefault("observability.trace_sample_ratio", 1.0)
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

	// Explicit type mapping defaults.
	v.SetDefault("type_mappings.uuid_columns", map[string][]string{})
	v.SetDefault("type_mappings.tinyint1_boolean_columns", map[string][]string{})
	v.SetDefault("type_mappings.tinyint1_int_columns", map[string][]string{})

	// Naming defaults
	v.SetDefault("naming.plural_overrides", map[string]string{})
	v.SetDefault("naming.singular_overrides", map[string]string{})
	v.SetDefault("naming.type_overrides", map[string]string{})
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

func readRawFile(path string) (string, error) {
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
	return string(data), nil
}

func validateSingleStdinFileSource(v *viper.Viper) error {
	stdinBackedKeys := []string{
		"database.dsn_file",
		"database.mycnf_file",
		"database.password_file",
		"server.admin.auth_token_file",
	}

	var configured []string
	for _, key := range stdinBackedKeys {
		if strings.TrimSpace(v.GetString(key)) == "@-" {
			configured = append(configured, key)
		}
	}

	if len(configured) > 1 {
		return fmt.Errorf(
			"multiple stdin-backed file settings use @- (%s); only one @- source is allowed",
			strings.Join(configured, ", "),
		)
	}

	return nil
}

func parseMyCnfFile(path string) (myCnfSettings, error) {
	raw, err := readRawFile(path)
	if err != nil {
		return myCnfSettings{}, err
	}
	return parseMyCnf(raw)
}

func parseMyCnf(raw string) (myCnfSettings, error) {
	settings := myCnfSettings{}
	section := ""

	lines := strings.Split(raw, "\n")
	for i, line := range lines {
		lineno := i + 1
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}

		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.ToLower(strings.TrimSpace(line[1 : len(line)-1]))
			continue
		}

		key, value, ok := parseMyCnfKeyValue(line)
		if !ok {
			return myCnfSettings{}, fmt.Errorf("invalid my.cnf syntax on line %d", lineno)
		}

		key = strings.ToLower(key)
		switch section {
		case "client":
			switch key {
			case "host":
				settings.Host = value
			case "port":
				if value == "" {
					return myCnfSettings{}, fmt.Errorf("invalid my.cnf port on line %d: empty value", lineno)
				}
				port, err := parsePort(value)
				if err != nil {
					return myCnfSettings{}, fmt.Errorf("invalid my.cnf port on line %d: %w", lineno, err)
				}
				settings.Port = port
				settings.HasPort = true
			case "user":
				settings.User = value
			case "password":
				settings.Password = value
			case "database":
				settings.Database = value
				settings.HasDBName = true
			case "ssl-mode":
				tlsMode, err := mapMyCnfSSLMode(value)
				if err != nil {
					return myCnfSettings{}, fmt.Errorf("invalid my.cnf ssl-mode on line %d: %w", lineno, err)
				}
				settings.TLSMode = tlsMode
			}
		case "mysql":
			if key == "database" && !settings.HasDBName {
				settings.Database = value
				settings.HasDBName = true
			}
		}
	}

	return settings, nil
}

func parseMyCnfKeyValue(line string) (key string, value string, ok bool) {
	if parts := strings.SplitN(line, "=", 2); len(parts) == 2 {
		key = strings.TrimSpace(parts[0])
		value = strings.TrimSpace(parts[1])
		value = stripOptionalQuotes(value)
		return key, value, key != ""
	}

	parts := strings.Fields(line)
	if len(parts) < 2 {
		return "", "", false
	}
	key = strings.TrimSpace(parts[0])
	value = strings.TrimSpace(strings.Join(parts[1:], " "))
	value = stripOptionalQuotes(value)
	return key, value, key != ""
}

func stripOptionalQuotes(value string) string {
	if len(value) >= 2 {
		if (value[0] == '\'' && value[len(value)-1] == '\'') || (value[0] == '"' && value[len(value)-1] == '"') {
			return value[1 : len(value)-1]
		}
	}
	return value
}

func parsePort(raw string) (int, error) {
	port, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, err
	}
	if port < 1 || port > 65535 {
		return 0, fmt.Errorf("port %d is out of valid range (1-65535)", port)
	}
	return port, nil
}

func mapMyCnfSSLMode(value string) (string, error) {
	mode := strings.ToUpper(strings.TrimSpace(value))
	switch mode {
	case "":
		return "", nil
	case "DISABLED":
		return "off", nil
	case "REQUIRED", "PREFERRED":
		return "skip-verify", nil
	case "VERIFY_CA":
		return "verify-ca", nil
	case "VERIFY_IDENTITY":
		return "verify-full", nil
	default:
		return "", fmt.Errorf("unsupported ssl-mode %q", value)
	}
}

func databaseNameExplicitlyConfigured(v *viper.Viper) bool {
	if _, ok := os.LookupEnv("TIGQL_DATABASE_DATABASE"); ok {
		return true
	}
	if flag := pflag.CommandLine.Lookup("database.database"); flag != nil && flag.Changed {
		return true
	}
	return v.InConfig("database.database")
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
