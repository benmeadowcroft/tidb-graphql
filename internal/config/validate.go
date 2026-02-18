package config

import (
	"fmt"
	"net"
	"net/url"
	"path"
	"regexp"
	"strings"

	"tidb-graphql/internal/naming"
	"tidb-graphql/internal/schemafilter"
)

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

	// Validate explicit type mappings
	c.TypeMappings.validate(result)

	// Validate schema filters
	validateSchemaFilters(result, c.SchemaFilters)

	// Validate naming config
	validateNamingConfig(result, c.Naming)

	return result
}

func (t *TypeMappingsConfig) validate(result *ValidationResult) {
	validatePatternMap(result, "type_mappings.uuid_columns", t.UUIDColumns)
	validatePatternMap(result, "type_mappings.tinyint1_boolean_columns", t.TinyInt1BooleanColumns)
	validatePatternMap(result, "type_mappings.tinyint1_int_columns", t.TinyInt1IntColumns)
}

func validateSchemaFilters(result *ValidationResult, filters schemafilter.Config) {
	validateGlobList(result, "schema_filters.allow_tables", filters.AllowTables)
	validateGlobList(result, "schema_filters.deny_tables", filters.DenyTables)
	validateGlobList(result, "schema_filters.deny_mutation_tables", filters.DenyMutationTables)
	validatePatternMap(result, "schema_filters.allow_columns", filters.AllowColumns)
	validatePatternMap(result, "schema_filters.deny_columns", filters.DenyColumns)
	validatePatternMap(result, "schema_filters.deny_mutation_columns", filters.DenyMutationColumns)
}

var mutationReservedTypeNames = map[string]bool{
	"MutationError":        true,
	"InputValidationError": true,
	"ConflictError":        true,
	"ConstraintError":      true,
	"PermissionError":      true,
	"NotFoundError":        true,
	"InternalError":        true,
}

var pascalCaseTypePattern = regexp.MustCompile(`^[A-Z][A-Za-z0-9]*$`)

func validateNamingConfig(result *ValidationResult, cfg naming.Config) {
	for tableName, typeName := range cfg.TypeOverrides {
		tableName = strings.TrimSpace(tableName)
		typeName = strings.TrimSpace(typeName)
		if tableName == "" {
			result.Errors = append(result.Errors, ValidationError{
				Field:   "naming.type_overrides",
				Message: "table name cannot be empty",
			})
			continue
		}
		if typeName == "" {
			result.Errors = append(result.Errors, ValidationError{
				Field:   "naming.type_overrides",
				Message: fmt.Sprintf("type override for table %q cannot be empty", tableName),
			})
			continue
		}
		if !pascalCaseTypePattern.MatchString(typeName) {
			result.Errors = append(result.Errors, ValidationError{
				Field:   "naming.type_overrides",
				Message: fmt.Sprintf("type override %q for table %q must be PascalCase", typeName, tableName),
			})
			continue
		}
		if mutationReservedTypeNames[typeName] {
			result.Errors = append(result.Errors, ValidationError{
				Field:   "naming.type_overrides",
				Message: fmt.Sprintf("type override %q for table %q is reserved for mutation error/result types", typeName, tableName),
			})
		}
	}
}

func validatePatternMap(result *ValidationResult, field string, patternMap map[string][]string) {
	for tablePattern, columnPatterns := range patternMap {
		if strings.TrimSpace(tablePattern) == "" {
			result.Errors = append(result.Errors, ValidationError{
				Field:   field,
				Message: "table pattern cannot be empty",
			})
			continue
		}
		if _, err := path.Match(strings.ToLower(tablePattern), "probe"); err != nil {
			result.Errors = append(result.Errors, ValidationError{
				Field:   field,
				Message: fmt.Sprintf("invalid table glob pattern %q: %v", tablePattern, err),
			})
		}
		for _, columnPattern := range columnPatterns {
			if strings.TrimSpace(columnPattern) == "" {
				result.Errors = append(result.Errors, ValidationError{
					Field:   field,
					Message: fmt.Sprintf("column pattern for table pattern %q cannot be empty", tablePattern),
				})
				continue
			}
			if _, err := path.Match(strings.ToLower(columnPattern), "probe"); err != nil {
				result.Errors = append(result.Errors, ValidationError{
					Field:   field,
					Message: fmt.Sprintf("invalid column glob pattern %q for table pattern %q: %v", columnPattern, tablePattern, err),
				})
			}
		}
	}
}

func (d *DatabaseConfig) validate(result *ValidationResult) {
	if strings.TrimSpace(d.MyCnfFile) != "" && (strings.TrimSpace(d.ConnectionString) != "" || strings.TrimSpace(d.ConnectionStringFile) != "") {
		result.Errors = append(result.Errors, ValidationError{
			Field:   "database.mycnf_file",
			Message: "mycnf_file is mutually exclusive with dsn/dsn_file",
			Hint:    "set either mycnf_file or dsn/dsn_file, not both",
		})
	}

	if strings.TrimSpace(d.MyCnfFile) != "" {
		settings, err := parseMyCnfFile(d.MyCnfFile)
		if err != nil {
			result.Errors = append(result.Errors, ValidationError{
				Field:   "database.mycnf_file",
				Message: fmt.Sprintf("failed to parse my.cnf file: %v", err),
				Hint:    "provide a valid MySQL defaults file with [client] settings",
			})
		} else {
			if d.Host == "" && settings.Host != "" {
				d.Host = settings.Host
			}
			if d.Port == 0 && settings.HasPort {
				d.Port = settings.Port
			}
			if d.User == "" && settings.User != "" {
				d.User = settings.User
			}
			if d.Password == "" && settings.Password != "" {
				d.Password = settings.Password
			}
			if d.TLS.Mode == "" && settings.TLSMode != "" {
				d.TLS.Mode = settings.TLSMode
			}
			if settings.HasDBName {
				if strings.TrimSpace(d.Database) == "" {
					d.Database = settings.Database
				} else if d.Database != settings.Database {
					result.Errors = append(result.Errors, ValidationError{
						Field:   "database.database",
						Message: fmt.Sprintf("database mismatch: database.database=%q but database.mycnf_file targets %q", d.Database, settings.Database),
						Hint:    "either remove database.database or set it to match my.cnf database",
					})
				}
			}
		}
	}

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

	effectiveDatabase, _, err := resolveEffectiveDatabaseName(d.Database, d.ConnectionString, d.MyCnfFile)
	if err != nil {
		switch {
		case strings.HasPrefix(err.Error(), "database.dsn"):
			result.Errors = append(result.Errors, ValidationError{
				Field:   "database.dsn",
				Message: err.Error(),
				Hint:    "set a valid MySQL DSN in database.dsn/database.dsn_file",
			})
		case strings.HasPrefix(err.Error(), "database.mycnf_file"):
			result.Errors = append(result.Errors, ValidationError{
				Field:   "database.mycnf_file",
				Message: err.Error(),
				Hint:    "set a valid my.cnf file and include [client] database or database.database",
			})
		case strings.Contains(err.Error(), "mismatch"):
			result.Errors = append(result.Errors, ValidationError{
				Field:   "database.database",
				Message: err.Error(),
				Hint:    "either remove database.database or set it to match the DSN/my.cnf database",
			})
		default:
			result.Errors = append(result.Errors, ValidationError{
				Field:   "database.database",
				Message: err.Error(),
				Hint:    "set database.database or include a /database in database.dsn/database.dsn_file or database.mycnf_file",
			})
		}
		return
	}

	// Keep runtime behavior deterministic for callers that consume Database.Database.
	d.Database = effectiveDatabase
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
	if s.Search.VectorMaxTopK < 0 {
		result.Errors = append(result.Errors, ValidationError{
			Field:   "server.search.vector_max_top_k",
			Message: "vector_max_top_k cannot be negative",
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
	if s.Auth.DBRoleEnabled {
		if s.Auth.RoleSchemaMaxRoles <= 0 {
			result.Errors = append(result.Errors, ValidationError{
				Field:   "server.auth.role_schema_max_roles",
				Message: "role_schema_max_roles must be greater than 0",
			})
		}
		validateGlobList(result, "server.auth.role_schema_include", s.Auth.RoleSchemaInclude)
		validateGlobList(result, "server.auth.role_schema_exclude", s.Auth.RoleSchemaExclude)
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

func validateGlobList(result *ValidationResult, field string, patterns []string) {
	for _, pattern := range patterns {
		if strings.TrimSpace(pattern) == "" {
			result.Errors = append(result.Errors, ValidationError{
				Field:   field,
				Message: "glob pattern cannot be empty",
			})
			continue
		}
		if _, err := path.Match(strings.ToLower(pattern), "probe"); err != nil {
			result.Errors = append(result.Errors, ValidationError{
				Field:   field,
				Message: fmt.Sprintf("invalid glob pattern %q: %v", pattern, err),
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
