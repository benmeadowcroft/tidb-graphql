package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDatabaseConfig_DSN(t *testing.T) {
	tests := []struct {
		name     string
		config   DatabaseConfig
		expected string
	}{
		{
			name: "basic DSN",
			config: DatabaseConfig{
				Host:     "localhost",
				Port:     4000,
				User:     "root",
				Password: "password",
				Database: "test",
			},
			expected: "root:password@tcp(localhost:4000)/test?parseTime=true&loc=UTC",
		},
		{
			name: "with special characters in password",
			config: DatabaseConfig{
				Host:     "db.example.com",
				Port:     3306,
				User:     "admin",
				Password: "p@ss:w0rd!",
				Database: "mydb",
			},
			expected: "admin:p@ss:w0rd!@tcp(db.example.com:3306)/mydb?parseTime=true&loc=UTC",
		},
		{
			name: "empty password",
			config: DatabaseConfig{
				Host:     "localhost",
				Port:     4000,
				User:     "root",
				Password: "",
				Database: "test",
			},
			expected: "root:@tcp(localhost:4000)/test?parseTime=true&loc=UTC",
		},
		{
			name: "dsn with existing loc",
			config: DatabaseConfig{
				ConnectionString: "root:password@tcp(localhost:4000)/test?parseTime=true&loc=Local",
			},
			expected: "root:password@tcp(localhost:4000)/test?parseTime=true&loc=Local",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.config.DSN()
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestLoad_WithEnvVars tests configuration loading from environment variables
func TestLoad_WithEnvVars(t *testing.T) {
	// Save original env vars
	origHost := os.Getenv("TIGQL_DATABASE_HOST")
	origPort := os.Getenv("TIGQL_DATABASE_PORT")
	origUser := os.Getenv("TIGQL_DATABASE_USER")

	// Clean up after test
	t.Cleanup(func() {
		os.Setenv("TIGQL_DATABASE_HOST", origHost)
		os.Setenv("TIGQL_DATABASE_PORT", origPort)
		os.Setenv("TIGQL_DATABASE_USER", origUser)
		os.Unsetenv("TIGQL_DATABASE_PASSWORD")
		os.Unsetenv("TIGQL_DATABASE_DATABASE")
		os.Unsetenv("TIGQL_SERVER_PORT")
	})

	// Set test environment variables
	os.Setenv("TIGQL_DATABASE_HOST", "envhost")
	os.Setenv("TIGQL_DATABASE_PORT", "5000")
	os.Setenv("TIGQL_DATABASE_USER", "envuser")
	os.Setenv("TIGQL_DATABASE_PASSWORD", "envpass")
	os.Setenv("TIGQL_DATABASE_DATABASE", "envdb")
	os.Setenv("TIGQL_SERVER_PORT", "9999")

	// Verify env var naming convention
	assert.Equal(t, "envhost", os.Getenv("TIGQL_DATABASE_HOST"))
	assert.Equal(t, "5000", os.Getenv("TIGQL_DATABASE_PORT"))
	assert.Equal(t, "envuser", os.Getenv("TIGQL_DATABASE_USER"))
}

// Note: Full integration tests for Load() should be done in integration tests
// because Load() relies on global state (pflag.CommandLine) which is difficult
// to test in isolation without causing conflicts between tests.

func TestConfig_Validate(t *testing.T) {
	// Helper to create a valid base config
	validConfig := func() *Config {
		return &Config{
			Database: DatabaseConfig{
				Host:     "localhost",
				Port:     4000,
				User:     "root",
				Database: "test",
				TLS: DatabaseTLSConfig{
					Mode: "off",
				},
				Pool: PoolConfig{
					MaxOpen: 25,
					MaxIdle: 5,
				},
			},
			Server: ServerConfig{
				Port: 8080,
			},
			Observability: ObservabilityConfig{
				Logging: LoggingConfig{
					Level:  "info",
					Format: "json",
				},
				OTLP: OTLPConfig{
					Protocol:    "grpc",
					Compression: "gzip",
				},
			},
		}
	}

	t.Run("valid config passes validation", func(t *testing.T) {
		cfg := validConfig()
		result := cfg.Validate()
		assert.False(t, result.HasErrors())
		assert.Empty(t, result.Errors)
	})

	t.Run("invalid database port", func(t *testing.T) {
		cfg := validConfig()
		cfg.Database.Port = 0
		result := cfg.Validate()
		assert.True(t, result.HasErrors())
		assert.Contains(t, result.Error(), "database.port")
	})

	t.Run("invalid database port high", func(t *testing.T) {
		cfg := validConfig()
		cfg.Database.Port = 70000
		result := cfg.Validate()
		assert.True(t, result.HasErrors())
		assert.Contains(t, result.Error(), "database.port")
	})

	t.Run("invalid server port", func(t *testing.T) {
		cfg := validConfig()
		cfg.Server.Port = -1
		result := cfg.Validate()
		assert.True(t, result.HasErrors())
		assert.Contains(t, result.Error(), "server.port")
	})

	t.Run("invalid TLS mode", func(t *testing.T) {
		cfg := validConfig()
		cfg.Database.TLS.Mode = "invalid"
		result := cfg.Validate()
		assert.True(t, result.HasErrors())
		assert.Contains(t, result.Error(), "database.tls.mode")
	})

	t.Run("valid uuid column mapping patterns", func(t *testing.T) {
		cfg := validConfig()
		cfg.TypeMappings.UUIDColumns = map[string][]string{
			"*":      {"*_uuid"},
			"orders": {"id"},
		}
		result := cfg.Validate()
		assert.False(t, result.HasErrors())
	})

	t.Run("invalid uuid table glob pattern", func(t *testing.T) {
		cfg := validConfig()
		cfg.TypeMappings.UUIDColumns = map[string][]string{
			"[bad": {"id"},
		}
		result := cfg.Validate()
		assert.True(t, result.HasErrors())
		assert.Contains(t, result.Error(), "type_mappings.uuid_columns")
	})

	t.Run("invalid uuid column glob pattern", func(t *testing.T) {
		cfg := validConfig()
		cfg.TypeMappings.UUIDColumns = map[string][]string{
			"orders": {"[bad"},
		}
		result := cfg.Validate()
		assert.True(t, result.HasErrors())
		assert.Contains(t, result.Error(), "type_mappings.uuid_columns")
	})

	t.Run("valid TLS modes", func(t *testing.T) {
		for _, mode := range []string{"", "off", "skip-verify", "verify-ca", "verify-full"} {
			cfg := validConfig()
			if mode == "verify-ca" || mode == "verify-full" {
				cfg.Database.TLS.CAFile = "/path/to/ca.pem"
			}
			cfg.Database.TLS.Mode = mode
			result := cfg.Validate()
			assert.False(t, result.HasErrors(), "TLS mode %q should be valid", mode)
		}
	})

	t.Run("invalid log level", func(t *testing.T) {
		cfg := validConfig()
		cfg.Observability.Logging.Level = "invalid"
		result := cfg.Validate()
		assert.True(t, result.HasErrors())
		assert.Contains(t, result.Error(), "observability.logging.level")
	})

	t.Run("invalid log format", func(t *testing.T) {
		cfg := validConfig()
		cfg.Observability.Logging.Format = "xml"
		result := cfg.Validate()
		assert.True(t, result.HasErrors())
		assert.Contains(t, result.Error(), "observability.logging.format")
	})

	t.Run("invalid OTLP protocol", func(t *testing.T) {
		cfg := validConfig()
		cfg.Observability.OTLP.Protocol = "http"
		result := cfg.Validate()
		assert.True(t, result.HasErrors())
		assert.Contains(t, result.Error(), "observability.otlp.protocol")
	})

	t.Run("valid OTLP protocols", func(t *testing.T) {
		for _, protocol := range []string{"", "grpc", "http/protobuf"} {
			cfg := validConfig()
			cfg.Observability.OTLP.Protocol = protocol
			if protocol == "http/protobuf" {
				cfg.Observability.OTLP.Endpoint = "localhost:4318"
			}
			result := cfg.Validate()
			assert.False(t, result.HasErrors(), "protocol %q should be valid", protocol)
		}
	})

	t.Run("invalid OTLP http/protobuf endpoint", func(t *testing.T) {
		cfg := validConfig()
		cfg.Observability.OTLP.Protocol = "http/protobuf"
		cfg.Observability.OTLP.Endpoint = "localhost"
		result := cfg.Validate()
		assert.True(t, result.HasErrors())
		assert.Contains(t, result.Error(), "observability.otlp.endpoint")
	})

	t.Run("valid OTLP http/protobuf endpoint", func(t *testing.T) {
		cfg := validConfig()
		cfg.Observability.OTLP.Protocol = "http/protobuf"
		cfg.Observability.OTLP.Endpoint = "localhost:4318"
		result := cfg.Validate()
		assert.False(t, result.HasErrors())
	})

	t.Run("rate limit enabled without RPS", func(t *testing.T) {
		cfg := validConfig()
		cfg.Server.RateLimitEnabled = true
		cfg.Server.RateLimitRPS = 0
		cfg.Server.RateLimitBurst = 10
		result := cfg.Validate()
		assert.True(t, result.HasErrors())
		assert.Contains(t, result.Error(), "rate_limit_rps")
	})

	t.Run("rate limit enabled without burst", func(t *testing.T) {
		cfg := validConfig()
		cfg.Server.RateLimitEnabled = true
		cfg.Server.RateLimitRPS = 100
		cfg.Server.RateLimitBurst = 0
		result := cfg.Validate()
		assert.True(t, result.HasErrors())
		assert.Contains(t, result.Error(), "rate_limit_burst")
	})

	t.Run("rate limit valid config", func(t *testing.T) {
		cfg := validConfig()
		cfg.Server.RateLimitEnabled = true
		cfg.Server.RateLimitRPS = 100
		cfg.Server.RateLimitBurst = 10
		result := cfg.Validate()
		assert.False(t, result.HasErrors())
	})

	t.Run("rate limit disabled with values warns", func(t *testing.T) {
		cfg := validConfig()
		cfg.Server.RateLimitEnabled = false
		cfg.Server.RateLimitRPS = 100
		cfg.Server.RateLimitBurst = 10
		result := cfg.Validate()
		assert.False(t, result.HasErrors())
		assert.Len(t, result.Warnings, 1)
		assert.Contains(t, result.Warnings[0].Message, "rate limit values")
	})

	t.Run("CORS enabled without origins", func(t *testing.T) {
		cfg := validConfig()
		cfg.Server.CORSEnabled = true
		cfg.Server.CORSAllowedOrigins = []string{}
		result := cfg.Validate()
		assert.True(t, result.HasErrors())
		assert.Contains(t, result.Error(), "cors_allowed_origins")
	})

	t.Run("CORS wildcard with credentials", func(t *testing.T) {
		cfg := validConfig()
		cfg.Server.CORSEnabled = true
		cfg.Server.CORSAllowedOrigins = []string{"*"}
		cfg.Server.CORSAllowCredentials = true
		result := cfg.Validate()
		assert.True(t, result.HasErrors())
		assert.Contains(t, result.Error(), "wildcard")
	})

	t.Run("CORS wildcard without credentials warns", func(t *testing.T) {
		cfg := validConfig()
		cfg.Server.CORSEnabled = true
		cfg.Server.CORSAllowedOrigins = []string{"*"}
		cfg.Server.CORSAllowCredentials = false
		result := cfg.Validate()
		assert.False(t, result.HasErrors())
		assert.Len(t, result.Warnings, 1)
		assert.Contains(t, result.Warnings[0].Message, "wildcard")
	})

	t.Run("CORS specific origins valid", func(t *testing.T) {
		cfg := validConfig()
		cfg.Server.CORSEnabled = true
		cfg.Server.CORSAllowedOrigins = []string{"https://example.com"}
		cfg.Server.CORSAllowCredentials = true
		result := cfg.Validate()
		assert.False(t, result.HasErrors())
	})

	t.Run("CORS http origins with TLS enabled warns", func(t *testing.T) {
		cfg := validConfig()
		cfg.Server.CORSEnabled = true
		cfg.Server.TLSMode = "auto"
		cfg.Server.CORSAllowedOrigins = []string{"http://example.com"}
		result := cfg.Validate()
		assert.False(t, result.HasErrors())
		assert.Len(t, result.Warnings, 1)
		assert.Contains(t, result.Warnings[0].Message, "http://")
	})

	t.Run("TLS file mode requires cert files", func(t *testing.T) {
		cfg := validConfig()
		cfg.Server.TLSMode = "file"
		cfg.Server.TLSCertFile = ""
		cfg.Server.TLSKeyFile = ""
		result := cfg.Validate()
		assert.True(t, result.HasErrors())
		assert.Contains(t, result.Error(), "tls_cert_file")
		assert.Contains(t, result.Error(), "tls_key_file")
	})

	t.Run("TLS auto mode valid", func(t *testing.T) {
		cfg := validConfig()
		cfg.Server.TLSMode = "auto"
		result := cfg.Validate()
		assert.False(t, result.HasErrors())
	})

	t.Run("max_idle greater than max_open warns", func(t *testing.T) {
		cfg := validConfig()
		cfg.Database.Pool.MaxOpen = 10
		cfg.Database.Pool.MaxIdle = 20
		result := cfg.Validate()
		assert.False(t, result.HasErrors())
		assert.Len(t, result.Warnings, 1)
		assert.Contains(t, result.Warnings[0].Message, "max_idle")
	})

	t.Run("db role enabled requires OIDC", func(t *testing.T) {
		cfg := validConfig()
		cfg.Server.Auth.DBRoleEnabled = true
		cfg.Server.Auth.OIDCEnabled = false
		cfg.Server.Auth.DBRoleIntrospectionRole = "app_introspect"
		result := cfg.Validate()
		assert.True(t, result.HasErrors())
		assert.Contains(t, result.Error(), "db_role_enabled")
	})

	t.Run("db role enabled requires introspection role", func(t *testing.T) {
		cfg := validConfig()
		cfg.Server.Auth.DBRoleEnabled = true
		cfg.Server.Auth.OIDCEnabled = true
		cfg.Server.Auth.DBRoleIntrospectionRole = ""
		cfg.Server.Auth.OIDCIssuerURL = "https://issuer.test"
		cfg.Server.Auth.OIDCAudience = "aud"
		result := cfg.Validate()
		assert.True(t, result.HasErrors())
		assert.Contains(t, result.Error(), "db_role_introspection_role")
	})

	t.Run("db role enabled requires positive role schema max", func(t *testing.T) {
		cfg := validConfig()
		cfg.Server.Auth.DBRoleEnabled = true
		cfg.Server.Auth.OIDCEnabled = true
		cfg.Server.Auth.OIDCIssuerURL = "https://issuer.test"
		cfg.Server.Auth.OIDCAudience = "aud"
		cfg.Server.Auth.DBRoleIntrospectionRole = "app_introspect"
		cfg.Server.Auth.RoleSchemaMaxRoles = 0
		result := cfg.Validate()
		assert.True(t, result.HasErrors())
		assert.Contains(t, result.Error(), "role_schema_max_roles")
	})

	t.Run("db role enabled validates role schema include globs", func(t *testing.T) {
		cfg := validConfig()
		cfg.Server.Auth.DBRoleEnabled = true
		cfg.Server.Auth.OIDCEnabled = true
		cfg.Server.Auth.OIDCIssuerURL = "https://issuer.test"
		cfg.Server.Auth.OIDCAudience = "aud"
		cfg.Server.Auth.DBRoleIntrospectionRole = "app_introspect"
		cfg.Server.Auth.RoleSchemaMaxRoles = 64
		cfg.Server.Auth.RoleSchemaInclude = []string{"[bad"}
		result := cfg.Validate()
		assert.True(t, result.HasErrors())
		assert.Contains(t, result.Error(), "role_schema_include")
	})

	t.Run("db role enabled validates role schema exclude globs", func(t *testing.T) {
		cfg := validConfig()
		cfg.Server.Auth.DBRoleEnabled = true
		cfg.Server.Auth.OIDCEnabled = true
		cfg.Server.Auth.OIDCIssuerURL = "https://issuer.test"
		cfg.Server.Auth.OIDCAudience = "aud"
		cfg.Server.Auth.DBRoleIntrospectionRole = "app_introspect"
		cfg.Server.Auth.RoleSchemaMaxRoles = 64
		cfg.Server.Auth.RoleSchemaExclude = []string{"[bad"}
		result := cfg.Validate()
		assert.True(t, result.HasErrors())
		assert.Contains(t, result.Error(), "role_schema_exclude")
	})

	t.Run("OIDC enabled requires issuer and audience", func(t *testing.T) {
		cfg := validConfig()
		cfg.Server.Auth.OIDCEnabled = true
		cfg.Server.Auth.OIDCIssuerURL = ""
		cfg.Server.Auth.OIDCAudience = ""
		result := cfg.Validate()
		assert.True(t, result.HasErrors())
		assert.Contains(t, result.Error(), "oidc_issuer_url")
		assert.Contains(t, result.Error(), "oidc_audience")
	})

	t.Run("negative GraphQL limits invalid", func(t *testing.T) {
		cfg := validConfig()
		cfg.Server.GraphQLMaxDepth = -1
		cfg.Server.GraphQLMaxComplexity = -1
		cfg.Server.GraphQLMaxRows = -1
		cfg.Server.GraphQLDefaultLimit = -1
		result := cfg.Validate()
		assert.True(t, result.HasErrors())
		assert.Contains(t, result.Error(), "graphql_max_depth")
		assert.Contains(t, result.Error(), "graphql_max_complexity")
		assert.Contains(t, result.Error(), "graphql_max_rows")
		assert.Contains(t, result.Error(), "graphql_default_limit")
	})

	t.Run("multiple errors collected", func(t *testing.T) {
		cfg := validConfig()
		cfg.Database.Port = 0
		cfg.Server.Port = 0
		cfg.Observability.Logging.Level = "invalid"
		result := cfg.Validate()
		assert.True(t, result.HasErrors())
		assert.Len(t, result.Errors, 3)
	})
}

func TestValidationError_Error(t *testing.T) {
	t.Run("with hint", func(t *testing.T) {
		err := ValidationError{
			Field:   "test.field",
			Message: "test message",
			Hint:    "try this",
		}
		assert.Equal(t, "test.field: test message (hint: try this)", err.Error())
	})

	t.Run("without hint", func(t *testing.T) {
		err := ValidationError{
			Field:   "test.field",
			Message: "test message",
		}
		assert.Equal(t, "test.field: test message", err.Error())
	})
}

func TestDatabaseConfig_EffectiveDatabaseName(t *testing.T) {
	tests := []struct {
		name          string
		config        DatabaseConfig
		expectedName  string
		expectedSrc   string
		expectedError string
	}{
		{
			name: "discrete database only",
			config: DatabaseConfig{
				Database: "appdb",
			},
			expectedName: "appdb",
			expectedSrc:  "database.database",
		},
		{
			name: "dsn database only",
			config: DatabaseConfig{
				ConnectionString: "root:pass@tcp(localhost:4000)/dsn_db?parseTime=true",
			},
			expectedName: "dsn_db",
			expectedSrc:  "dsn",
		},
		{
			name: "dsn and discrete match",
			config: DatabaseConfig{
				ConnectionString: "root:pass@tcp(localhost:4000)/same_db?parseTime=true",
				Database:         "same_db",
			},
			expectedName: "same_db",
			expectedSrc:  "database.database",
		},
		{
			name: "dsn and discrete mismatch",
			config: DatabaseConfig{
				ConnectionString: "root:pass@tcp(localhost:4000)/dsn_db?parseTime=true",
				Database:         "other_db",
			},
			expectedError: "database mismatch",
		},
		{
			name: "dsn without database falls back to discrete",
			config: DatabaseConfig{
				ConnectionString: "root:pass@tcp(localhost:4000)/?parseTime=true",
				Database:         "fallback_db",
			},
			expectedName: "fallback_db",
			expectedSrc:  "database.database",
		},
		{
			name: "dsn without database and no discrete database is invalid",
			config: DatabaseConfig{
				ConnectionString: "root:pass@tcp(localhost:4000)/?parseTime=true",
			},
			expectedError: "no effective database name configured",
		},
		{
			name: "invalid dsn is invalid",
			config: DatabaseConfig{
				ConnectionString: "not-a-valid-dsn",
			},
			expectedError: "database.dsn is invalid",
		},
		{
			name:          "empty everything is invalid",
			config:        DatabaseConfig{},
			expectedError: "no effective database name configured",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, source, err := tt.config.EffectiveDatabaseName()
			if tt.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedName, name)
			assert.Equal(t, tt.expectedSrc, source)
		})
	}
}

func TestConfigValidate_DatabaseResolution(t *testing.T) {
	t.Run("dsn with matching database passes", func(t *testing.T) {
		cfg := &Config{
			Database: DatabaseConfig{
				ConnectionString: "root:pass@tcp(localhost:4000)/match_db?parseTime=true",
				Database:         "match_db",
				Port:             4000,
				Pool: PoolConfig{
					MaxOpen: 1,
					MaxIdle: 1,
				},
			},
			Server: ServerConfig{Port: 8080},
			Observability: ObservabilityConfig{
				Logging: LoggingConfig{Level: "info", Format: "json"},
				OTLP:    OTLPConfig{Protocol: "grpc", Compression: "gzip"},
			},
		}

		result := cfg.Validate()
		assert.False(t, result.HasErrors())
		assert.Equal(t, "match_db", cfg.Database.Database)
	})

	t.Run("dsn mismatch with database errors", func(t *testing.T) {
		cfg := &Config{
			Database: DatabaseConfig{
				ConnectionString: "root:pass@tcp(localhost:4000)/dsn_db?parseTime=true",
				Database:         "other_db",
				Port:             4000,
				Pool: PoolConfig{
					MaxOpen: 1,
					MaxIdle: 1,
				},
			},
			Server: ServerConfig{Port: 8080},
			Observability: ObservabilityConfig{
				Logging: LoggingConfig{Level: "info", Format: "json"},
				OTLP:    OTLPConfig{Protocol: "grpc", Compression: "gzip"},
			},
		}

		result := cfg.Validate()
		assert.True(t, result.HasErrors())
		assert.Contains(t, result.Error(), "database mismatch")
	})

	t.Run("dsn without database and no database field errors", func(t *testing.T) {
		cfg := &Config{
			Database: DatabaseConfig{
				ConnectionString: "root:pass@tcp(localhost:4000)/?parseTime=true",
				Database:         "",
				Port:             4000,
				Pool: PoolConfig{
					MaxOpen: 1,
					MaxIdle: 1,
				},
			},
			Server: ServerConfig{Port: 8080},
			Observability: ObservabilityConfig{
				Logging: LoggingConfig{Level: "info", Format: "json"},
				OTLP:    OTLPConfig{Protocol: "grpc", Compression: "gzip"},
			},
		}

		result := cfg.Validate()
		assert.True(t, result.HasErrors())
		assert.Contains(t, result.Error(), "no effective database name configured")
	})
}

func TestParseMyCnf(t *testing.T) {
	t.Run("parses supported client keys", func(t *testing.T) {
		raw := `
[client]
host = gateway.tidbcloud.com
port = 4000
user = app_user
password = "super-secret"
database = app_db
ssl-mode = VERIFY_IDENTITY
`
		settings, err := parseMyCnf(raw)
		assert.NoError(t, err)
		assert.Equal(t, "gateway.tidbcloud.com", settings.Host)
		assert.True(t, settings.HasPort)
		assert.Equal(t, 4000, settings.Port)
		assert.Equal(t, "app_user", settings.User)
		assert.Equal(t, "super-secret", settings.Password)
		assert.True(t, settings.HasDBName)
		assert.Equal(t, "app_db", settings.Database)
		assert.Equal(t, "verify-full", settings.TLSMode)
	})

	t.Run("mysql database fallback is used", func(t *testing.T) {
		raw := `
[client]
host = localhost
[mysql]
database = fallback_db
`
		settings, err := parseMyCnf(raw)
		assert.NoError(t, err)
		assert.True(t, settings.HasDBName)
		assert.Equal(t, "fallback_db", settings.Database)
	})
}

func TestConfigValidate_MyCnfResolution(t *testing.T) {
	newMyCnf := func(t *testing.T, content string) string {
		t.Helper()
		dir := t.TempDir()
		path := filepath.Join(dir, "test.my.cnf")
		err := os.WriteFile(path, []byte(content), 0o600)
		assert.NoError(t, err)
		return path
	}

	validBase := func() *Config {
		return &Config{
			Server: ServerConfig{Port: 8080},
			Observability: ObservabilityConfig{
				Logging: LoggingConfig{Level: "info", Format: "json"},
				OTLP:    OTLPConfig{Protocol: "grpc", Compression: "gzip"},
			},
			Database: DatabaseConfig{
				Pool: PoolConfig{MaxOpen: 1, MaxIdle: 1},
			},
		}
	}

	t.Run("mycnf only config passes and resolves source", func(t *testing.T) {
		cfg := validBase()
		cfg.Database.MyCnfFile = newMyCnf(t, `
[client]
host=localhost
port=4000
user=root
password=pass
database=mycnf_db
ssl-mode=REQUIRED
`)

		result := cfg.Validate()
		assert.False(t, result.HasErrors())
		assert.Equal(t, "mycnf_db", cfg.Database.Database)
		assert.Equal(t, "localhost", cfg.Database.Host)
		assert.Equal(t, 4000, cfg.Database.Port)
		assert.Equal(t, "skip-verify", cfg.Database.TLS.Mode)

		name, source, err := cfg.Database.EffectiveDatabaseName()
		assert.NoError(t, err)
		assert.Equal(t, "mycnf_db", name)
		assert.Equal(t, "mycnf", source)
	})

	t.Run("mycnf mismatched database errors", func(t *testing.T) {
		cfg := validBase()
		cfg.Database.MyCnfFile = newMyCnf(t, `
[client]
host=localhost
port=4000
user=root
password=pass
database=mycnf_db
`)
		cfg.Database.Database = "other_db"

		result := cfg.Validate()
		assert.True(t, result.HasErrors())
		assert.Contains(t, result.Error(), "database mismatch")
		assert.Contains(t, result.Error(), "database.mycnf_file")
	})

	t.Run("mycnf and dsn together errors", func(t *testing.T) {
		cfg := validBase()
		cfg.Database.MyCnfFile = newMyCnf(t, `
[client]
host=localhost
port=4000
user=root
password=pass
database=mycnf_db
`)
		cfg.Database.ConnectionString = "root:pass@tcp(localhost:4000)/dsn_db?parseTime=true"

		result := cfg.Validate()
		assert.True(t, result.HasErrors())
		assert.Contains(t, result.Error(), "mutually exclusive")
	})

	t.Run("mycnf without database and no database field errors", func(t *testing.T) {
		cfg := validBase()
		cfg.Database.MyCnfFile = newMyCnf(t, `
[client]
host=localhost
port=4000
user=root
password=pass
`)
		cfg.Database.Database = ""

		result := cfg.Validate()
		assert.True(t, result.HasErrors())
		assert.Contains(t, result.Error(), "database.mycnf_file does not provide a database name")
	})
}
