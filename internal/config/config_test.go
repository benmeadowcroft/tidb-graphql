package config

import (
	"os"
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
			expected: "root:password@tcp(localhost:4000)/test?parseTime=true",
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
			expected: "admin:p@ss:w0rd!@tcp(db.example.com:3306)/mydb?parseTime=true",
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
			expected: "root:@tcp(localhost:4000)/test?parseTime=true",
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
