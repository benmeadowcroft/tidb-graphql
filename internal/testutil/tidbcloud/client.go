package tidbcloud

import (
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"tidb-graphql/internal/sqlutil"
)

// TestDB represents a test database connection to TiDB Cloud Serverless
type TestDB struct {
	DB           *sql.DB
	DatabaseName string
	config       Config
}

// RoleTestDB represents admin/runtime connections for role-based integration tests.
type RoleTestDB struct {
	AdminDB      *sql.DB
	RuntimeDB    *sql.DB
	DatabaseName string
	RuntimeUser  string
	RuntimeHost  string
	config       Config
}

// Config holds TiDB Cloud connection information
type Config struct {
	Host       string
	Port       string
	User       string
	UserPrefix string
	Password   string
	TLSMode    string
}

// NewTestDB creates a new test database connection using TiDB Cloud Serverless.
// Each test gets its own isolated database that is automatically cleaned up.
func NewTestDB(t *testing.T) *TestDB {
	t.Helper()

	cfg := getTestConfig(t)

	// Create a unique database name for this test run
	dbName := fmt.Sprintf("test_%s_%d",
		sanitizeName(t.Name()),
		time.Now().UnixMilli())

	// Connect to default database first
	dsn := buildDSN(cfg, "information_schema")
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("Failed to connect to TiDB Cloud: %v", err)
	}

	// Set reasonable connection pool settings for tests
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Verify connection
	if err := db.Ping(); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			t.Logf("Warning: failed to close database connection: %v", closeErr)
		}
		t.Fatalf("Failed to ping TiDB Cloud: %v", err)
	}

	// Validate database name to ensure it's safe (only alphanumeric and underscores)
	if !isValidDatabaseName(dbName) {
		if closeErr := db.Close(); closeErr != nil {
			t.Logf("Warning: failed to close database connection: %v", closeErr)
		}
		t.Fatalf("Invalid database name generated: %s", dbName)
	}

	// Create test database - safe to use string formatting because dbName is validated
	createDBQuery := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName)
	_, err = db.Exec(createDBQuery)
	if err != nil {
		if closeErr := db.Close(); closeErr != nil {
			t.Logf("Warning: failed to close database connection: %v", closeErr)
		}
		t.Fatalf("Failed to create test database %s: %v", dbName, err)
	}

	// Close initial connection
	if closeErr := db.Close(); closeErr != nil {
		t.Logf("Warning: failed to close database connection: %v", closeErr)
	}

	// Reconnect to the test database
	dsn = buildDSN(cfg, dbName)
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("Failed to connect to test database: %v", err)
	}

	// Set connection pool settings again for test DB connection
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			t.Logf("Warning: failed to close database connection: %v", closeErr)
		}
		t.Fatalf("Failed to ping test database: %v", err)
	}

	testDB := &TestDB{
		DB:           db,
		DatabaseName: dbName,
		config:       cfg,
	}

	// Register cleanup
	t.Cleanup(func() {
		testDB.Teardown(t)
	})

	return testDB
}

// NewRoleTestDB creates a test database with a temporary runtime user for role-based tests.
func NewRoleTestDB(t *testing.T) *RoleTestDB {
	t.Helper()

	cfg := getTestConfig(t)

	// Use milliseconds instead of nanoseconds to keep name under 64 char limit
	dbName := fmt.Sprintf("test_%s_%d",
		sanitizeName(t.Name()),
		time.Now().UnixMilli())

	adminBootstrap, err := sql.Open("mysql", buildDSN(cfg, "information_schema"))
	if err != nil {
		t.Fatalf("Failed to connect to TiDB Cloud: %v", err)
	}
	configureTestPool(adminBootstrap)

	if err := adminBootstrap.Ping(); err != nil {
		_ = adminBootstrap.Close()
		t.Fatalf("Failed to ping TiDB Cloud: %v", err)
	}

	if !isValidDatabaseName(dbName) {
		_ = adminBootstrap.Close()
		t.Fatalf("Invalid database name generated: %s", dbName)
	}

	createDBQuery := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName)
	if _, err := adminBootstrap.Exec(createDBQuery); err != nil {
		_ = adminBootstrap.Close()
		t.Fatalf("Failed to create test database %s: %v", dbName, err)
	}

	if err := adminBootstrap.Close(); err != nil {
		t.Logf("Warning: failed to close database connection: %v", err)
	}

	adminDB, err := sql.Open("mysql", buildDSN(cfg, dbName))
	if err != nil {
		t.Fatalf("Failed to connect to test database: %v", err)
	}
	configureTestPool(adminDB)

	if err := adminDB.Ping(); err != nil {
		_ = adminDB.Close()
		t.Fatalf("Failed to ping test database: %v", err)
	}

	// limit to 32 bytes to fit into user name length restriction
	runtimeUserBytes := []byte(fmt.Sprintf("%stgrt_%d", cfg.UserPrefix, time.Now().UnixNano()))
	if len(runtimeUserBytes) > 32 {
		runtimeUserBytes = runtimeUserBytes[:32]
	}
	runtimeUser := string(runtimeUserBytes)
	runtimeHost := "%"
	runtimePassword, err := generatePassword(24)
	if err != nil {
		_ = adminDB.Close()
		t.Fatalf("Failed to generate runtime password: %v", err)
	}

	runtimeIdentity := quoteUserHost(runtimeUser, runtimeHost)
	// CREATE USER doesn't support parameterized IDENTIFIED BY in MySQL/TiDB,
	// so we use QuoteString to safely escape the password.
	if _, err := adminDB.Exec(fmt.Sprintf("CREATE USER %s IDENTIFIED BY %s", runtimeIdentity, sqlutil.QuoteString(runtimePassword))); err != nil {
		_ = adminDB.Close()
		t.Fatalf("Failed to create runtime user %s: %v", runtimeUser, err)
	}
	if _, err := adminDB.Exec(fmt.Sprintf("GRANT SELECT ON INFORMATION_SCHEMA.* TO %s", runtimeIdentity)); err != nil {
		_ = adminDB.Close()
		t.Fatalf("Failed to grant information_schema access to %s: %v", runtimeUser, err)
	}

	if _, err := adminDB.Exec(fmt.Sprintf("GRANT USAGE ON `%s`.* TO %s", dbName, runtimeIdentity)); err != nil {
		_ = adminDB.Close()
		t.Fatalf("Failed to grant usage access to %s for %s: %v", dbName, runtimeUser, err)
	}

	runtimeCfg := cfg
	runtimeCfg.User = runtimeUser
	runtimeCfg.Password = runtimePassword

	runtimeDB, err := sql.Open("mysql", buildDSN(runtimeCfg, "information_schema"))
	if err != nil {
		_ = adminDB.Close()
		t.Fatalf("Failed to connect to runtime database: %v", err)
	}
	configureTestPool(runtimeDB)

	if err := runtimeDB.Ping(); err != nil {
		_ = runtimeDB.Close()
		_ = adminDB.Close()
		t.Fatalf("Failed to ping runtime database: %v", err)
	}

	testDB := &RoleTestDB{
		AdminDB:      adminDB,
		RuntimeDB:    runtimeDB,
		DatabaseName: dbName,
		RuntimeUser:  runtimeUser,
		RuntimeHost:  runtimeHost,
		config:       cfg,
	}

	t.Cleanup(func() {
		testDB.Teardown(t)
	})

	return testDB
}

// Teardown cleans up the test database
func (tdb *TestDB) Teardown(t *testing.T) {
	t.Helper()

	if tdb.DB != nil {
		// Validate database name before dropping (extra safety)
		if isValidDatabaseName(tdb.DatabaseName) {
			// Drop the test database - safe because name is validated
			dropDBQuery := fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", tdb.DatabaseName)
			_, err := tdb.DB.Exec(dropDBQuery)
			if err != nil {
				t.Logf("Warning: Failed to drop test database %s: %v", tdb.DatabaseName, err)
			}
		}

		if err := tdb.DB.Close(); err != nil {
			t.Logf("Warning: failed to close test database connection: %v", err)
		}
	}
}

// Teardown cleans up the role-based test database and runtime user.
func (tdb *RoleTestDB) Teardown(t *testing.T) {
	t.Helper()

	if tdb.RuntimeDB != nil {
		if err := tdb.RuntimeDB.Close(); err != nil {
			t.Logf("Warning: failed to close runtime database connection: %v", err)
		}
	}

	if tdb.AdminDB != nil {
		if tdb.RuntimeUser != "" {
			runtimeIdentity := quoteUserHost(tdb.RuntimeUser, tdb.RuntimeHost)
			if _, err := tdb.AdminDB.Exec(fmt.Sprintf("DROP USER IF EXISTS %s", runtimeIdentity)); err != nil {
				t.Logf("Warning: failed to drop runtime user %s: %v", tdb.RuntimeUser, err)
			}
		}

		if isValidDatabaseName(tdb.DatabaseName) {
			dropDBQuery := fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", tdb.DatabaseName)
			if _, err := tdb.AdminDB.Exec(dropDBQuery); err != nil {
				t.Logf("Warning: Failed to drop test database %s: %v", tdb.DatabaseName, err)
			}
		}

		if err := tdb.AdminDB.Close(); err != nil {
			t.Logf("Warning: failed to close admin database connection: %v", err)
		}
	}
}

// LoadSchemaAdmin loads a SQL schema file using the admin connection.
func (tdb *RoleTestDB) LoadSchemaAdmin(t *testing.T, schemaPath string) {
	t.Helper()
	loadSQLFile(t, tdb.AdminDB, schemaPath)
}

// LoadFixturesAdmin loads fixture data using the admin connection.
func (tdb *RoleTestDB) LoadFixturesAdmin(t *testing.T, fixturePath string) {
	t.Helper()
	loadSQLFile(t, tdb.AdminDB, fixturePath)
}

// LoadSchema loads a SQL schema file into the test database.
// The file can contain multiple statements separated by semicolons.
func (tdb *TestDB) LoadSchema(t *testing.T, schemaPath string) {
	t.Helper()
	loadSQLFile(t, tdb.DB, schemaPath)
}

// LoadFixtures loads test data from a SQL file.
// The file can contain INSERT, UPDATE, or other data manipulation statements.
func (tdb *TestDB) LoadFixtures(t *testing.T, fixturePath string) {
	t.Helper()
	loadSQLFile(t, tdb.DB, fixturePath)
}

// getTestConfig reads TiDB Cloud connection info from environment variables
func getTestConfig(t *testing.T) Config {
	t.Helper()

	host := os.Getenv("TIDB_HOST")
	port := os.Getenv("TIDB_PORT")
	user := os.Getenv("TIDB_USER")
	userPrefix := os.Getenv("TIDB_USER_PREFIX")
	tlsMode := os.Getenv("TIDB_TLS_MODE")
	if userPrefix != "" && !strings.HasPrefix(user, userPrefix) {
		user = userPrefix + user
	}
	password := os.Getenv("TIDB_PASSWORD")

	if host == "" || user == "" || password == "" {
		t.Skip("TiDB credentials not set. Set TIDB_HOST, TIDB_USER, TIDB_PASSWORD environment variables to run integration tests")
	}

	if port == "" {
		port = "4000"
	}
	if tlsMode == "" {
		tlsMode = "true"
	}

	return Config{
		Host:       host,
		Port:       port,
		User:       user,
		UserPrefix: userPrefix,
		Password:   password,
		TLSMode:    tlsMode,
	}
}

// buildDSN constructs a TiDB Cloud connection string
func buildDSN(cfg Config, database string) string {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		cfg.User,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		database,
	)

	if cfg.TLSMode != "" {
		dsn += fmt.Sprintf("&tls=%s", cfg.TLSMode)
	}

	return dsn
}

func configureTestPool(db *sql.DB) {
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(5 * time.Minute)
}

func loadSQLFile(t *testing.T, db *sql.DB, filePath string) {
	t.Helper()

	payload, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read SQL file %s: %v", filePath, err)
	}

	statements := splitSQL(string(payload))
	for i, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("Failed to execute SQL statement %d: %v\nStatement: %s", i+1, err, stmt)
		}
	}
}

func generatePassword(length int) (string, error) {
	if length < 12 {
		length = 12
	}
	buf := make([]byte, length)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func quoteUserHost(user, host string) string {
	escapedUser := strings.ReplaceAll(user, "'", "''")
	escapedHost := strings.ReplaceAll(host, "'", "''")
	return fmt.Sprintf("'%s'@'%s'", escapedUser, escapedHost)
}

// sanitizeName makes a test name safe for use as a database name.
// MySQL/TiDB database names are limited to 64 characters and can only
// contain alphanumeric characters and underscores.
func sanitizeName(name string) string {
	var result strings.Builder

	for _, ch := range name {
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') {
			result.WriteRune(ch)
		} else {
			result.WriteRune('_')
		}
	}

	sanitized := result.String()

	// Limit length (MySQL/TiDB database names max 64 chars, but we need room for timestamp)
	if len(sanitized) > 40 {
		sanitized = sanitized[:40]
	}

	return sanitized
}

// splitSQL splits SQL text into individual statements.
// This is a simple implementation that splits on semicolons.
// It doesn't handle semicolons inside strings or comments.
func splitSQL(sql string) []string {
	statements := strings.Split(sql, ";")
	result := make([]string, 0, len(statements))

	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt != "" {
			result = append(result, stmt)
		}
	}

	return result
}

// isValidDatabaseName validates that a database name only contains safe characters.
// MySQL/TiDB database names should only contain alphanumeric characters and underscores.
// This prevents SQL injection in CREATE/DROP DATABASE statements.
func isValidDatabaseName(name string) bool {
	if name == "" || len(name) > 64 {
		return false
	}

	for _, ch := range name {
		if !isValidDatabaseChar(ch) {
			return false
		}
	}

	return true
}

func isValidDatabaseChar(ch rune) bool {
	return (ch >= 'a' && ch <= 'z') ||
		(ch >= 'A' && ch <= 'Z') ||
		(ch >= '0' && ch <= '9') ||
		ch == '_'
}
