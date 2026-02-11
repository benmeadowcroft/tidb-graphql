//go:build integration
// +build integration

package integration

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/testutil/tidbcloud"
)

func TestIntrospectDatabase_SimpleSchema(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Setup test database
	testDB := tidbcloud.NewTestDB(t)

	// Load schema
	testDB.LoadSchema(t, "../fixtures/simple_schema.sql")

	// Test introspection
	schema, err := introspection.IntrospectDatabase(testDB.DB, testDB.DatabaseName)
	require.NoError(t, err, "IntrospectDatabase should not return an error")
	require.NotNil(t, schema, "Schema should not be nil")

	// Verify tables
	assert.Len(t, schema.Tables, 3, "Should have 3 tables (users, posts, comments)")

	// Build table name index for easier testing
	tablesByName := make(map[string]introspection.Table)
	for _, table := range schema.Tables {
		tablesByName[table.Name] = table
	}

	// Verify users table
	usersTable, exists := tablesByName["users"]
	require.True(t, exists, "users table should exist")
	assert.Len(t, usersTable.Columns, 4, "users table should have 4 columns")

	// Find specific columns
	// Find specific columns
	var idCol, usernameCol *introspection.Column
	for i := range usersTable.Columns {
		col := &usersTable.Columns[i]
		switch col.Name {
		case "id":
			idCol = col
		case "username":
			usernameCol = col
		}
	}

	// Verify id column
	require.NotNil(t, idCol, "id column should exist")
	assert.True(t, idCol.IsPrimaryKey, "id should be primary key")
	assert.False(t, idCol.IsNullable, "id should not be nullable")
	assert.Contains(t, idCol.DataType, "int", "id should be an integer type")

	// Verify username column
	require.NotNil(t, usernameCol, "username column should exist")
	assert.False(t, usernameCol.IsNullable, "username should not be nullable")
	assert.Contains(t, usernameCol.DataType, "varchar", "username should be varchar")

	// Verify posts table
	postsTable, exists := tablesByName["posts"]
	require.True(t, exists, "posts table should exist")
	assert.Len(t, postsTable.Columns, 6, "posts table should have 6 columns")

	// Verify foreign key column exists
	var userIdCol *introspection.Column
	for i := range postsTable.Columns {
		if postsTable.Columns[i].Name == "user_id" {
			userIdCol = &postsTable.Columns[i]
			break
		}
	}
	require.NotNil(t, userIdCol, "posts.user_id column should exist")
	assert.False(t, userIdCol.IsNullable, "user_id should not be nullable")

	// Verify comments table
	commentsTable, exists := tablesByName["comments"]
	require.True(t, exists, "comments table should exist")
	assert.Len(t, commentsTable.Columns, 5, "comments table should have 5 columns")
}

func TestIntrospectDatabase_WithData(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Setup test database
	testDB := tidbcloud.NewTestDB(t)

	// Load schema and seed data
	testDB.LoadSchema(t, "../fixtures/simple_schema.sql")
	testDB.LoadFixtures(t, "../fixtures/simple_seed.sql")

	// Introspection should work the same whether data exists or not
	schema, err := introspection.IntrospectDatabase(testDB.DB, testDB.DatabaseName)
	require.NoError(t, err)
	require.NotNil(t, schema)

	// Verify we still get the correct table structure
	assert.Len(t, schema.Tables, 3, "Should have 3 tables")

	// Verify we can query the data (validates schema is correct)
	var userCount int
	err = testDB.DB.QueryRow("SELECT COUNT(*) FROM users").Scan(&userCount)
	require.NoError(t, err)
	assert.Equal(t, 3, userCount, "Should have 3 users from seed data")

	var postCount int
	err = testDB.DB.QueryRow("SELECT COUNT(*) FROM posts").Scan(&postCount)
	require.NoError(t, err)
	assert.Equal(t, 4, postCount, "Should have 4 posts from seed data")
}

func TestIntrospectDatabase_EmptyDatabase(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Setup test database without loading any schema
	testDB := tidbcloud.NewTestDB(t)

	// Test introspection on empty database
	schema, err := introspection.IntrospectDatabase(testDB.DB, testDB.DatabaseName)
	require.NoError(t, err)
	require.NotNil(t, schema)

	// Should return empty schema with no tables
	assert.Empty(t, schema.Tables, "Empty database should have no tables")
}

func TestIntrospectDatabase_AutoRandomColumn(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)

	_, err := testDB.DB.Exec("CREATE TABLE auto_random_check (id BIGINT PRIMARY KEY /*T![auto_rand] AUTO_RANDOM(5) */, name VARCHAR(20))")
	require.NoError(t, err, "failed to create auto_random_check table")

	schema, err := introspection.IntrospectDatabase(testDB.DB, testDB.DatabaseName)
	require.NoError(t, err, "IntrospectDatabase should not return an error")

	var autoTable *introspection.Table
	for i := range schema.Tables {
		if schema.Tables[i].Name == "auto_random_check" {
			autoTable = &schema.Tables[i]
			break
		}
	}
	require.NotNil(t, autoTable, "auto_random_check table should exist")

	var idCol *introspection.Column
	for i := range autoTable.Columns {
		if autoTable.Columns[i].Name == "id" {
			idCol = &autoTable.Columns[i]
			break
		}
	}
	require.NotNil(t, idCol, "id column should exist")
	assert.True(t, idCol.IsAutoRandom, "id column should be detected as auto_random")
	assert.False(t, idCol.IsAutoIncrement, "id column should not be auto_increment")
}

func TestIntrospectDatabase_TypeMapping(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Setup test database
	testDB := tidbcloud.NewTestDB(t)

	// Create a table with various data types
	_, err := testDB.DB.Exec(`
		CREATE TABLE type_test (
			id INT PRIMARY KEY AUTO_INCREMENT,
			int_col INT NOT NULL,
			bigint_col BIGINT,
			varchar_col VARCHAR(255) NOT NULL,
			text_col TEXT,
			bool_col BOOLEAN,
			float_col FLOAT,
			double_col DOUBLE,
			decimal_col DECIMAL(10,2),
			date_col DATE,
			datetime_col DATETIME,
			timestamp_col TIMESTAMP,
			json_col JSON
		)
	`)
	require.NoError(t, err, "Failed to create type_test table")

	// Test introspection
	dbSchema, err := introspection.IntrospectDatabase(testDB.DB, testDB.DatabaseName)
	require.NoError(t, err)
	require.Len(t, dbSchema.Tables, 1, "Should have 1 table")

	table := dbSchema.Tables[0]
	assert.Equal(t, "type_test", table.Name)

	// Build column index
	columns := make(map[string]introspection.Column)
	for _, col := range table.Columns {
		columns[col.Name] = col
	}

	// Verify integer types
	intCol, exists := columns["int_col"]
	require.True(t, exists)
	assert.Contains(t, intCol.DataType, "int")
	assert.False(t, intCol.IsNullable)

	bigintCol, exists := columns["bigint_col"]
	require.True(t, exists)
	assert.Contains(t, bigintCol.DataType, "bigint")

	// Verify string types
	varcharCol, exists := columns["varchar_col"]
	require.True(t, exists)
	assert.Contains(t, varcharCol.DataType, "varchar")

	textCol, exists := columns["text_col"]
	require.True(t, exists)
	assert.Contains(t, textCol.DataType, "text")

	// Verify boolean type
	boolCol, exists := columns["bool_col"]
	require.True(t, exists)
	assert.Contains(t, boolCol.DataType, "tinyint") // MySQL represents BOOLEAN as TINYINT(1)

	// Verify floating point types
	floatCol, exists := columns["float_col"]
	require.True(t, exists)
	assert.Contains(t, floatCol.DataType, "float")

	doubleCol, exists := columns["double_col"]
	require.True(t, exists)
	assert.Contains(t, doubleCol.DataType, "double")

	decimalCol, exists := columns["decimal_col"]
	require.True(t, exists)
	assert.Contains(t, decimalCol.DataType, "decimal")

	// Verify date/time types
	dateCol, exists := columns["date_col"]
	require.True(t, exists)
	assert.Contains(t, dateCol.DataType, "date")

	datetimeCol, exists := columns["datetime_col"]
	require.True(t, exists)
	assert.Contains(t, datetimeCol.DataType, "datetime")

	// Verify JSON type
	jsonCol, exists := columns["json_col"]
	require.True(t, exists)
	assert.Contains(t, jsonCol.DataType, "json")
}

func TestIntrospectDatabase_Views(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewTestDB(t)

	_, err := testDB.DB.Exec(`
		CREATE TABLE view_source (
			id INT PRIMARY KEY AUTO_INCREMENT,
			name VARCHAR(255) NOT NULL
		)
	`)
	require.NoError(t, err, "Failed to create view_source table")

	_, err = testDB.DB.Exec(`
		CREATE VIEW active_users AS
		SELECT id, name FROM view_source
	`)
	require.NoError(t, err, "Failed to create active_users view")

	schema, err := introspection.IntrospectDatabase(testDB.DB, testDB.DatabaseName)
	require.NoError(t, err)

	tables := make(map[string]introspection.Table)
	for _, table := range schema.Tables {
		tables[table.Name] = table
	}

	view, exists := tables["active_users"]
	require.True(t, exists, "active_users view should exist")
	assert.True(t, view.IsView)
	assert.Empty(t, view.Indexes)
	assert.Empty(t, view.ForeignKeys)
}
