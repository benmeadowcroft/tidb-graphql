package sqltype

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMapToGraphQL_IntegerTypes(t *testing.T) {
	intTypes := []string{
		"TINYINT", "tinyint",
		"SMALLINT", "smallint",
		"MEDIUMINT", "mediumint",
		"INT", "int",
		"INTEGER", "integer",
		"BIGINT", "bigint",
		"SERIAL", "serial",
		"BIT", "bit",
	}

	for _, sqlType := range intTypes {
		t.Run(sqlType, func(t *testing.T) {
			assert.Equal(t, TypeInt, MapToGraphQL(sqlType))
			assert.Equal(t, "Int", MapToGraphQL(sqlType).String())
			assert.Equal(t, "IntFilter", MapToGraphQL(sqlType).FilterTypeName())
		})
	}
}

func TestMapToGraphQL_FloatTypes(t *testing.T) {
	floatTypes := []string{
		"FLOAT", "float",
		"DOUBLE", "double",
		"DECIMAL", "decimal",
		"NUMERIC", "numeric",
	}

	for _, sqlType := range floatTypes {
		t.Run(sqlType, func(t *testing.T) {
			assert.Equal(t, TypeFloat, MapToGraphQL(sqlType))
			assert.Equal(t, "Float", MapToGraphQL(sqlType).String())
			assert.Equal(t, "FloatFilter", MapToGraphQL(sqlType).FilterTypeName())
		})
	}
}

func TestMapToGraphQL_BooleanTypes(t *testing.T) {
	boolTypes := []string{
		"BOOL", "bool",
		"BOOLEAN", "boolean",
	}

	for _, sqlType := range boolTypes {
		t.Run(sqlType, func(t *testing.T) {
			assert.Equal(t, TypeBoolean, MapToGraphQL(sqlType))
			assert.Equal(t, "Boolean", MapToGraphQL(sqlType).String())
			assert.Equal(t, "BooleanFilter", MapToGraphQL(sqlType).FilterTypeName())
		})
	}
}

func TestMapToGraphQL_StringTypes(t *testing.T) {
	stringTypes := []string{
		"CHAR", "char",
		"VARCHAR", "varchar",
		"TINYTEXT", "tinytext",
		"TEXT", "text",
		"MEDIUMTEXT", "mediumtext",
		"LONGTEXT", "longtext",
		"BLOB", "blob",
		"TINYBLOB", "tinyblob",
		"MEDIUMBLOB", "mediumblob",
		"LONGBLOB", "longblob",
		"BINARY", "binary",
		"VARBINARY", "varbinary",
		"ENUM", "enum",
		"SET", "set",
		"DATE", "date",
		"DATETIME", "datetime",
		"TIMESTAMP", "timestamp",
		"TIME", "time",
		"YEAR", "year",
	}

	for _, sqlType := range stringTypes {
		t.Run(sqlType, func(t *testing.T) {
			assert.Equal(t, TypeString, MapToGraphQL(sqlType))
			assert.Equal(t, "String", MapToGraphQL(sqlType).String())
			assert.Equal(t, "StringFilter", MapToGraphQL(sqlType).FilterTypeName())
		})
	}
}

func TestMapToGraphQL_JSONType(t *testing.T) {
	jsonTypes := []string{"JSON", "json"}

	for _, sqlType := range jsonTypes {
		t.Run(sqlType, func(t *testing.T) {
			assert.Equal(t, TypeJSON, MapToGraphQL(sqlType))
			assert.Equal(t, "JSON", MapToGraphQL(sqlType).String())
			// JSON uses StringFilter (JSON columns are typically skipped in WHERE)
			assert.Equal(t, "StringFilter", MapToGraphQL(sqlType).FilterTypeName())
		})
	}
}

func TestMapToGraphQL_UnknownTypesDefaultToString(t *testing.T) {
	unknownTypes := []string{
		"GEOMETRY",
		"POINT",
		"LINESTRING",
		"POLYGON",
		"UNKNOWN_TYPE",
		"",
	}

	for _, sqlType := range unknownTypes {
		t.Run(sqlType, func(t *testing.T) {
			assert.Equal(t, TypeString, MapToGraphQL(sqlType))
			assert.Equal(t, "String", MapToGraphQL(sqlType).String())
			assert.Equal(t, "StringFilter", MapToGraphQL(sqlType).FilterTypeName())
		})
	}
}

func TestMapToGraphQL_NoFalsePositives(t *testing.T) {
	// These types previously matched incorrectly with strings.Contains
	testCases := []struct {
		sqlType  string
		expected GraphQLType
	}{
		// "POINT" should NOT match "int"
		{"POINT", TypeString},
		// "MULTIPOINT" should NOT match "int"
		{"MULTIPOINT", TypeString},
		// "TINYINT" SHOULD match int
		{"TINYINT", TypeInt},
	}

	for _, tc := range testCases {
		t.Run(tc.sqlType, func(t *testing.T) {
			assert.Equal(t, tc.expected, MapToGraphQL(tc.sqlType))
		})
	}
}

func TestMapToGraphQL_WithSizeSpecifiers(t *testing.T) {
	testCases := []struct {
		sqlType      string
		expected     GraphQLType
		expectedName string
	}{
		{"varchar(255)", TypeString, "String"},
		{"VARCHAR(100)", TypeString, "String"},
		{"char(10)", TypeString, "String"},
		{"decimal(10,2)", TypeFloat, "Float"},
		{"DECIMAL(18,4)", TypeFloat, "Float"},
		{"int(11)", TypeInt, "Int"},
		{"INT(10)", TypeInt, "Int"},
		{"bigint(20)", TypeInt, "Int"},
		{"tinyint(1)", TypeInt, "Int"},
		{"enum('a','b','c')", TypeString, "String"},
	}

	for _, tc := range testCases {
		t.Run(tc.sqlType, func(t *testing.T) {
			assert.Equal(t, tc.expected, MapToGraphQL(tc.sqlType))
			assert.Equal(t, tc.expectedName, MapToGraphQL(tc.sqlType).String())
		})
	}
}

func TestIsNumeric(t *testing.T) {
	testCases := []struct {
		graphQLType GraphQLType
		expected    bool
	}{
		{TypeInt, true},
		{TypeFloat, true},
		{TypeString, false},
		{TypeBoolean, false},
		{TypeJSON, false},
	}

	for _, tc := range testCases {
		t.Run(tc.graphQLType.String(), func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.graphQLType.IsNumeric())
		})
	}
}

func TestIsComparable(t *testing.T) {
	testCases := []struct {
		graphQLType GraphQLType
		expected    bool
	}{
		{TypeInt, true},
		{TypeFloat, true},
		{TypeString, true},
		{TypeBoolean, true},
		{TypeJSON, false}, // JSON is not comparable
	}

	for _, tc := range testCases {
		t.Run(tc.graphQLType.String(), func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.graphQLType.IsComparable())
		})
	}
}
