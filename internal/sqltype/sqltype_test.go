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

func TestMapToGraphQL_BigIntTypes(t *testing.T) {
	bigIntTypes := []string{
		"BIGINT", "bigint",
	}

	for _, sqlType := range bigIntTypes {
		t.Run(sqlType, func(t *testing.T) {
			assert.Equal(t, TypeBigInt, MapToGraphQL(sqlType))
			assert.Equal(t, "BigInt", MapToGraphQL(sqlType).String())
			assert.Equal(t, "BigIntFilter", MapToGraphQL(sqlType).FilterTypeName())
		})
	}
}

func TestMapToGraphQL_FloatTypes(t *testing.T) {
	floatTypes := []string{
		"FLOAT", "float",
		"DOUBLE", "double",
	}

	for _, sqlType := range floatTypes {
		t.Run(sqlType, func(t *testing.T) {
			assert.Equal(t, TypeFloat, MapToGraphQL(sqlType))
			assert.Equal(t, "Float", MapToGraphQL(sqlType).String())
			assert.Equal(t, "FloatFilter", MapToGraphQL(sqlType).FilterTypeName())
		})
	}
}

func TestMapToGraphQL_DecimalTypes(t *testing.T) {
	decimalTypes := []string{
		"DECIMAL", "decimal",
		"NUMERIC", "numeric",
	}

	for _, sqlType := range decimalTypes {
		t.Run(sqlType, func(t *testing.T) {
			assert.Equal(t, TypeDecimal, MapToGraphQL(sqlType))
			assert.Equal(t, "Decimal", MapToGraphQL(sqlType).String())
			assert.Equal(t, "DecimalFilter", MapToGraphQL(sqlType).FilterTypeName())
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
		"ENUM", "enum",
	}

	for _, sqlType := range stringTypes {
		t.Run(sqlType, func(t *testing.T) {
			assert.Equal(t, TypeString, MapToGraphQL(sqlType))
			assert.Equal(t, "String", MapToGraphQL(sqlType).String())
			assert.Equal(t, "StringFilter", MapToGraphQL(sqlType).FilterTypeName())
		})
	}
}

func TestMapToGraphQL_BytesTypes(t *testing.T) {
	bytesTypes := []string{
		"BLOB", "blob",
		"TINYBLOB", "tinyblob",
		"MEDIUMBLOB", "mediumblob",
		"LONGBLOB", "longblob",
		"BINARY", "binary",
		"VARBINARY", "varbinary",
	}

	for _, sqlType := range bytesTypes {
		t.Run(sqlType, func(t *testing.T) {
			assert.Equal(t, TypeBytes, MapToGraphQL(sqlType))
			assert.Equal(t, "Bytes", MapToGraphQL(sqlType).String())
			assert.Equal(t, "BytesFilter", MapToGraphQL(sqlType).FilterTypeName())
		})
	}
}

func TestMapToGraphQL_DateTimeTypes(t *testing.T) {
	dateTypes := []string{
		"DATETIME", "datetime",
		"TIMESTAMP", "timestamp",
	}

	for _, sqlType := range dateTypes {
		t.Run(sqlType, func(t *testing.T) {
			assert.Equal(t, TypeDateTime, MapToGraphQL(sqlType))
			assert.Equal(t, "DateTime", MapToGraphQL(sqlType).String())
			assert.Equal(t, "DateTimeFilter", MapToGraphQL(sqlType).FilterTypeName())
		})
	}
}

func TestMapToGraphQL_DateTypes(t *testing.T) {
	dateTypes := []string{
		"DATE", "date",
	}

	for _, sqlType := range dateTypes {
		t.Run(sqlType, func(t *testing.T) {
			assert.Equal(t, TypeDate, MapToGraphQL(sqlType))
			assert.Equal(t, "Date", MapToGraphQL(sqlType).String())
			assert.Equal(t, "DateFilter", MapToGraphQL(sqlType).FilterTypeName())
		})
	}
}

func TestMapToGraphQL_TimeTypes(t *testing.T) {
	timeTypes := []string{
		"TIME", "time",
	}

	for _, sqlType := range timeTypes {
		t.Run(sqlType, func(t *testing.T) {
			assert.Equal(t, TypeTime, MapToGraphQL(sqlType))
			assert.Equal(t, "Time", MapToGraphQL(sqlType).String())
			assert.Equal(t, "TimeFilter", MapToGraphQL(sqlType).FilterTypeName())
		})
	}
}

func TestMapToGraphQL_YearTypes(t *testing.T) {
	yearTypes := []string{
		"YEAR", "year",
	}

	for _, sqlType := range yearTypes {
		t.Run(sqlType, func(t *testing.T) {
			assert.Equal(t, TypeYear, MapToGraphQL(sqlType))
			assert.Equal(t, "Year", MapToGraphQL(sqlType).String())
			assert.Equal(t, "YearFilter", MapToGraphQL(sqlType).FilterTypeName())
		})
	}
}

func TestMapToGraphQL_SetTypes(t *testing.T) {
	setTypes := []string{
		"SET", "set",
	}

	for _, sqlType := range setTypes {
		t.Run(sqlType, func(t *testing.T) {
			assert.Equal(t, TypeSet, MapToGraphQL(sqlType))
			assert.Equal(t, "Set", MapToGraphQL(sqlType).String())
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

func TestGraphQLType_UUID(t *testing.T) {
	assert.Equal(t, "UUID", TypeUUID.String())
	assert.Equal(t, "UUIDFilter", TypeUUID.FilterTypeName())
	assert.False(t, TypeUUID.IsNumeric())
	assert.True(t, TypeUUID.IsComparable())
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
		{"decimal(10,2)", TypeDecimal, "Decimal"},
		{"DECIMAL(18,4)", TypeDecimal, "Decimal"},
		{"int(11)", TypeInt, "Int"},
		{"INT(10)", TypeInt, "Int"},
		{"bigint(20)", TypeBigInt, "BigInt"},
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
		{TypeBigInt, true},
		{TypeFloat, true},
		{TypeDecimal, true},
		{TypeString, false},
		{TypeBoolean, false},
		{TypeJSON, false},
		{TypeDate, false},
		{TypeDateTime, false},
		{TypeTime, false},
		{TypeYear, false},
		{TypeSet, false},
		{TypeBytes, false},
		{TypeUUID, false},
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
		{TypeBigInt, true},
		{TypeFloat, true},
		{TypeDecimal, true},
		{TypeString, true},
		{TypeBoolean, true},
		{TypeJSON, false}, // JSON is not comparable
		{TypeDate, true},
		{TypeDateTime, true},
		{TypeTime, true},
		{TypeYear, true},
		{TypeSet, true},
		{TypeBytes, true},
		{TypeUUID, true},
	}

	for _, tc := range testCases {
		t.Run(tc.graphQLType.String(), func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.graphQLType.IsComparable())
		})
	}
}
