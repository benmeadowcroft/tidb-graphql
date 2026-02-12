// Package sqltype provides a shared mapping from SQL data types to GraphQL type categories.
// This ensures consistent type mapping across schema generation and query resolution.
package sqltype

import "strings"

// GraphQLType represents the category of GraphQL scalar type for a SQL column.
type GraphQLType int

const (
	// TypeString is the default type for text, dates, and unknown SQL types.
	TypeString GraphQLType = iota
	// TypeInt represents integer numeric types.
	TypeInt
	// TypeBigInt represents 64-bit integer numeric types.
	TypeBigInt
	// TypeFloat represents floating-point and fixed-point numeric types.
	TypeFloat
	// TypeDecimal represents fixed-point numeric types that should preserve precision.
	TypeDecimal
	// TypeBoolean represents boolean types.
	TypeBoolean
	// TypeJSON represents JSON data types.
	TypeJSON
	// TypeDate represents date-only data types.
	TypeDate
	// TypeDateTime represents date/time data types.
	TypeDateTime
	// TypeTime represents time data types.
	TypeTime
	// TypeYear represents year data types.
	TypeYear
	// TypeSet represents SQL SET data types.
	TypeSet
	// TypeBytes represents binary/blob SQL data types.
	TypeBytes
	// TypeUUID represents UUID values mapped explicitly via configuration.
	TypeUUID
)

// MapToGraphQL converts a SQL data type string to its corresponding GraphQL type category.
// The input is case-insensitive. Size specifiers like (10,2) or (255) are stripped before matching.
// This handles both INFORMATION_SCHEMA.COLUMNS.DATA_TYPE (base type only) and COLUMN_TYPE (full type with size).
func MapToGraphQL(sqlType string) GraphQLType {
	// Strip size specifiers like (10,2) or (255)
	if idx := strings.Index(sqlType, "("); idx != -1 {
		sqlType = sqlType[:idx]
	}
	switch strings.ToUpper(sqlType) {
	// Integer Numeric Data Types
	case "TINYINT", "SMALLINT", "MEDIUMINT", "INT",
		"INTEGER", "SERIAL", "BIT":
		return TypeInt
	case "BIGINT":
		return TypeBigInt
	// Floating Point Numeric Data Types
	case "FLOAT", "DOUBLE":
		return TypeFloat
	// Fixed-Point Numeric Data Types
	case "DECIMAL", "NUMERIC":
		return TypeDecimal
	// Boolean Data Type
	case "BOOL", "BOOLEAN":
		return TypeBoolean
	// JSON Type
	case "JSON":
		return TypeJSON
	// String Data Types (explicit)
	case "CHAR", "VARCHAR", "TINYTEXT", "TEXT",
		"MEDIUMTEXT", "LONGTEXT", "ENUM":
		return TypeString
	case "SET":
		return TypeSet
	case "BINARY", "VARBINARY", "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB":
		return TypeBytes
	// Date and Time Data Types
	case "DATE":
		return TypeDate
	case "DATETIME", "TIMESTAMP":
		return TypeDateTime
	case "TIME":
		return TypeTime
	case "YEAR":
		return TypeYear
	default:
		return TypeString
	}
}

// String returns the GraphQL scalar type name for schema generation.
func (t GraphQLType) String() string {
	switch t {
	case TypeInt:
		return "Int"
	case TypeBigInt:
		return "BigInt"
	case TypeFloat:
		return "Float"
	case TypeDecimal:
		return "Decimal"
	case TypeBoolean:
		return "Boolean"
	case TypeJSON:
		return "JSON"
	case TypeDate:
		return "Date"
	case TypeDateTime:
		return "DateTime"
	case TypeTime:
		return "Time"
	case TypeYear:
		return "Year"
	case TypeSet:
		return "Set"
	case TypeBytes:
		return "Bytes"
	case TypeUUID:
		return "UUID"
	default:
		return "String"
	}
}

// FilterTypeName returns the corresponding filter input type name for WHERE clauses.
func (t GraphQLType) FilterTypeName() string {
	switch t {
	case TypeInt:
		return "IntFilter"
	case TypeBigInt:
		return "BigIntFilter"
	case TypeFloat:
		return "FloatFilter"
	case TypeDecimal:
		return "DecimalFilter"
	case TypeBoolean:
		return "BooleanFilter"
	case TypeDate:
		return "DateFilter"
	case TypeDateTime:
		return "DateTimeFilter"
	case TypeTime:
		return "TimeFilter"
	case TypeYear:
		return "YearFilter"
	case TypeSet:
		return "StringFilter"
	case TypeBytes:
		return "BytesFilter"
	case TypeUUID:
		return "UUIDFilter"
	default:
		// JSON and String both use StringFilter (JSON columns are skipped in WHERE)
		return "StringFilter"
	}
}

// IsNumeric returns true if the type can be used with AVG/SUM aggregations.
func (t GraphQLType) IsNumeric() bool {
	return t == TypeInt || t == TypeBigInt || t == TypeFloat || t == TypeDecimal
}

// IsComparable returns true if the type can be used with MIN/MAX aggregations.
// All types except JSON are comparable in SQL.
func (t GraphQLType) IsComparable() bool {
	return t != TypeJSON
}
