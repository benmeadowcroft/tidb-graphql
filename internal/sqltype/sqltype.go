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
	// TypeFloat represents floating-point and fixed-point numeric types.
	TypeFloat
	// TypeBoolean represents boolean types.
	TypeBoolean
	// TypeJSON represents JSON data types.
	TypeJSON
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
		"INTEGER", "BIGINT", "SERIAL", "BIT":
		return TypeInt
	// Floating Point Numeric Data Types
	case "FLOAT", "DOUBLE":
		return TypeFloat
	// Fixed-Point Numeric Data Types
	case "DECIMAL", "NUMERIC":
		return TypeFloat
	// Boolean Data Type
	case "BOOL", "BOOLEAN":
		return TypeBoolean
	// JSON Type
	case "JSON":
		return TypeJSON
	// String Data Types (explicit)
	case "CHAR", "VARCHAR", "TINYTEXT", "TEXT",
		"MEDIUMTEXT", "LONGTEXT", "BLOB", "TINYBLOB",
		"MEDIUMBLOB", "LONGBLOB", "BINARY", "VARBINARY",
		"ENUM", "SET":
		return TypeString
	// Date and Time Data Types
	case "DATE", "DATETIME", "TIMESTAMP", "TIME", "YEAR":
		return TypeString
	default:
		return TypeString
	}
}

// String returns the GraphQL scalar type name for schema generation.
func (t GraphQLType) String() string {
	switch t {
	case TypeInt:
		return "Int"
	case TypeFloat:
		return "Float"
	case TypeBoolean:
		return "Boolean"
	case TypeJSON:
		return "JSON"
	default:
		return "String"
	}
}

// FilterTypeName returns the corresponding filter input type name for WHERE clauses.
func (t GraphQLType) FilterTypeName() string {
	switch t {
	case TypeInt:
		return "IntFilter"
	case TypeFloat:
		return "FloatFilter"
	case TypeBoolean:
		return "BooleanFilter"
	default:
		// JSON and String both use StringFilter (JSON columns are skipped in WHERE)
		return "StringFilter"
	}
}
