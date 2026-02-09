// Package cursor encodes and decodes Relay-style connection cursors.
// Cursors are opaque base64-encoded JSON arrays containing ordering context
// and string-coerced values for seek-based pagination.
package cursor

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/nodeid"
)

// EncodeCursor builds an opaque cursor from type name, orderBy key, direction, and column values.
// Values are string-coerced for JSON safety (avoids float64→int64 precision loss).
func EncodeCursor(typeName, orderByKey, direction string, values ...interface{}) string {
	parts := make([]string, 0, 3+len(values))
	parts = append(parts, typeName, orderByKey, direction)
	for _, v := range values {
		parts = append(parts, coerceToString(v))
	}
	data, err := json.Marshal(parts)
	if err != nil {
		return ""
	}
	return base64.StdEncoding.EncodeToString(data)
}

// DecodeCursor parses a base64-encoded JSON cursor into its components.
// Returns type name, orderBy key, direction, and string-encoded column values.
func DecodeCursor(raw string) (typeName, orderByKey, direction string, values []string, err error) {
	data, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return "", "", "", nil, fmt.Errorf("invalid cursor: %w", err)
	}
	var parts []string
	if err := json.Unmarshal(data, &parts); err != nil {
		return "", "", "", nil, fmt.Errorf("invalid cursor: %w", err)
	}
	if len(parts) < 4 {
		return "", "", "", nil, fmt.Errorf("invalid cursor: expected at least 4 elements, got %d", len(parts))
	}
	return parts[0], parts[1], parts[2], parts[3:], nil
}

// ValidateCursor confirms the cursor matches the expected query context.
func ValidateCursor(expectedType, expectedOrderByKey, expectedDirection, actualType, actualOrderByKey, actualDirection string) error {
	if actualType != expectedType {
		return fmt.Errorf("cursor type mismatch: expected %s, got %s", expectedType, actualType)
	}
	if actualOrderByKey != expectedOrderByKey {
		return fmt.Errorf("cursor orderBy mismatch: expected %s, got %s", expectedOrderByKey, actualOrderByKey)
	}
	if actualDirection != expectedDirection {
		return fmt.Errorf("cursor direction mismatch: expected %s, got %s", expectedDirection, actualDirection)
	}
	return nil
}

// ParseCursorValues converts string-encoded cursor values into native Go types
// based on the column definitions. Reuses nodeid.ParsePKValue for type coercion.
func ParseCursorValues(stringVals []string, columns []introspection.Column) ([]interface{}, error) {
	if len(stringVals) != len(columns) {
		return nil, fmt.Errorf("cursor value count mismatch: expected %d, got %d", len(columns), len(stringVals))
	}
	result := make([]interface{}, len(stringVals))
	for i, sv := range stringVals {
		parsed, err := nodeid.ParsePKValue(columns[i], sv)
		if err != nil {
			return nil, fmt.Errorf("invalid cursor value for %s: %w", columns[i].Name, err)
		}
		result[i] = parsed
	}
	return result, nil
}

func coerceToString(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case time.Time:
		return val.Format(time.RFC3339)
	case int:
		return fmt.Sprintf("%d", val)
	case int32:
		return fmt.Sprintf("%d", val)
	case int64:
		return fmt.Sprintf("%d", val)
	case uint:
		return fmt.Sprintf("%d", val)
	case uint32:
		return fmt.Sprintf("%d", val)
	case uint64:
		return fmt.Sprintf("%d", val)
	case float32:
		return fmt.Sprintf("%g", val)
	case float64:
		return fmt.Sprintf("%g", val)
	case bool:
		return fmt.Sprintf("%t", val)
	case []byte:
		return string(val)
	default:
		return fmt.Sprintf("%v", val)
	}
}
