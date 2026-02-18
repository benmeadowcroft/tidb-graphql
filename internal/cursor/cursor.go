// Package cursor encodes and decodes Relay-style connection cursors.
// Cursors are opaque base64-encoded JSON arrays containing ordering context
// and string-coerced values for seek-based pagination.
package cursor

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/nodeid"
)

type payloadV2 struct {
	Version    int      `json:"v"`
	TypeName   string   `json:"t"`
	OrderByKey string   `json:"k"`
	Directions []string `json:"d"`
	Values     []string `json:"vals"`
}

// EncodeCursor builds an opaque cursor from type name, orderBy key, directions, and column values.
// Values are string-coerced for JSON safety (avoids float64→int64 precision loss).
func EncodeCursor(typeName, orderByKey string, directions []string, values ...interface{}) string {
	normalizedDirections := make([]string, len(directions))
	for i, direction := range directions {
		normalizedDirections[i] = strings.ToUpper(direction)
	}
	stringValues := make([]string, 0, len(values))
	for _, v := range values {
		stringValues = append(stringValues, coerceToString(v))
	}
	payload := payloadV2{
		Version:    2,
		TypeName:   typeName,
		OrderByKey: orderByKey,
		Directions: normalizedDirections,
		Values:     stringValues,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	return base64.StdEncoding.EncodeToString(data)
}

// DecodeCursor parses a base64-encoded JSON cursor into its components.
// Returns type name, orderBy key, directions, and string-encoded column values.
func DecodeCursor(raw string) (typeName, orderByKey string, directions []string, values []string, err error) {
	data, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return "", "", nil, nil, fmt.Errorf("invalid cursor: %w", err)
	}
	var payload payloadV2
	if err := json.Unmarshal(data, &payload); err != nil {
		return "", "", nil, nil, fmt.Errorf("invalid cursor format: expected orderBy v2 cursor")
	}
	if payload.Version != 2 {
		return "", "", nil, nil, fmt.Errorf("invalid cursor format: expected orderBy v2 cursor")
	}
	if payload.TypeName == "" || payload.OrderByKey == "" {
		return "", "", nil, nil, fmt.Errorf("invalid cursor: missing type or orderBy key")
	}
	if len(payload.Directions) == 0 {
		return "", "", nil, nil, fmt.Errorf("invalid cursor: missing directions")
	}
	for i, direction := range payload.Directions {
		direction = strings.ToUpper(direction)
		if direction != "ASC" && direction != "DESC" {
			return "", "", nil, nil, fmt.Errorf("invalid cursor: direction %d must be ASC or DESC", i)
		}
		payload.Directions[i] = direction
	}
	if len(payload.Values) != len(payload.Directions) {
		return "", "", nil, nil, fmt.Errorf("invalid cursor: value count mismatch for orderBy columns")
	}
	return payload.TypeName, payload.OrderByKey, payload.Directions, payload.Values, nil
}

// ValidateCursor confirms the cursor matches the expected query context.
func ValidateCursor(expectedType, expectedOrderByKey string, expectedDirections []string, actualType, actualOrderByKey string, actualDirections []string) error {
	if actualType != expectedType {
		return fmt.Errorf("cursor type mismatch: expected %s, got %s", expectedType, actualType)
	}
	if actualOrderByKey != expectedOrderByKey {
		return fmt.Errorf("cursor orderBy mismatch: expected %s, got %s", expectedOrderByKey, actualOrderByKey)
	}
	if len(actualDirections) != len(expectedDirections) {
		return fmt.Errorf("cursor direction count mismatch: expected %d, got %d", len(expectedDirections), len(actualDirections))
	}
	for i := range expectedDirections {
		expectedDirection := strings.ToUpper(expectedDirections[i])
		actualDirection := strings.ToUpper(actualDirections[i])
		if actualDirection != expectedDirection {
			return fmt.Errorf("cursor direction mismatch at position %d: expected %s, got %s", i, expectedDirection, actualDirection)
		}
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

// ParseVectorCursorValues parses a vector-search cursor payload where the first
// value is the computed distance (float) followed by table primary key values.
func ParseVectorCursorValues(stringVals []string, pkColumns []introspection.Column) (float64, []interface{}, error) {
	if len(stringVals) != len(pkColumns)+1 {
		return 0, nil, fmt.Errorf("cursor value count mismatch: expected %d, got %d", len(pkColumns)+1, len(stringVals))
	}
	distance, err := strconv.ParseFloat(stringVals[0], 64)
	if err != nil || math.IsNaN(distance) || math.IsInf(distance, 0) {
		return 0, nil, fmt.Errorf("invalid cursor distance value")
	}
	pkValues := make([]interface{}, len(pkColumns))
	for i := range pkColumns {
		parsed, err := nodeid.ParsePKValue(pkColumns[i], stringVals[i+1])
		if err != nil {
			return 0, nil, fmt.Errorf("invalid cursor value for %s: %w", pkColumns[i].Name, err)
		}
		pkValues[i] = parsed
	}
	return distance, pkValues, nil
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
