// Package nodeid encodes and decodes Relay-style global node IDs.
package nodeid

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/sqltype"
)

const (
	dateLayout = "2006-01-02"
)

// Encode marshals the type name and primary key values into a base64-encoded JSON array.
func Encode(typeName string, pkValues ...interface{}) string {
	payload := make([]interface{}, 0, len(pkValues)+1)
	payload = append(payload, typeName)
	payload = append(payload, pkValues...)
	data, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	return base64.StdEncoding.EncodeToString(data)
}

// Decode parses a base64-encoded JSON array node ID and returns the type name and raw PK values.
func Decode(nodeID string) (string, []interface{}, error) {
	raw, err := base64.StdEncoding.DecodeString(nodeID)
	if err != nil {
		return "", nil, fmt.Errorf("invalid id: %w", err)
	}
	var payload []interface{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return "", nil, fmt.Errorf("invalid id: %w", err)
	}
	if len(payload) < 2 {
		return "", nil, errors.New("invalid id: missing type or primary key values")
	}
	typeName, ok := payload[0].(string)
	if !ok || typeName == "" {
		return "", nil, errors.New("invalid id: missing type name")
	}
	return typeName, payload[1:], nil
}

// ParsePKValue converts a decoded JSON value into the Go type expected by a PK column.
func ParsePKValue(col introspection.Column, raw interface{}) (interface{}, error) {
	if raw == nil {
		return nil, fmt.Errorf("missing primary key value for %s", col.Name)
	}

	switch sqltype.MapToGraphQL(col.DataType) {
	case sqltype.TypeInt, sqltype.TypeBigInt:
		switch v := raw.(type) {
		case float64:
			if v != math.Trunc(v) {
				return nil, fmt.Errorf("invalid integer value for %s", col.Name)
			}
			return int64(v), nil
		case int:
			return int64(v), nil
		case int32:
			return int64(v), nil
		case int64:
			return v, nil
		case uint:
			return int64(v), nil
		case uint32:
			return int64(v), nil
		case uint64:
			return int64(v), nil
		case string:
			parsed, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid integer value for %s", col.Name)
			}
			return parsed, nil
		default:
			return nil, fmt.Errorf("invalid integer value for %s", col.Name)
		}
	case sqltype.TypeFloat:
		switch v := raw.(type) {
		case float64:
			return v, nil
		case float32:
			return float64(v), nil
		case string:
			parsed, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid float value for %s", col.Name)
			}
			return parsed, nil
		default:
			return nil, fmt.Errorf("invalid float value for %s", col.Name)
		}
	case sqltype.TypeDecimal:
		switch v := raw.(type) {
		case string:
			return v, nil
		case []byte:
			return string(v), nil
		case float64:
			return strconv.FormatFloat(v, 'f', -1, 64), nil
		case float32:
			return strconv.FormatFloat(float64(v), 'f', -1, 32), nil
		default:
			return fmt.Sprintf("%v", v), nil
		}
	case sqltype.TypeBoolean:
		switch v := raw.(type) {
		case bool:
			return v, nil
		case string:
			parsed, err := strconv.ParseBool(v)
			if err != nil {
				return nil, fmt.Errorf("invalid boolean value for %s", col.Name)
			}
			return parsed, nil
		default:
			return nil, fmt.Errorf("invalid boolean value for %s", col.Name)
		}
	case sqltype.TypeDate:
		switch v := raw.(type) {
		case time.Time:
			return v, nil
		case string:
			if parsed, err := time.Parse(dateLayout, v); err == nil {
				return parsed, nil
			}
			if parsed, err := time.Parse(time.RFC3339, v); err == nil {
				return time.Date(parsed.Year(), parsed.Month(), parsed.Day(), 0, 0, 0, 0, time.UTC), nil
			}
			if parsed, err := time.Parse(time.RFC3339Nano, v); err == nil {
				return time.Date(parsed.Year(), parsed.Month(), parsed.Day(), 0, 0, 0, 0, time.UTC), nil
			}
			return nil, fmt.Errorf("invalid date value for %s", col.Name)
		default:
			return nil, fmt.Errorf("invalid date value for %s", col.Name)
		}
	case sqltype.TypeDateTime:
		switch v := raw.(type) {
		case time.Time:
			return v, nil
		case string:
			if parsed, err := time.Parse(time.RFC3339, v); err == nil {
				return parsed, nil
			}
			if parsed, err := time.Parse(time.RFC3339Nano, v); err == nil {
				return parsed, nil
			}
			return nil, fmt.Errorf("invalid datetime value for %s", col.Name)
		default:
			return nil, fmt.Errorf("invalid datetime value for %s", col.Name)
		}
	case sqltype.TypeBytes:
		switch v := raw.(type) {
		case string:
			decoded, err := base64.StdEncoding.DecodeString(v)
			if err != nil {
				return nil, fmt.Errorf("invalid bytes value for %s", col.Name)
			}
			return decoded, nil
		case []byte:
			return v, nil
		default:
			return nil, fmt.Errorf("invalid bytes value for %s", col.Name)
		}
	default:
		switch v := raw.(type) {
		case string:
			return v, nil
		case []byte:
			return string(v), nil
		default:
			return nil, fmt.Errorf("invalid value for %s", col.Name)
		}
	}
}
