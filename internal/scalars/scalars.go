package scalars

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"time"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
)

func NonNegativeInt() *graphql.Scalar {
	return graphql.NewScalar(graphql.ScalarConfig{
		Name:        "NonNegativeInt",
		Description: "An integer greater than or equal to zero.",
		Serialize: func(value interface{}) interface{} {
			if parsed, ok := coerceNonNegativeInt(value); ok {
				return parsed
			}
			return nil
		},
		ParseValue: func(value interface{}) interface{} {
			if parsed, ok := coerceNonNegativeInt(value); ok {
				return parsed
			}
			return nil
		},
		ParseLiteral: func(valueAST ast.Value) interface{} {
			intValue, ok := valueAST.(*ast.IntValue)
			if !ok {
				return nil
			}
			parsed, err := strconv.Atoi(intValue.Value)
			if err != nil || parsed < 0 {
				return nil
			}
			return parsed
		},
	})
}

func JSON() *graphql.Scalar {
	return graphql.NewScalar(graphql.ScalarConfig{
		Name:        "JSON",
		Description: "Arbitrary JSON value serialized as a string.",
		Serialize: func(value interface{}) interface{} {
			switch v := value.(type) {
			case []byte:
				return string(v)
			case string:
				return v
			case nil:
				return nil
			default:
				serialized, err := json.Marshal(v)
				if err != nil {
					slog.Default().Warn("failed to serialize JSON scalar", slog.String("error", err.Error()))
					return nil
				}
				return string(serialized)
			}
		},
		ParseValue: func(value interface{}) interface{} {
			if s, ok := value.(string); ok {
				return s
			}
			return nil
		},
		ParseLiteral: func(valueAST ast.Value) interface{} {
			if sv, ok := valueAST.(*ast.StringValue); ok {
				return sv.Value
			}
			return nil
		},
	})
}

func BigInt() *graphql.Scalar {
	return graphql.NewScalar(graphql.ScalarConfig{
		Name:        "BigInt",
		Description: "64-bit integer value serialized as a string.",
		Serialize: func(value interface{}) interface{} {
			switch v := value.(type) {
			case int:
				return strconv.FormatInt(int64(v), 10)
			case int8:
				return strconv.FormatInt(int64(v), 10)
			case int16:
				return strconv.FormatInt(int64(v), 10)
			case int32:
				return strconv.FormatInt(int64(v), 10)
			case int64:
				return strconv.FormatInt(v, 10)
			case uint:
				return strconv.FormatUint(uint64(v), 10)
			case uint8:
				return strconv.FormatUint(uint64(v), 10)
			case uint16:
				return strconv.FormatUint(uint64(v), 10)
			case uint32:
				return strconv.FormatUint(uint64(v), 10)
			case uint64:
				return strconv.FormatUint(v, 10)
			case float64:
				if v != math.Trunc(v) {
					return nil
				}
				return strconv.FormatInt(int64(v), 10)
			case string:
				if _, err := strconv.ParseInt(v, 10, 64); err == nil {
					return v
				}
				return nil
			case []byte:
				strVal := string(v)
				if _, err := strconv.ParseInt(strVal, 10, 64); err == nil {
					return strVal
				}
				return nil
			default:
				return nil
			}
		},
		ParseValue: func(value interface{}) interface{} {
			switch v := value.(type) {
			case int:
				return int64(v)
			case int8:
				return int64(v)
			case int16:
				return int64(v)
			case int32:
				return int64(v)
			case int64:
				return v
			case uint:
				return int64(v)
			case uint8:
				return int64(v)
			case uint16:
				return int64(v)
			case uint32:
				return int64(v)
			case uint64:
				if v > math.MaxInt64 {
					return nil
				}
				return int64(v)
			case float64:
				if v != math.Trunc(v) {
					return nil
				}
				return int64(v)
			case string:
				parsed, err := strconv.ParseInt(v, 10, 64)
				if err != nil {
					return nil
				}
				return parsed
			default:
				return nil
			}
		},
		ParseLiteral: func(valueAST ast.Value) interface{} {
			switch v := valueAST.(type) {
			case *ast.IntValue:
				parsed, err := strconv.ParseInt(v.Value, 10, 64)
				if err != nil {
					return nil
				}
				return parsed
			case *ast.StringValue:
				parsed, err := strconv.ParseInt(v.Value, 10, 64)
				if err != nil {
					return nil
				}
				return parsed
			default:
				return nil
			}
		},
	})
}

func Decimal() *graphql.Scalar {
	return graphql.NewScalar(graphql.ScalarConfig{
		Name:        "Decimal",
		Description: "Fixed-point decimal value serialized as a string.",
		Serialize: func(value interface{}) interface{} {
			switch v := value.(type) {
			case []byte:
				return string(v)
			case string:
				return v
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
				return fmt.Sprintf("%v", v)
			case float32, float64:
				return fmt.Sprintf("%v", v)
			default:
				return nil
			}
		},
		ParseValue: func(value interface{}) interface{} {
			switch v := value.(type) {
			case string:
				return v
			case []byte:
				return string(v)
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
				return fmt.Sprintf("%v", v)
			case float32, float64:
				return fmt.Sprintf("%v", v)
			default:
				return nil
			}
		},
		ParseLiteral: func(valueAST ast.Value) interface{} {
			switch v := valueAST.(type) {
			case *ast.StringValue:
				return v.Value
			case *ast.IntValue:
				return v.Value
			case *ast.FloatValue:
				return v.Value
			default:
				return nil
			}
		},
	})
}

func Date() *graphql.Scalar {
	return graphql.NewScalar(graphql.ScalarConfig{
		Name:        "Date",
		Description: "Date value serialized as YYYY-MM-DD.",
		Serialize: func(value interface{}) interface{} {
			switch v := value.(type) {
			case time.Time:
				return v.UTC().Format("2006-01-02")
			case *time.Time:
				if v == nil {
					return nil
				}
				return v.UTC().Format("2006-01-02")
			default:
				return nil
			}
		},
		ParseValue: func(value interface{}) interface{} {
			switch v := value.(type) {
			case time.Time:
				return v
			case string:
				if parsed, err := time.Parse("2006-01-02", v); err == nil {
					return parsed
				}
				if parsed, err := time.Parse(time.RFC3339, v); err == nil {
					return time.Date(parsed.Year(), parsed.Month(), parsed.Day(), 0, 0, 0, 0, time.UTC)
				}
				return nil
			default:
				return nil
			}
		},
		ParseLiteral: func(valueAST ast.Value) interface{} {
			if sv, ok := valueAST.(*ast.StringValue); ok {
				if parsed, err := time.Parse("2006-01-02", sv.Value); err == nil {
					return parsed
				}
				if parsed, err := time.Parse(time.RFC3339, sv.Value); err == nil {
					return time.Date(parsed.Year(), parsed.Month(), parsed.Day(), 0, 0, 0, 0, time.UTC)
				}
			}
			return nil
		},
	})
}

func coerceNonNegativeInt(value interface{}) (int, bool) {
	switch v := value.(type) {
	case int:
		if v < 0 {
			return 0, false
		}
		return v, true
	case int32:
		if v < 0 {
			return 0, false
		}
		return int(v), true
	case int64:
		if v < 0 || v > math.MaxInt {
			return 0, false
		}
		return int(v), true
	case float64:
		if v != math.Trunc(v) || v < 0 || v > math.MaxInt {
			return 0, false
		}
		return int(v), true
	case string:
		parsed, err := strconv.Atoi(v)
		if err != nil || parsed < 0 {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}
