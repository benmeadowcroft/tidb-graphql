package scalars

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"tidb-graphql/internal/uuidutil"

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
	// GraphQL inputs may arrive as float64; guard bounds before int64 conversion
	// to avoid silent overflow on large numeric values.
	parseFloatInt := func(v float64) (int64, bool) {
		if v != math.Trunc(v) {
			return 0, false
		}
		if v < float64(math.MinInt64) || v > float64(math.MaxInt64) {
			return 0, false
		}
		return int64(v), true
	}

	// Unsigned JSON numbers can exceed signed 64-bit range.
	parseUint := func(v uint64) (int64, bool) {
		if v > math.MaxInt64 {
			return 0, false
		}
		return int64(v), true
	}

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
				parsed, ok := parseFloatInt(v)
				if !ok {
					return nil
				}
				return strconv.FormatInt(parsed, 10)
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
				parsed, ok := parseUint(uint64(v))
				if !ok {
					return nil
				}
				return parsed
			case uint8:
				return int64(v)
			case uint16:
				return int64(v)
			case uint32:
				return int64(v)
			case uint64:
				parsed, ok := parseUint(v)
				if !ok {
					return nil
				}
				return parsed
			case float64:
				parsed, ok := parseFloatInt(v)
				if !ok {
					return nil
				}
				return parsed
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
				if normalized, ok := normalizeDecimal(v); ok {
					return normalized
				}
				return nil
			case []byte:
				if normalized, ok := normalizeDecimal(string(v)); ok {
					return normalized
				}
				return nil
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
				return fmt.Sprintf("%v", v)
			case float32:
				if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
					return nil
				}
				return strconv.FormatFloat(float64(v), 'g', -1, 32)
			case float64:
				if math.IsNaN(v) || math.IsInf(v, 0) {
					return nil
				}
				return strconv.FormatFloat(v, 'g', -1, 64)
			default:
				return nil
			}
		},
		ParseLiteral: func(valueAST ast.Value) interface{} {
			switch v := valueAST.(type) {
			case *ast.StringValue:
				if normalized, ok := normalizeDecimal(v.Value); ok {
					return normalized
				}
				return nil
			case *ast.IntValue:
				return v.Value
			case *ast.FloatValue:
				if normalized, ok := normalizeDecimal(v.Value); ok {
					return normalized
				}
				return nil
			default:
				return nil
			}
		},
	})
}

func normalizeDecimal(raw string) (string, bool) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", false
	}
	if !decimalPattern.MatchString(value) {
		return "", false
	}
	return value, true
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

func Time() *graphql.Scalar {
	return graphql.NewScalar(graphql.ScalarConfig{
		Name:        "Time",
		Description: "Time value serialized as HH:MM:SS[.fraction]. Supports TiDB TIME range -838:59:59.000000 to 838:59:59.000000.",
		Serialize: func(value interface{}) interface{} {
			switch v := value.(type) {
			case string:
				normalized, ok := normalizeTiDBTime(v)
				if !ok {
					return nil
				}
				return normalized
			case []byte:
				normalized, ok := normalizeTiDBTime(string(v))
				if !ok {
					return nil
				}
				return normalized
			default:
				return nil
			}
		},
		ParseValue: func(value interface{}) interface{} {
			switch v := value.(type) {
			case string:
				normalized, ok := normalizeTiDBTime(v)
				if !ok {
					return nil
				}
				return normalized
			case []byte:
				normalized, ok := normalizeTiDBTime(string(v))
				if !ok {
					return nil
				}
				return normalized
			default:
				return nil
			}
		},
		ParseLiteral: func(valueAST ast.Value) interface{} {
			if sv, ok := valueAST.(*ast.StringValue); ok {
				normalized, ok := normalizeTiDBTime(sv.Value)
				if !ok {
					return nil
				}
				return normalized
			}
			return nil
		},
	})
}

func Year() *graphql.Scalar {
	return graphql.NewScalar(graphql.ScalarConfig{
		Name:        "Year",
		Description: "Year value in YYYY format. Supports TiDB YEAR range 0000 to 2155.",
		Serialize: func(value interface{}) interface{} {
			if normalized, ok := normalizeTiDBYear(value); ok {
				return normalized
			}
			return nil
		},
		ParseValue: func(value interface{}) interface{} {
			if normalized, ok := normalizeTiDBYear(value); ok {
				return normalized
			}
			return nil
		},
		ParseLiteral: func(valueAST ast.Value) interface{} {
			switch v := valueAST.(type) {
			case *ast.StringValue:
				if normalized, ok := normalizeTiDBYear(v.Value); ok {
					return normalized
				}
				return nil
			case *ast.IntValue:
				if normalized, ok := normalizeTiDBYear(v.Value); ok {
					return normalized
				}
				return nil
			default:
				return nil
			}
		},
	})
}

func Bytes() *graphql.Scalar {
	return graphql.NewScalar(graphql.ScalarConfig{
		Name:        "Bytes",
		Description: "Binary data serialized as RFC4648 base64 (standard alphabet with padding).",
		Serialize: func(value interface{}) interface{} {
			switch v := value.(type) {
			case []byte:
				return base64.StdEncoding.EncodeToString(v)
			case string:
				decoded, err := base64.StdEncoding.DecodeString(v)
				if err != nil {
					return nil
				}
				return base64.StdEncoding.EncodeToString(decoded)
			default:
				return nil
			}
		},
		ParseValue: func(value interface{}) interface{} {
			s, ok := value.(string)
			if !ok {
				return nil
			}
			decoded, err := base64.StdEncoding.DecodeString(s)
			if err != nil {
				return nil
			}
			return decoded
		},
		ParseLiteral: func(valueAST ast.Value) interface{} {
			sv, ok := valueAST.(*ast.StringValue)
			if !ok {
				return nil
			}
			decoded, err := base64.StdEncoding.DecodeString(sv.Value)
			if err != nil {
				return nil
			}
			return decoded
		},
	})
}

func UUID() *graphql.Scalar {
	return graphql.NewScalar(graphql.ScalarConfig{
		Name:        "UUID",
		Description: "UUID value serialized as lowercase canonical RFC4122 string.",
		Serialize: func(value interface{}) interface{} {
			switch v := value.(type) {
			case string:
				_, canonical, err := uuidutil.ParseString(v)
				if err != nil {
					return nil
				}
				return canonical
			case []byte:
				_, canonical, err := uuidutil.ParseBytes(v)
				if err != nil {
					return nil
				}
				return canonical
			default:
				return nil
			}
		},
		ParseValue: func(value interface{}) interface{} {
			s, ok := value.(string)
			if !ok {
				return nil
			}
			_, canonical, err := uuidutil.ParseString(s)
			if err != nil {
				return nil
			}
			return canonical
		},
		ParseLiteral: func(valueAST ast.Value) interface{} {
			sv, ok := valueAST.(*ast.StringValue)
			if !ok {
				return nil
			}
			_, canonical, err := uuidutil.ParseString(sv.Value)
			if err != nil {
				return nil
			}
			return canonical
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

var (
	decimalPattern          = regexp.MustCompile(`^[+-]?(?:\d+(?:\.\d+)?|\.\d+)(?:[eE][+-]?\d+)?$`)
	timeWithColonPattern    = regexp.MustCompile(`^(\d+):(\d{1,2})(?::(\d{1,2}))?(?:\.(\d{1,6}))?$`)
	timeNoColonPattern      = regexp.MustCompile(`^(\d{1,6})(?:\.(\d{1,6}))?$`)
	yearStringFormatPattern = regexp.MustCompile(`^\d{4}$`)
)

func normalizeTiDBTime(raw string) (string, bool) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return "", false
	}

	sign := ""
	if strings.HasPrefix(s, "-") || strings.HasPrefix(s, "+") {
		sign = s[:1]
		s = s[1:]
		if s == "" {
			return "", false
		}
	}

	hours, minutes, seconds, fraction, ok := parseTimeComponents(s)
	if !ok {
		return "", false
	}
	if minutes < 0 || minutes > 59 || seconds < 0 || seconds > 59 {
		return "", false
	}
	if hours > 838 || (hours == 838 && (minutes > 59 || seconds > 59)) {
		return "", false
	}
	if hours == 0 && minutes == 0 && seconds == 0 && sign == "-" {
		sign = ""
	}

	value := fmt.Sprintf("%s%02d:%02d:%02d", sign, hours, minutes, seconds)
	if fraction != "" {
		value += "." + fraction
	}
	return value, true
}

func parseTimeComponents(raw string) (int, int, int, string, bool) {
	if matches := timeWithColonPattern.FindStringSubmatch(raw); matches != nil {
		hours, err := strconv.Atoi(matches[1])
		if err != nil {
			return 0, 0, 0, "", false
		}
		minutes, err := strconv.Atoi(matches[2])
		if err != nil {
			return 0, 0, 0, "", false
		}
		seconds := 0
		if matches[3] != "" {
			seconds, err = strconv.Atoi(matches[3])
			if err != nil {
				return 0, 0, 0, "", false
			}
		}
		return hours, minutes, seconds, matches[4], true
	}

	matches := timeNoColonPattern.FindStringSubmatch(raw)
	if matches == nil {
		return 0, 0, 0, "", false
	}
	digits := matches[1]

	var hours, minutes, seconds int
	switch len(digits) {
	case 1, 2:
		parsed, err := strconv.Atoi(digits)
		if err != nil {
			return 0, 0, 0, "", false
		}
		seconds = parsed
	case 3, 4:
		minVal, err := strconv.Atoi(digits[:len(digits)-2])
		if err != nil {
			return 0, 0, 0, "", false
		}
		secVal, err := strconv.Atoi(digits[len(digits)-2:])
		if err != nil {
			return 0, 0, 0, "", false
		}
		minutes = minVal
		seconds = secVal
	case 5, 6:
		hourVal, err := strconv.Atoi(digits[:len(digits)-4])
		if err != nil {
			return 0, 0, 0, "", false
		}
		minVal, err := strconv.Atoi(digits[len(digits)-4 : len(digits)-2])
		if err != nil {
			return 0, 0, 0, "", false
		}
		secVal, err := strconv.Atoi(digits[len(digits)-2:])
		if err != nil {
			return 0, 0, 0, "", false
		}
		hours = hourVal
		minutes = minVal
		seconds = secVal
	default:
		return 0, 0, 0, "", false
	}

	return hours, minutes, seconds, matches[2], true
}

func normalizeTiDBYear(value interface{}) (string, bool) {
	var year int

	switch v := value.(type) {
	case int:
		year = v
	case int8:
		year = int(v)
	case int16:
		year = int(v)
	case int32:
		year = int(v)
	case int64:
		if v < math.MinInt32 || v > math.MaxInt32 {
			return "", false
		}
		year = int(v)
	case uint:
		if v > math.MaxInt32 {
			return "", false
		}
		year = int(v)
	case uint8:
		year = int(v)
	case uint16:
		year = int(v)
	case uint32:
		if v > math.MaxInt32 {
			return "", false
		}
		year = int(v)
	case uint64:
		if v > math.MaxInt32 {
			return "", false
		}
		year = int(v)
	case string:
		trimmed := strings.TrimSpace(v)
		if !yearStringFormatPattern.MatchString(trimmed) {
			return "", false
		}
		parsed, err := strconv.Atoi(trimmed)
		if err != nil {
			return "", false
		}
		year = parsed
	case []byte:
		return normalizeTiDBYear(string(v))
	default:
		return "", false
	}

	if year < 0 || year > 2155 {
		return "", false
	}
	return fmt.Sprintf("%04d", year), true
}
