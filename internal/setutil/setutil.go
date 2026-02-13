package setutil

import (
	"fmt"
	"strings"
)

// Canonicalize validates and canonicalizes a SET value list according to allowed declaration order.
// Duplicate values are removed and the resulting CSV follows allowed order.
func Canonicalize(values []string, allowed []string) (string, error) {
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, v := range allowed {
		allowedSet[v] = struct{}{}
	}

	selected := make(map[string]struct{}, len(values))
	for _, v := range values {
		if _, ok := allowedSet[v]; !ok {
			return "", fmt.Errorf("invalid set value: %s", v)
		}
		selected[v] = struct{}{}
	}

	ordered := make([]string, 0, len(selected))
	for _, option := range allowed {
		if _, ok := selected[option]; ok {
			ordered = append(ordered, option)
		}
	}
	return strings.Join(ordered, ","), nil
}

// CanonicalizeAny canonicalizes a single set value provided as []string or []interface{}.
func CanonicalizeAny(input interface{}, allowed []string) (string, error) {
	values, err := normalizeStringSlice(input)
	if err != nil {
		return "", err
	}
	return Canonicalize(values, allowed)
}

// CanonicalizeMany canonicalizes a list of set values provided as [][]string, []interface{} of []interface{},
// or []interface{} of []string.
func CanonicalizeMany(input interface{}, allowed []string) ([]string, error) {
	switch v := input.(type) {
	case []interface{}:
		out := make([]string, 0, len(v))
		for _, item := range v {
			csv, err := CanonicalizeAny(item, allowed)
			if err != nil {
				return nil, err
			}
			out = append(out, csv)
		}
		return out, nil
	case [][]string:
		out := make([]string, 0, len(v))
		for _, item := range v {
			csv, err := Canonicalize(item, allowed)
			if err != nil {
				return nil, err
			}
			out = append(out, csv)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("set values must be an array")
	}
}

func normalizeStringSlice(input interface{}) ([]string, error) {
	switch v := input.(type) {
	case []string:
		return append([]string(nil), v...), nil
	case []interface{}:
		out := make([]string, 0, len(v))
		for _, item := range v {
			strVal, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("set values must be strings")
			}
			out = append(out, strVal)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("set values must be an array")
	}
}
