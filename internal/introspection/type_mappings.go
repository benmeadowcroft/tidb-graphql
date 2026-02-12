package introspection

import (
	"fmt"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"

	"tidb-graphql/internal/sqltype"
)

// EffectiveGraphQLType returns the final GraphQL type category for a column,
// including explicit overrides resolved during schema preparation.
func EffectiveGraphQLType(col Column) sqltype.GraphQLType {
	if col.HasOverrideType {
		return col.OverrideType
	}
	return sqltype.MapToGraphQL(col.DataType)
}

// ApplyUUIDTypeOverrides marks columns as TypeUUID based on SQL table/column glob patterns.
// Patterns are matched case-insensitively against SQL names.
func ApplyUUIDTypeOverrides(schema *Schema, patterns map[string][]string) error {
	if schema == nil || len(patterns) == 0 {
		return nil
	}
	for ti := range schema.Tables {
		table := &schema.Tables[ti]
		columnPatterns := mergePatterns(patterns, table.Name)
		if len(columnPatterns) == 0 {
			continue
		}
		for ci := range table.Columns {
			col := &table.Columns[ci]
			if !matchesAny(col.Name, columnPatterns) {
				continue
			}
			if err := validateUUIDOverrideColumn(*col); err != nil {
				return fmt.Errorf("invalid UUID mapping for %s.%s: %w", table.Name, col.Name, err)
			}
			col.OverrideType = sqltype.TypeUUID
			col.HasOverrideType = true
		}
	}
	return nil
}

func mergePatterns(patterns map[string][]string, table string) []string {
	if patterns == nil {
		return nil
	}
	tableLower := strings.ToLower(table)
	keys := make([]string, 0, len(patterns))
	for key := range patterns {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	combined := make([]string, 0)
	for _, key := range keys {
		// Table keys are glob patterns over SQL table names (case-insensitive),
		// so "*" and specific patterns can contribute column patterns.
		pattern := strings.ToLower(strings.TrimSpace(key))
		if pattern == "" {
			continue
		}
		matched, err := path.Match(pattern, tableLower)
		if err != nil || !matched {
			continue
		}
		combined = append(combined, patterns[key]...)
	}
	return slices.Compact(combined)
}

func matchesAny(value string, patterns []string) bool {
	value = strings.ToLower(value)
	for _, pattern := range patterns {
		if pattern == "" {
			continue
		}
		ok, err := path.Match(strings.ToLower(pattern), value)
		if err != nil {
			continue
		}
		if ok {
			return true
		}
	}
	return false
}

func validateUUIDOverrideColumn(col Column) error {
	baseType := strings.ToLower(strings.TrimSpace(col.DataType))
	switch baseType {
	case "binary", "varbinary":
		length, ok := sqlTypeLength(col)
		if !ok || length != 16 {
			return fmt.Errorf("%s requires length 16 for UUID binary storage", strings.ToUpper(baseType))
		}
		return nil
	case "char", "varchar":
		length, ok := sqlTypeLength(col)
		if !ok || length < 36 {
			return fmt.Errorf("%s requires length >= 36 for UUID text storage", strings.ToUpper(baseType))
		}
		return nil
	default:
		return fmt.Errorf("unsupported SQL type %q for UUID mapping", col.DataType)
	}
}

func sqlTypeLength(col Column) (int, bool) {
	typeSpec := strings.TrimSpace(col.ColumnType)
	if typeSpec == "" {
		typeSpec = strings.TrimSpace(col.DataType)
	}
	start := strings.Index(typeSpec, "(")
	end := strings.Index(typeSpec, ")")
	if start == -1 || end == -1 || end <= start+1 {
		return 0, false
	}
	lengthSpec := strings.TrimSpace(typeSpec[start+1 : end])
	if idx := strings.Index(lengthSpec, ","); idx != -1 {
		lengthSpec = strings.TrimSpace(lengthSpec[:idx])
	}
	if lengthSpec == "" {
		return 0, false
	}
	length, err := strconv.Atoi(lengthSpec)
	if err != nil {
		return 0, false
	}
	return length, true
}
