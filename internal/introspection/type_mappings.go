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
	if isTinyIntOne(col) {
		return sqltype.TypeBoolean
	}
	return sqltype.MapToGraphQL(col.DataType)
}

// ApplyTinyInt1TypeOverrides marks tinyint(1) columns as explicit bool/int overrides
// based on SQL table/column glob patterns. Patterns are matched case-insensitively
// against SQL names. intPatterns take precedence over boolPatterns when both match.
func ApplyTinyInt1TypeOverrides(schema *Schema, boolPatterns, intPatterns map[string][]string) error {
	if schema == nil || (len(boolPatterns) == 0 && len(intPatterns) == 0) {
		return nil
	}

	for ti := range schema.Tables {
		table := &schema.Tables[ti]
		boolColumnPatterns := mergePatterns(boolPatterns, table.Name)
		intColumnPatterns := mergePatterns(intPatterns, table.Name)
		if len(boolColumnPatterns) == 0 && len(intColumnPatterns) == 0 {
			continue
		}

		for ci := range table.Columns {
			col := &table.Columns[ci]
			matchesBool := matchesAny(col.Name, boolColumnPatterns)
			matchesInt := matchesAny(col.Name, intColumnPatterns)
			if !matchesBool && !matchesInt {
				continue
			}

			if !isTinyIntOne(*col) {
				typeSpec := strings.TrimSpace(col.ColumnType)
				if typeSpec == "" {
					typeSpec = strings.TrimSpace(col.DataType)
				}
				return fmt.Errorf("invalid tinyint(1) mapping for %s.%s: expected tinyint(1), got %q", table.Name, col.Name, typeSpec)
			}

			// int mappings are the explicit escape hatch and always win.
			if matchesInt {
				col.OverrideType = sqltype.TypeInt
				col.HasOverrideType = true
				continue
			}
			col.OverrideType = sqltype.TypeBoolean
			col.HasOverrideType = true
		}
	}
	return nil
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
	// Use explicit first-seen deduplication: slices.Compact only removed adjacent duplicates.
	deduped := make([]string, 0, len(combined))
	for _, pattern := range combined {
		if !slices.Contains(deduped, pattern) {
			deduped = append(deduped, pattern)
		}
	}
	return deduped
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

func isTinyIntOne(col Column) bool {
	typeSpec := strings.ToLower(strings.TrimSpace(col.ColumnType))
	if typeSpec == "" {
		return false
	}
	if !strings.HasPrefix(typeSpec, "tinyint(") {
		return false
	}

	open := strings.Index(typeSpec, "(")
	close := strings.Index(typeSpec, ")")
	if open == -1 || close == -1 || close <= open+1 {
		return false
	}
	widthSpec := strings.TrimSpace(typeSpec[open+1 : close])
	width, err := strconv.Atoi(widthSpec)
	if err != nil {
		return false
	}
	return width == 1
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
