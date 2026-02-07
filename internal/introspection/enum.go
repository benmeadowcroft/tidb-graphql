package introspection

import (
	"fmt"
	"strings"
)

// parseEnumValues interprets INFORMATION_SCHEMA.COLUMNS.COLUMN_TYPE without a DB round-trip,
// so schema builds can carry enum metadata even when only metadata is available.
func parseEnumValues(columnType string) ([]string, error) {
	trimmed := strings.TrimSpace(columnType)
	if len(trimmed) < len("enum()") {
		return nil, fmt.Errorf("invalid enum definition")
	}
	lower := strings.ToLower(trimmed)
	if !strings.HasPrefix(lower, "enum(") || !strings.HasSuffix(lower, ")") {
		return nil, fmt.Errorf("invalid enum prefix or suffix")
	}

	definition := trimmed[len("enum(") : len(trimmed)-1]
	values := []string{}
	i := 0
	for i < len(definition) {
		for i < len(definition) && (definition[i] == ' ' || definition[i] == ',') {
			i++
		}
		if i >= len(definition) {
			break
		}
		if definition[i] != '\'' {
			return nil, fmt.Errorf("expected quote at position %d", i)
		}
		i++
		var sb strings.Builder
		for i < len(definition) {
			ch := definition[i]
			if ch == '\\' {
				if i+1 >= len(definition) {
					return nil, fmt.Errorf("unterminated escape")
				}
				sb.WriteByte(definition[i+1])
				i += 2
				continue
			}
			if ch == '\'' {
				if i+1 < len(definition) && definition[i+1] == '\'' {
					sb.WriteByte('\'')
					i += 2
					continue
				}
				i++
				break
			}
			sb.WriteByte(ch)
			i++
		}
		values = append(values, sb.String())
		for i < len(definition) && definition[i] == ' ' {
			i++
		}
		if i < len(definition) {
			if definition[i] == ',' {
				i++
			} else {
				return nil, fmt.Errorf("expected comma at position %d", i)
			}
		}
	}

	if len(values) == 0 {
		return nil, fmt.Errorf("no enum values parsed")
	}
	return values, nil
}
