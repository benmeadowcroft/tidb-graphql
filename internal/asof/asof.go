package asof

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
)

const (
	DirectiveName     = "asOf"
	ArgTime           = "time"
	ArgOffsetSeconds  = "offsetSeconds"
	rootFieldDepth    = 1
	offsetSecondsUnit = time.Second
	minGraphQLInt     = -1 << 31
	maxGraphQLInt     = 1<<31 - 1
)

// Spec describes an exact historical snapshot read.
type Spec struct {
	Time time.Time
}

// Identity returns a stable string suitable for cache keys and hidden row metadata.
func (s Spec) Identity() string {
	return s.Time.UTC().Format(time.RFC3339Nano)
}

// SessionValue returns the timestamp string used for TiDB snapshot session state.
func (s Spec) SessionValue() string {
	return s.Time.UTC().Format("2006-01-02 15:04:05.999999999")
}

// ResolveFieldDirective resolves the @asOf directive on a field when present.
func ResolveFieldDirective(field *ast.Field, variables map[string]any, now time.Time) (*Spec, error) {
	directive := FindFieldDirective(field)
	if directive == nil {
		return nil, nil
	}
	return ResolveDirective(directive, variables, now)
}

// FindFieldDirective returns the @asOf directive on the field when present.
func FindFieldDirective(field *ast.Field) *ast.Directive {
	if field == nil {
		return nil
	}
	for _, directive := range field.Directives {
		if directive != nil && directive.Name != nil && directive.Name.Value == DirectiveName {
			return directive
		}
	}
	return nil
}

// ResolveDirective parses and validates an @asOf directive into an exact snapshot.
func ResolveDirective(directive *ast.Directive, variables map[string]any, now time.Time) (*Spec, error) {
	if directive == nil {
		return nil, nil
	}

	timeArg := findDirectiveArgument(directive, ArgTime)
	offsetArg := findDirectiveArgument(directive, ArgOffsetSeconds)

	argCount := 0
	if timeArg != nil {
		argCount++
	}
	if offsetArg != nil {
		argCount++
	}
	if argCount != 1 {
		return nil, fmt.Errorf("@asOf requires exactly one of: time, offsetSeconds")
	}

	if timeArg != nil {
		resolved, ok := resolveValue(timeArg.Value, variables)
		if !ok {
			return nil, fmt.Errorf("@asOf time must be a valid DateTime")
		}
		parsed, ok := graphql.DateTime.ParseValue(resolved).(time.Time)
		if !ok {
			return nil, fmt.Errorf("@asOf time must be a valid DateTime")
		}
		if parsed.After(now) {
			return nil, fmt.Errorf("@asOf time must not be in the future")
		}
		return &Spec{Time: parsed}, nil
	}

	resolved, ok := resolveValue(offsetArg.Value, variables)
	if !ok {
		return nil, fmt.Errorf("@asOf offsetSeconds must be an Int")
	}
	offset, ok := parseOffsetSeconds(resolved)
	if !ok {
		return nil, fmt.Errorf("@asOf offsetSeconds must be an Int")
	}
	if offset > 0 {
		return nil, fmt.Errorf("@asOf offsetSeconds must be less than or equal to 0")
	}

	snapshot := now.Add(time.Duration(offset) * offsetSecondsUnit)
	if snapshot.After(now) {
		return nil, fmt.Errorf("@asOf time must not be in the future")
	}
	return &Spec{Time: snapshot}, nil
}

// ValidateOperation validates @asOf usage for the selected operation.
func ValidateOperation(op *ast.OperationDefinition, fragments map[string]*ast.FragmentDefinition, variables map[string]any, now time.Time) error {
	if op == nil {
		return nil
	}
	if fragments == nil {
		fragments = map[string]*ast.FragmentDefinition{}
	}
	visited := map[string]bool{}
	return validateSelectionSet(op.GetSelectionSet(), string(op.Operation), rootFieldDepth, fragments, visited, variables, now)
}

func validateSelectionSet(selectionSet *ast.SelectionSet, operationType string, depth int, fragments map[string]*ast.FragmentDefinition, visited map[string]bool, variables map[string]any, now time.Time) error {
	if selectionSet == nil {
		return nil
	}

	for _, selection := range selectionSet.Selections {
		switch sel := selection.(type) {
		case *ast.Field:
			if directive := FindFieldDirective(sel); directive != nil {
				if operationType != "query" || depth != rootFieldDepth {
					return fmt.Errorf("@asOf is only allowed on root query fields")
				}
				if _, err := ResolveDirective(directive, variables, now); err != nil {
					return err
				}
			}
			if err := validateSelectionSet(sel.SelectionSet, operationType, depth+1, fragments, visited, variables, now); err != nil {
				return err
			}
		case *ast.InlineFragment:
			if err := validateSelectionSet(sel.SelectionSet, operationType, depth, fragments, visited, variables, now); err != nil {
				return err
			}
		case *ast.FragmentSpread:
			name := ""
			if sel.Name != nil {
				name = sel.Name.Value
			}
			if name == "" || visited[name] {
				continue
			}
			visited[name] = true
			fragment := fragments[name]
			if fragment == nil {
				continue
			}
			if err := validateSelectionSet(fragment.SelectionSet, operationType, depth, fragments, visited, variables, now); err != nil {
				return err
			}
		}
	}

	return nil
}

func findDirectiveArgument(directive *ast.Directive, name string) *ast.Argument {
	if directive == nil {
		return nil
	}
	for _, arg := range directive.Arguments {
		if arg != nil && arg.Name != nil && arg.Name.Value == name {
			return arg
		}
	}
	return nil
}

func resolveValue(value ast.Value, variables map[string]any) (any, bool) {
	switch v := value.(type) {
	case *ast.Variable:
		if v == nil || v.Name == nil {
			return nil, false
		}
		resolved, ok := variables[v.Name.Value]
		return resolved, ok
	case *ast.StringValue:
		if v == nil {
			return nil, false
		}
		return v.Value, true
	case *ast.IntValue:
		if v == nil {
			return nil, false
		}
		return graphql.Int.ParseLiteral(v), true
	default:
		return nil, false
	}
}

func parseOffsetSeconds(value any) (int, bool) {
	switch v := value.(type) {
	case int:
		return v, true
	case int8:
		return int(v), true
	case int16:
		return int(v), true
	case int32:
		return int(v), true
	case int64:
		if v < minGraphQLInt || v > maxGraphQLInt {
			return 0, false
		}
		return int(v), true
	case float64:
		if math.Trunc(v) != v || v < minGraphQLInt || v > maxGraphQLInt {
			return 0, false
		}
		return int(v), true
	case json.Number:
		i, err := v.Int64()
		if err != nil || i < minGraphQLInt || i > maxGraphQLInt {
			return 0, false
		}
		return int(i), true
	default:
		return 0, false
	}
}

// DecodeVariables normalizes GraphQL variables payload into a generic map.
func DecodeVariables(raw json.RawMessage) (map[string]any, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, nil
	}
	var variables map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&variables); err != nil {
		return nil, err
	}
	return variables, nil
}
