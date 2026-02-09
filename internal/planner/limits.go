package planner

import (
	"fmt"

	"github.com/graphql-go/graphql/language/ast"
)

const DefaultListLimit = 100

// PlanLimits defines cost limits applied during planning.
type PlanLimits struct {
	MaxDepth       int
	MaxComplexity  int
	MaxRows        int
	MaxStatements  int
	MaxRowsPerNode int
}

// PlanCost captures estimated cost for a query.
type PlanCost struct {
	Depth      int
	Complexity int
	Rows       int
	Statements int
}

// EstimateCost estimates cost based on the field selection and arguments.
func EstimateCost(field *ast.Field, args map[string]interface{}, fallbackLimit int, fragments map[string]ast.Definition) PlanCost {
	if field == nil {
		return PlanCost{}
	}

	depth := selectionDepth(field, 1, fragments)
	rows := estimateRowsRecursive(field, args, fallbackLimit, fragments)
	complexity := estimateComplexityRecursive(field, args, fallbackLimit, fragments)

	return PlanCost{
		Depth:      depth,
		Complexity: complexity,
		Rows:       rows,
		Statements: 1,
	}
}

func validateLimits(cost PlanCost, limits PlanLimits) error {
	if limits.MaxDepth > 0 && cost.Depth > limits.MaxDepth {
		return fmt.Errorf("query exceeds maximum depth of %d (depth: %d)", limits.MaxDepth, cost.Depth)
	}
	if limits.MaxComplexity > 0 && cost.Complexity > limits.MaxComplexity {
		return fmt.Errorf("query exceeds maximum complexity of %d (complexity: %d)", limits.MaxComplexity, cost.Complexity)
	}
	if limits.MaxRows > 0 && cost.Rows > limits.MaxRows {
		return fmt.Errorf("query exceeds maximum rows of %d (estimated: %d)", limits.MaxRows, cost.Rows)
	}
	if limits.MaxStatements > 0 && cost.Statements > limits.MaxStatements {
		return fmt.Errorf("query exceeds maximum statement count of %d (estimated: %d)", limits.MaxStatements, cost.Statements)
	}
	if limits.MaxRowsPerNode > 0 && cost.Rows > limits.MaxRowsPerNode {
		return fmt.Errorf("query exceeds maximum rows per node of %d (estimated: %d)", limits.MaxRowsPerNode, cost.Rows)
	}
	return nil
}

// isConnectionField returns true if the field uses Relay connection pagination
// (has a "first" argument), indicating its children are connection wrappers
// rather than actual data fields.
func isConnectionField(field *ast.Field) bool {
	return hasFirstArg(field)
}

// connectionDataSelections extracts the actual data field selections from a
// connection field by unwrapping Relay scaffolding. It looks inside
// edges → node and nodes for real column fields, and skips pageInfo,
// totalCount, and cursor which have no SQL cost.
func connectionDataSelections(field *ast.Field, fragments map[string]ast.Definition) []ast.Selection {
	if field.SelectionSet == nil {
		return nil
	}

	var result []ast.Selection

	var appendDataSelections func(selections []ast.Selection)
	appendDataSelections = func(selections []ast.Selection) {
		for _, sel := range selections {
			switch s := sel.(type) {
			case *ast.Field:
				result = append(result, s)
			case *ast.InlineFragment:
				if s.SelectionSet != nil {
					appendDataSelections(s.SelectionSet.Selections)
				}
			case *ast.FragmentSpread:
				if fragments == nil || s.Name == nil {
					continue
				}
				def, ok := fragments[s.Name.Value]
				if !ok {
					continue
				}
				frag, ok := def.(*ast.FragmentDefinition)
				if !ok || frag.SelectionSet == nil {
					continue
				}
				appendDataSelections(frag.SelectionSet.Selections)
			}
		}
	}

	var appendEdgeNodeSelections func(selections []ast.Selection)
	appendEdgeNodeSelections = func(selections []ast.Selection) {
		for _, sel := range selections {
			switch s := sel.(type) {
			case *ast.Field:
				if s.Name == nil {
					continue
				}
				if s.Name.Value == "node" && s.SelectionSet != nil {
					appendDataSelections(s.SelectionSet.Selections)
				}
			case *ast.InlineFragment:
				if s.SelectionSet != nil {
					appendEdgeNodeSelections(s.SelectionSet.Selections)
				}
			case *ast.FragmentSpread:
				if fragments == nil || s.Name == nil {
					continue
				}
				def, ok := fragments[s.Name.Value]
				if !ok {
					continue
				}
				frag, ok := def.(*ast.FragmentDefinition)
				if !ok || frag.SelectionSet == nil {
					continue
				}
				appendEdgeNodeSelections(frag.SelectionSet.Selections)
			}
		}
	}

	var visit func(selections []ast.Selection)
	visit = func(selections []ast.Selection) {
		for _, sel := range selections {
			switch s := sel.(type) {
			case *ast.Field:
				if s.Name == nil {
					continue
				}
				switch s.Name.Value {
				case "edges":
					if s.SelectionSet != nil {
						appendEdgeNodeSelections(s.SelectionSet.Selections)
					}
				case "nodes":
					if s.SelectionSet != nil {
						appendDataSelections(s.SelectionSet.Selections)
					}
				case "pageInfo", "totalCount":
					// No SQL cost — skip entirely
				}
			case *ast.InlineFragment:
				if s.SelectionSet != nil {
					visit(s.SelectionSet.Selections)
				}
			case *ast.FragmentSpread:
				if fragments == nil || s.Name == nil {
					continue
				}
				def, ok := fragments[s.Name.Value]
				if !ok {
					continue
				}
				frag, ok := def.(*ast.FragmentDefinition)
				if !ok || frag.SelectionSet == nil {
					continue
				}
				visit(frag.SelectionSet.Selections)
			}
		}
	}

	visit(field.SelectionSet.Selections)

	return result
}

func selectionDepth(field *ast.Field, current int, fragments map[string]ast.Definition) int {
	if field.SelectionSet == nil || len(field.SelectionSet.Selections) == 0 {
		return current
	}

	// For connections, recurse into the unwrapped data fields so that
	// wrapper levels (edges, node, pageInfo) don't inflate depth.
	selections := field.SelectionSet.Selections
	if isConnectionField(field) {
		selections = connectionDataSelections(field, fragments)
		if len(selections) == 0 {
			return current
		}
	}

	maxDepth := current
	for _, selection := range selections {
		sub, ok := selection.(*ast.Field)
		if !ok {
			continue
		}
		depth := selectionDepth(sub, current+1, fragments)
		if depth > maxDepth {
			maxDepth = depth
		}
	}
	return maxDepth
}

func estimateRowsRecursive(field *ast.Field, args map[string]interface{}, fallbackLimit int, fragments map[string]ast.Definition) int {
	if field == nil {
		return 0
	}

	limit := listLimitForField(field, args, fallbackLimit)
	rows := limit

	if field.SelectionSet == nil || len(field.SelectionSet.Selections) == 0 {
		return rows
	}

	// Unwrap connection scaffolding to count only real data fields.
	selections := field.SelectionSet.Selections
	if isConnectionField(field) {
		selections = connectionDataSelections(field, fragments)
		if len(selections) == 0 {
			return rows
		}
	}

	for _, selection := range selections {
		sub, ok := selection.(*ast.Field)
		if !ok {
			continue
		}
		childRows := estimateRowsRecursive(sub, nil, fallbackLimit, fragments)
		rows += limit * childRows
	}

	return rows
}

func estimateComplexityRecursive(field *ast.Field, args map[string]interface{}, fallbackLimit int, fragments map[string]ast.Definition) int {
	if field == nil {
		return 0
	}

	limit := listLimitForField(field, args, fallbackLimit)
	complexity := 1

	if field.SelectionSet == nil || len(field.SelectionSet.Selections) == 0 {
		return complexity * limit
	}

	// Unwrap connection scaffolding to count only real data fields.
	selections := field.SelectionSet.Selections
	if isConnectionField(field) {
		selections = connectionDataSelections(field, fragments)
		if len(selections) == 0 {
			return complexity * limit
		}
	}

	for _, selection := range selections {
		sub, ok := selection.(*ast.Field)
		if !ok {
			continue
		}
		complexity += limit * estimateComplexityRecursive(sub, nil, fallbackLimit, fragments)
	}

	return complexity
}

func limitFromAST(field *ast.Field, fallback int) int {
	if field == nil {
		return fallback
	}
	for _, arg := range field.Arguments {
		if arg == nil || arg.Name == nil || arg.Value == nil {
			continue
		}
		if arg.Name.Value != "limit" {
			continue
		}
		if intVal, ok := arg.Value.(*ast.IntValue); ok {
			if intVal.Value != "" {
				if parsed, err := parseInt(intVal.Value); err == nil {
					return parsed
				}
			}
		}
	}
	return fallback
}

func argInt(args map[string]interface{}, key string) (int, bool) {
	if args == nil {
		return 0, false
	}
	val, ok := args[key]
	if !ok {
		return 0, false
	}
	intVal, ok := val.(int)
	if !ok {
		return 0, false
	}
	if intVal < 0 {
		return 0, false
	}
	return intVal, true
}

func listLimitForField(field *ast.Field, args map[string]interface{}, fallback int) int {
	if !hasLimitArg(field) && !hasFirstArg(field) {
		if _, ok := argInt(args, "limit"); !ok {
			if _, ok := argInt(args, "first"); !ok {
				return 1
			}
		}
	}

	if limit, ok := argInt(args, "limit"); ok {
		return limit
	}
	if first, ok := argInt(args, "first"); ok {
		return first
	}
	// Check AST arguments without fallback so that a missing "limit" arg
	// doesn't shadow a present "first" arg via the fallback value.
	if limit := limitFromAST(field, 0); limit > 0 {
		return limit
	}
	if first := firstFromAST(field, 0); first > 0 {
		return first
	}
	return fallback
}

func hasLimitArg(field *ast.Field) bool {
	return hasArgNamed(field, "limit")
}

func hasFirstArg(field *ast.Field) bool {
	return hasArgNamed(field, "first")
}

func hasArgNamed(field *ast.Field, name string) bool {
	if field == nil {
		return false
	}
	for _, arg := range field.Arguments {
		if arg == nil || arg.Name == nil {
			continue
		}
		if arg.Name.Value == name {
			return true
		}
	}
	return false
}

func firstFromAST(field *ast.Field, fallback int) int {
	if field == nil {
		return fallback
	}
	for _, arg := range field.Arguments {
		if arg == nil || arg.Name == nil || arg.Value == nil {
			continue
		}
		if arg.Name.Value != "first" {
			continue
		}
		if intVal, ok := arg.Value.(*ast.IntValue); ok {
			if intVal.Value != "" {
				if parsed, err := parseInt(intVal.Value); err == nil {
					return parsed
				}
			}
		}
	}
	return fallback
}

func parseInt(value string) (int, error) {
	var result int
	_, err := fmt.Sscanf(value, "%d", &result)
	return result, err
}
