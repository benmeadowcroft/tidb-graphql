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
func EstimateCost(field *ast.Field, args map[string]interface{}, fallbackLimit int) PlanCost {
	if field == nil {
		return PlanCost{}
	}

	depth := selectionDepth(field, 1)
	rows := estimateRowsRecursive(field, args, fallbackLimit)
	complexity := estimateComplexityRecursive(field, args, fallbackLimit)

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

func selectionDepth(field *ast.Field, current int) int {
	if field.SelectionSet == nil || len(field.SelectionSet.Selections) == 0 {
		return current
	}

	maxDepth := current
	for _, selection := range field.SelectionSet.Selections {
		sub, ok := selection.(*ast.Field)
		if !ok {
			continue
		}
		depth := selectionDepth(sub, current+1)
		if depth > maxDepth {
			maxDepth = depth
		}
	}
	return maxDepth
}

func estimateRowsRecursive(field *ast.Field, args map[string]interface{}, fallbackLimit int) int {
	if field == nil {
		return 0
	}

	limit := listLimitForField(field, args, fallbackLimit)
	rows := limit

	if field.SelectionSet == nil || len(field.SelectionSet.Selections) == 0 {
		return rows
	}

	for _, selection := range field.SelectionSet.Selections {
		sub, ok := selection.(*ast.Field)
		if !ok {
			continue
		}
		childRows := estimateRowsRecursive(sub, nil, fallbackLimit)
		rows += limit * childRows
	}

	return rows
}

func estimateComplexityRecursive(field *ast.Field, args map[string]interface{}, fallbackLimit int) int {
	if field == nil {
		return 0
	}

	limit := listLimitForField(field, args, fallbackLimit)
	complexity := 1

	if field.SelectionSet == nil || len(field.SelectionSet.Selections) == 0 {
		return complexity * limit
	}

	for _, selection := range field.SelectionSet.Selections {
		sub, ok := selection.(*ast.Field)
		if !ok {
			continue
		}
		complexity += limit * estimateComplexityRecursive(sub, nil, fallbackLimit)
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
	if !hasLimitArg(field) {
		if _, ok := argInt(args, "limit"); !ok {
			return 1
		}
	}

	if limit, ok := argInt(args, "limit"); ok {
		return limit
	}
	if limit := limitFromAST(field, fallback); limit > 0 {
		return limit
	}
	return fallback
}

func hasLimitArg(field *ast.Field) bool {
	if field == nil {
		return false
	}
	for _, arg := range field.Arguments {
		if arg == nil || arg.Name == nil {
			continue
		}
		if arg.Name.Value == "limit" {
			return true
		}
	}
	return false
}

func parseInt(value string) (int, error) {
	var result int
	_, err := fmt.Sscanf(value, "%d", &result)
	return result, err
}
