package planner

import (
	"errors"
	"fmt"
	"math"

	"tidb-graphql/internal/introspection"

	"github.com/graphql-go/graphql/language/ast"
)

// Plan is the top-level output of planning a GraphQL query.
// It will eventually hold root SQL plus any dependent child queries.
type Plan struct {
	Root    SQLQuery
	Table   introspection.Table
	Columns []introspection.Column
}

// RelationshipContext provides relationship-specific planning inputs.
type RelationshipContext struct {
	RelatedTable introspection.Table
	RemoteColumn string
	Value        interface{}
	IsManyToOne  bool
	IsOneToMany  bool
}

type planOptions struct {
	relationship *RelationshipContext
	limits       *PlanLimits
	fragments    map[string]ast.Definition
	defaultLimit int
}

// PlanOption customizes planning behavior for non-root contexts.
type PlanOption func(*planOptions)

// WithRelationship plans a relationship field using the provided context.
func WithRelationship(ctx RelationshipContext) PlanOption {
	return func(o *planOptions) {
		o.relationship = &ctx
	}
}

// WithLimits enforces planner cost limits for a query.
func WithLimits(limits PlanLimits) PlanOption {
	return func(o *planOptions) {
		o.limits = &limits
	}
}

// WithFragments provides GraphQL fragments for selection expansion.
func WithFragments(fragments map[string]ast.Definition) PlanOption {
	return func(o *planOptions) {
		o.fragments = fragments
	}
}

// WithDefaultListLimit overrides the fallback list limit used in planning.
func WithDefaultListLimit(limit int) PlanOption {
	return func(o *planOptions) {
		o.defaultLimit = limit
	}
}

// PlanQuery is the primary planning entrypoint (GraphQL AST -> SQL plan).
// This is a minimal implementation that plans a single root field.
func PlanQuery(dbSchema *introspection.Schema, field *ast.Field, args map[string]interface{}, opts ...PlanOption) (*Plan, error) {
	if dbSchema == nil || field == nil {
		return nil, errors.New("schema and field are required")
	}

	options := &planOptions{}
	for _, opt := range opts {
		opt(options)
	}

	defaultLimit := DefaultListLimit
	if options.defaultLimit > 0 {
		defaultLimit = options.defaultLimit
	}

	if err := validatePaginationArgs(args); err != nil {
		return nil, err
	}

	if options.limits != nil {
		cost := EstimateCost(field, args, defaultLimit)
		if err := validateLimits(cost, *options.limits); err != nil {
			return nil, err
		}
	}

	if options.relationship != nil {
		ctx := options.relationship
		if ctx.RelatedTable.Name == "" || ctx.RemoteColumn == "" {
			return nil, errors.New("relationship context is incomplete")
		}

		switch {
		case ctx.IsManyToOne:
			selected := SelectedColumns(ctx.RelatedTable, field, options.fragments)
			planned, err := PlanManyToOne(ctx.RelatedTable, selected, ctx.RemoteColumn, ctx.Value)
			if err != nil {
				return nil, err
			}
			return &Plan{Root: planned, Table: ctx.RelatedTable, Columns: selected}, nil
		case ctx.IsOneToMany:
			limit := GetArgInt(args, "limit", defaultLimit)
			offset := GetArgInt(args, "offset", 0)
			orderBy, err := ParseOrderBy(ctx.RelatedTable, args)
			if err != nil {
				return nil, err
			}
			selected := SelectedColumns(ctx.RelatedTable, field, options.fragments)
			planned, err := PlanOneToMany(ctx.RelatedTable, selected, ctx.RemoteColumn, ctx.Value, limit, offset, orderBy)
			if err != nil {
				return nil, err
			}
			return &Plan{Root: planned, Table: ctx.RelatedTable, Columns: selected}, nil
		default:
			return nil, errors.New("relationship context missing direction")
		}
	}

	fieldName := field.Name.Value

	for _, table := range dbSchema.Tables {
		listField := introspection.GraphQLQueryName(table)
		if fieldName == listField {
			limit := GetArgInt(args, "limit", defaultLimit)
			offset := GetArgInt(args, "offset", 0)
			orderBy, err := ParseOrderBy(table, args)
			if err != nil {
				return nil, err
			}

			// Parse WHERE clause if provided
			var whereClause *WhereClause
			if whereArg, ok := args["where"]; ok {
				if whereMap, ok := whereArg.(map[string]interface{}); ok {
					whereClause, err = BuildWhereClause(table, whereMap)
					if err != nil {
						return nil, fmt.Errorf("invalid WHERE clause: %w", err)
					}

					// Validate that at least one indexed column is used
					if whereClause != nil {
						if err := ValidateIndexedColumns(table, whereClause.UsedColumns); err != nil {
							return nil, err
						}
					}
				}
			}

			selected := SelectedColumns(table, field, options.fragments)
			planned, err := PlanTableList(table, selected, limit, offset, orderBy, whereClause)
			if err != nil {
				return nil, err
			}
			return &Plan{Root: planned, Table: table, Columns: selected}, nil
		}

		singleField := introspection.GraphQLSingleQueryName(table)
		// Primary key lookup uses the singular field name (e.g., "user" not "user_by_pk")
		if fieldName == singleField {
			pkCols := introspection.PrimaryKeyColumns(table)
			if len(pkCols) == 0 {
				return nil, fmt.Errorf("no primary key for table %s", table.Name)
			}

			selected := SelectedColumns(table, field, options.fragments)

			if len(pkCols) == 1 {
				// Single column PK - use existing optimized path
				pk := &pkCols[0]
				pkArgName := introspection.GraphQLFieldName(*pk)
				pkValue, ok := args[pkArgName]
				if !ok {
					return nil, fmt.Errorf("missing primary key argument %s", pkArgName)
				}
				planned, err := PlanTableByPK(table, selected, pk, pkValue)
				if err != nil {
					return nil, err
				}
				return &Plan{Root: planned, Table: table, Columns: selected}, nil
			}

			// Composite PK - extract all values
			values := make(map[string]interface{})
			for _, col := range pkCols {
				argName := introspection.GraphQLFieldName(col)
				argValue, ok := args[argName]
				if !ok {
					return nil, fmt.Errorf("missing primary key argument %s", argName)
				}
				values[col.Name] = argValue
			}
			planned, err := PlanTableByPKColumns(table, selected, pkCols, values)
			if err != nil {
				return nil, err
			}
			return &Plan{Root: planned, Table: table, Columns: selected}, nil
		}

		pkCols := introspection.PrimaryKeyColumns(table)
		if len(pkCols) > 0 {
			// Build expected field name
			pkField := singleField + "_by"
			for _, col := range pkCols {
				pkField += "_" + introspection.GraphQLFieldName(col)
			}

			if fieldName == pkField {
				// Extract argument values
				values := make(map[string]interface{})
				for _, col := range pkCols {
					argName := introspection.GraphQLFieldName(col)
					argValue, ok := args[argName]
					if !ok {
						return nil, fmt.Errorf("missing primary key argument %s", argName)
					}
					values[col.Name] = argValue
				}

				selected := SelectedColumns(table, field, options.fragments)
				planned, err := PlanTableByPKColumns(table, selected, pkCols, values)
				if err != nil {
					return nil, err
				}
				return &Plan{Root: planned, Table: table, Columns: selected}, nil
			}
		}

		// Check for unique key lookups
		for _, idx := range table.Indexes {
			if !idx.Unique || idx.Name == "PRIMARY" {
				continue
			}

			// Build expected field name
			uniqueField := singleField + "_by"
			for _, colName := range idx.Columns {
				col, ok := findColumn(table.Columns, colName)
				if ok {
					uniqueField += "_" + introspection.GraphQLFieldName(col)
				} else {
					uniqueField += "_" + introspection.ToGraphQLFieldName(colName)
				}
			}

			if fieldName == uniqueField {
				// Extract argument values
				values := make(map[string]interface{})
				for _, colName := range idx.Columns {
					col, ok := findColumn(table.Columns, colName)
					var argName string
					if ok {
						argName = introspection.GraphQLFieldName(col)
					} else {
						argName = introspection.ToGraphQLFieldName(colName)
					}
					argValue, ok := args[argName]
					if !ok {
						return nil, fmt.Errorf("missing unique key argument %s", argName)
					}
					values[colName] = argValue
				}

				selected := SelectedColumns(table, field, options.fragments)
				planned, err := PlanUniqueKeyLookup(table, selected, idx, values)
				if err != nil {
					return nil, err
				}
				return &Plan{Root: planned, Table: table, Columns: selected}, nil
			}
		}
	}

	return nil, fmt.Errorf("unsupported root field %s", fieldName)
}

// GetArgInt extracts an integer argument from a GraphQL args map.
// Returns the fallback value if the key is missing or not an int.
func GetArgInt(args map[string]interface{}, key string, fallback int) int {
	if args == nil {
		return fallback
	}
	if val, ok := args[key]; ok {
		if intVal, ok := val.(int); ok {
			return intVal
		}
	}
	return fallback
}

func validatePaginationArgs(args map[string]interface{}) error {
	if args == nil {
		return nil
	}
	if err := validateNonNegativeIntArg(args, "limit"); err != nil {
		return err
	}
	if err := validateNonNegativeIntArg(args, "offset"); err != nil {
		return err
	}
	return nil
}

func validateNonNegativeIntArg(args map[string]interface{}, key string) error {
	value, ok := args[key]
	if !ok || value == nil {
		return nil
	}

	switch v := value.(type) {
	case int:
		if v < 0 {
			return fmt.Errorf("%s must be non-negative", key)
		}
	case int64:
		if v < 0 {
			return fmt.Errorf("%s must be non-negative", key)
		}
	case float64:
		if v < 0 || v != math.Trunc(v) {
			return fmt.Errorf("%s must be a non-negative integer", key)
		}
	default:
		return fmt.Errorf("%s must be a non-negative integer", key)
	}

	return nil
}

// SelectedColumns determines which columns to select based on the field selection set.
func SelectedColumns(table introspection.Table, field *ast.Field, fragments map[string]ast.Definition) []introspection.Column {
	if field == nil || field.SelectionSet == nil {
		return table.Columns
	}

	columnByField := make(map[string]string, len(table.Columns))
	for _, col := range table.Columns {
		columnByField[introspection.GraphQLFieldName(col)] = col.Name
	}

	relationshipByField := make(map[string]string, len(table.Relationships))
	for _, rel := range table.Relationships {
		relationshipByField[rel.GraphQLFieldName] = rel.LocalColumn
	}

	selected := make(map[string]struct{})

	var visitSelections func(selections []ast.Selection)
	visitSelections = func(selections []ast.Selection) {
		for _, selection := range selections {
			switch sel := selection.(type) {
			case *ast.Field:
				if sel.Name == nil {
					continue
				}
				name := sel.Name.Value
				if name == "__typename" {
					continue
				}
				if colName, ok := columnByField[name]; ok {
					selected[colName] = struct{}{}
				}
				if relCol, ok := relationshipByField[name]; ok {
					selected[relCol] = struct{}{}
				}
			case *ast.InlineFragment:
				if sel.SelectionSet != nil {
					visitSelections(sel.SelectionSet.Selections)
				}
			case *ast.FragmentSpread:
				if fragments == nil || sel.Name == nil {
					continue
				}
				def, ok := fragments[sel.Name.Value]
				if !ok {
					continue
				}
				fragment, ok := def.(*ast.FragmentDefinition)
				if !ok || fragment.SelectionSet == nil {
					continue
				}
				visitSelections(fragment.SelectionSet.Selections)
			}
		}
	}

	visitSelections(field.SelectionSet.Selections)

	if len(selected) == 0 {
		return table.Columns
	}

	if len(introspection.PrimaryKeyColumns(table)) > 0 {
		for _, col := range table.Columns {
			if col.IsPrimaryKey {
				selected[col.Name] = struct{}{}
			}
		}
	}

	columns := make([]introspection.Column, 0, len(selected))
	for _, col := range table.Columns {
		if _, ok := selected[col.Name]; ok {
			columns = append(columns, col)
		}
	}

	if len(columns) == 0 {
		return table.Columns
	}
	return columns
}

func findColumn(columns []introspection.Column, name string) (introspection.Column, bool) {
	for _, col := range columns {
		if col.Name == name {
			return col, true
		}
	}
	return introspection.Column{}, false
}

// EnsureColumns adds required columns to a selection and preserves table order.
func EnsureColumns(table introspection.Table, columns []introspection.Column, required []string) []introspection.Column {
	if len(required) == 0 {
		return columns
	}

	selected := make(map[string]struct{}, len(columns)+len(required))
	for _, col := range columns {
		selected[col.Name] = struct{}{}
	}
	for _, name := range required {
		if name != "" {
			selected[name] = struct{}{}
		}
	}

	if len(selected) == 0 {
		return columns
	}

	result := make([]introspection.Column, 0, len(selected))
	for _, col := range table.Columns {
		if _, ok := selected[col.Name]; ok {
			result = append(result, col)
		}
	}

	if len(result) == 0 {
		return columns
	}
	return result
}
