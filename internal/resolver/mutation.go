package resolver

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/go-sql-driver/mysql"
	"github.com/graphql-go/graphql"

	"tidb-graphql/internal/dbexec"
	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/planner"
	"tidb-graphql/internal/schemafilter"
)

func (r *Resolver) addTableMutations(fields graphql.Fields, table introspection.Table) graphql.Fields {
	if table.IsView {
		return fields
	}
	if r.dbSchema != nil {
		if jc, ok := r.dbSchema.Junctions[table.Name]; ok && jc.Type == introspection.JunctionTypePure {
			return fields
		}
	}
	if !schemafilter.MutationTableAllowed(table.Name, r.filters) {
		return fields
	}

	pkCols := introspection.PrimaryKeyColumns(table)
	hasPK := len(pkCols) > 0

	typeName := r.singularTypeName(table)
	tableType := r.buildGraphQLType(table)

	insertableCols := r.mutationInsertableColumns(table)
	insertableMap := columnNameSet(insertableCols)
	if hasPK && len(insertableCols) > 0 && !missingRequiredInsertColumns(table, insertableMap) {
		createInput := r.createInputType(table, insertableCols)
		fields["create"+typeName] = &graphql.Field{
			Type: tableType,
			Args: graphql.FieldConfigArgument{
				"input": &graphql.ArgumentConfig{
					Type: graphql.NewNonNull(createInput),
				},
			},
			Resolve: r.makeCreateResolver(table, insertableMap),
		}
	}

	updatableCols := r.mutationUpdatableColumns(table)
	updatableMap := columnNameSet(updatableCols)
	if hasPK && len(updatableCols) > 0 {
		updateInput := r.updateSetInputType(table, updatableCols)
		args := r.primaryKeyArgs(pkCols)
		args["set"] = &graphql.ArgumentConfig{
			Type: updateInput,
		}
		fields["update"+typeName] = &graphql.Field{
			Type:    tableType,
			Args:    args,
			Resolve: r.makeUpdateResolver(table, updatableMap, pkCols),
		}
	}

	if hasPK {
		deletePayload := r.deletePayloadType(table, pkCols)
		args := r.primaryKeyArgs(pkCols)
		fields["delete"+typeName] = &graphql.Field{
			Type:    deletePayload,
			Args:    args,
			Resolve: r.makeDeleteResolver(table, pkCols),
		}
	}

	return fields
}

func (r *Resolver) mutationInsertableColumns(table introspection.Table) []introspection.Column {
	cols := make([]introspection.Column, 0, len(table.Columns))
	for _, col := range table.Columns {
		if col.IsGenerated {
			continue
		}
		if !schemafilter.MutationColumnAllowed(table.Name, col.Name, r.filters) {
			continue
		}
		cols = append(cols, col)
	}
	return cols
}

func (r *Resolver) mutationUpdatableColumns(table introspection.Table) []introspection.Column {
	cols := make([]introspection.Column, 0, len(table.Columns))
	for _, col := range table.Columns {
		if col.IsPrimaryKey || col.IsGenerated {
			continue
		}
		if !schemafilter.MutationColumnAllowed(table.Name, col.Name, r.filters) {
			continue
		}
		cols = append(cols, col)
	}
	return cols
}

func (r *Resolver) createInputType(table introspection.Table, columns []introspection.Column) *graphql.InputObject {
	typeName := "Create" + r.singularTypeName(table) + "Input"
	r.mu.RLock()
	cached, ok := r.createInputCache[typeName]
	r.mu.RUnlock()
	if ok {
		return cached
	}

	fields := graphql.InputObjectConfigFieldMap{}
	for _, col := range columns {
		fieldType := r.mapColumnTypeToGraphQLInput(&col)
		if isRequiredInsertColumn(col) {
			fieldType = graphql.NewNonNull(fieldType)
		}
		fields[introspection.GraphQLFieldName(col)] = &graphql.InputObjectFieldConfig{
			Type: fieldType,
		}
	}

	objType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name:   typeName,
		Fields: fields,
	})

	r.mu.Lock()
	if cached, ok := r.createInputCache[typeName]; ok {
		r.mu.Unlock()
		return cached
	}
	r.createInputCache[typeName] = objType
	r.mu.Unlock()

	return objType
}

func (r *Resolver) updateSetInputType(table introspection.Table, columns []introspection.Column) *graphql.InputObject {
	typeName := "Update" + r.singularTypeName(table) + "SetInput"
	r.mu.RLock()
	cached, ok := r.updateInputCache[typeName]
	r.mu.RUnlock()
	if ok {
		return cached
	}

	fields := graphql.InputObjectConfigFieldMap{}
	for _, col := range columns {
		fieldType := r.mapColumnTypeToGraphQLInput(&col)
		fields[introspection.GraphQLFieldName(col)] = &graphql.InputObjectFieldConfig{
			Type: fieldType,
		}
	}

	objType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name:   typeName,
		Fields: fields,
	})

	r.mu.Lock()
	if cached, ok := r.updateInputCache[typeName]; ok {
		r.mu.Unlock()
		return cached
	}
	r.updateInputCache[typeName] = objType
	r.mu.Unlock()

	return objType
}

func (r *Resolver) deletePayloadType(table introspection.Table, pkCols []introspection.Column) *graphql.Object {
	typeName := "Delete" + r.singularTypeName(table) + "Payload"
	r.mu.RLock()
	cached, ok := r.deletePayloadCache[typeName]
	r.mu.RUnlock()
	if ok {
		return cached
	}

	fields := graphql.Fields{}
	for _, col := range pkCols {
		fieldType := r.mapColumnTypeToGraphQL(&col)
		if !col.IsNullable {
			fieldType = graphql.NewNonNull(fieldType)
		}
		fields[introspection.GraphQLFieldName(col)] = &graphql.Field{
			Type: fieldType,
		}
	}

	objType := graphql.NewObject(graphql.ObjectConfig{
		Name:   typeName,
		Fields: fields,
	})

	r.mu.Lock()
	if cached, ok := r.deletePayloadCache[typeName]; ok {
		r.mu.Unlock()
		return cached
	}
	r.deletePayloadCache[typeName] = objType
	r.mu.Unlock()

	return objType
}

func (r *Resolver) primaryKeyArgs(pkCols []introspection.Column) graphql.FieldConfigArgument {
	args := graphql.FieldConfigArgument{}
	for i := range pkCols {
		col := &pkCols[i]
		argType := r.mapColumnTypeToGraphQLInput(col)
		args[introspection.GraphQLFieldName(*col)] = &graphql.ArgumentConfig{
			Type: graphql.NewNonNull(argType),
		}
	}
	return args
}

func (r *Resolver) makeCreateResolver(table introspection.Table, insertable map[string]bool) graphql.FieldResolveFn {
	return withMutationContext(func(p graphql.ResolveParams, mc *MutationContext) (interface{}, error) {

		inputArg, ok := p.Args["input"].(map[string]interface{})
		if !ok {
			return nil, newMutationError("invalid input", "invalid_input", 0)
		}

		columns, values, err := mapInputColumns(table, inputArg, insertable)
		if err != nil {
			return nil, err
		}
		if len(inputArg) > 0 && len(columns) == 0 {
			return nil, newMutationError("no insertable columns in input", "invalid_input", 0)
		}

		query, err := planner.PlanInsert(table, columns, values)
		if err != nil {
			return nil, err
		}

		result, err := mc.Tx().ExecContext(p.Context, query.SQL, query.Args...)
		if err != nil {
			return nil, normalizeMutationError(err)
		}

		pkCols := introspection.PrimaryKeyColumns(table)
		if len(pkCols) == 0 {
			return nil, nil
		}
		pkValues, err := resolveInsertPKValues(table, pkCols, inputArg, result)
		if err != nil {
			return nil, err
		}

		row, err := r.selectRowByPK(p, table, pkCols, pkValues, mc.Tx())
		if err != nil {
			return nil, err
		}
		return row, nil
	})
}

func (r *Resolver) makeUpdateResolver(table introspection.Table, updatable map[string]bool, pkCols []introspection.Column) graphql.FieldResolveFn {
	return withMutationContext(func(p graphql.ResolveParams, mc *MutationContext) (interface{}, error) {

		pkValues, err := pkValuesFromArgs(pkCols, p.Args)
		if err != nil {
			return nil, err
		}

		setArg, hasSet := p.Args["set"]
		if !hasSet || setArg == nil {
			row, err := r.selectRowByPK(p, table, pkCols, pkValues, mc.Tx())
			if err != nil {
				return nil, err
			}
			return row, nil
		}

		setMap, ok := setArg.(map[string]interface{})
		if !ok {
			return nil, newMutationError("invalid set input", "invalid_input", 0)
		}
		if len(setMap) == 0 {
			row, err := r.selectRowByPK(p, table, pkCols, pkValues, mc.Tx())
			if err != nil {
				return nil, err
			}
			return row, nil
		}

		setValues, err := mapSetColumns(table, setMap, updatable)
		if err != nil {
			return nil, err
		}
		if len(setMap) > 0 && len(setValues) == 0 {
			return nil, newMutationError("no updatable columns in set", "invalid_input", 0)
		}

		planned, err := planner.PlanUpdate(table, setValues, pkValues)
		if err != nil {
			return nil, err
		}

		result, err := mc.Tx().ExecContext(p.Context, planned.SQL, planned.Args...)
		if err != nil {
			return nil, normalizeMutationError(err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return nil, err
		}
		if rowsAffected == 0 {
			return nil, nil
		}

		row, err := r.selectRowByPK(p, table, pkCols, pkValues, mc.Tx())
		if err != nil {
			return nil, err
		}
		return row, nil
	})
}

func (r *Resolver) makeDeleteResolver(table introspection.Table, pkCols []introspection.Column) graphql.FieldResolveFn {
	return withMutationContext(func(p graphql.ResolveParams, mc *MutationContext) (interface{}, error) {

		pkValues, err := pkValuesFromArgs(pkCols, p.Args)
		if err != nil {
			return nil, err
		}

		planned, err := planner.PlanDelete(table, pkValues)
		if err != nil {
			return nil, err
		}

		result, err := mc.Tx().ExecContext(p.Context, planned.SQL, planned.Args...)
		if err != nil {
			return nil, normalizeMutationError(err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return nil, err
		}
		if rowsAffected == 0 {
			return nil, nil
		}

		payload := map[string]interface{}{}
		for _, col := range pkCols {
			fieldName := introspection.GraphQLFieldName(col)
			payload[fieldName] = pkValues[col.Name]
		}

		return payload, nil
	})
}

func withMutationContext(fn func(p graphql.ResolveParams, mc *MutationContext) (interface{}, error)) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		mc := MutationContextFromContext(p.Context)
		if mc == nil || mc.Tx() == nil {
			return nil, fmt.Errorf("mutation transaction not available")
		}
		result, err := fn(p, mc)
		if err != nil {
			mc.MarkError()
		}
		return result, err
	}
}

func (r *Resolver) selectRowByPK(p graphql.ResolveParams, table introspection.Table, pkCols []introspection.Column, pkValues map[string]interface{}, tx dbexec.TxExecutor) (map[string]interface{}, error) {
	field := firstFieldAST(p.Info.FieldASTs)
	if field == nil {
		return nil, fmt.Errorf("missing field AST in resolve params")
	}
	selected := planner.SelectedColumns(table, field, p.Info.Fragments)

	var query planner.SQLQuery
	var err error
	if len(pkCols) == 1 {
		pk := &pkCols[0]
		query, err = planner.PlanTableByPK(table, selected, pk, pkValues[pk.Name])
	} else {
		query, err = planner.PlanTableByPKColumns(table, selected, pkCols, pkValues)
	}
	if err != nil {
		return nil, err
	}

	rows, err := tx.QueryContext(p.Context, query.SQL, query.Args...)
	if err != nil {
		return nil, normalizeMutationError(err)
	}
	defer func() {
		_ = rows.Close()
	}()

	results, err := scanRows(rows, selected)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}
	return results[0], nil
}

func mapInputColumns(table introspection.Table, input map[string]interface{}, allowed map[string]bool) ([]string, []interface{}, error) {
	if len(input) == 0 {
		return nil, nil, nil
	}
	columns := make([]string, 0, len(input))
	values := make([]interface{}, 0, len(input))
	if err := collectInputColumns(table, input, allowed, func(col introspection.Column, value interface{}) {
		columns = append(columns, col.Name)
		values = append(values, value)
	}); err != nil {
		return nil, nil, err
	}
	return columns, values, nil
}

func mapSetColumns(table introspection.Table, input map[string]interface{}, allowed map[string]bool) (map[string]interface{}, error) {
	if len(input) == 0 {
		return nil, nil
	}
	setValues := make(map[string]interface{}, len(input))
	if err := collectInputColumns(table, input, allowed, func(col introspection.Column, value interface{}) {
		setValues[col.Name] = value
	}); err != nil {
		return nil, err
	}
	return setValues, nil
}

func collectInputColumns(table introspection.Table, input map[string]interface{}, allowed map[string]bool, handle func(col introspection.Column, value interface{})) error {
	seen := make(map[string]struct{}, len(input))
	for _, col := range table.Columns {
		if !allowed[col.Name] {
			continue
		}
		fieldName := introspection.GraphQLFieldName(col)
		value, ok := input[fieldName]
		if !ok {
			continue
		}
		handle(col, value)
		seen[fieldName] = struct{}{}
	}

	if len(seen) < len(input) {
		for key := range input {
			if _, ok := seen[key]; !ok {
				return newMutationError("unknown or disallowed column: "+key, "invalid_input", 0)
			}
		}
	}

	return nil
}

func pkValuesFromArgs(pkCols []introspection.Column, args map[string]interface{}) (map[string]interface{}, error) {
	values := make(map[string]interface{}, len(pkCols))
	for _, col := range pkCols {
		fieldName := introspection.GraphQLFieldName(col)
		value, ok := args[fieldName]
		if !ok {
			return nil, fmt.Errorf("missing primary key argument: %s", fieldName)
		}
		values[col.Name] = value
	}
	return values, nil
}

func resolveInsertPKValues(table introspection.Table, pkCols []introspection.Column, input map[string]interface{}, result sql.Result) (map[string]interface{}, error) {
	values := make(map[string]interface{}, len(pkCols))
	var autoCol *introspection.Column

	for i := range pkCols {
		col := &pkCols[i]
		fieldName := introspection.GraphQLFieldName(*col)
		if value, ok := input[fieldName]; ok && value != nil {
			values[col.Name] = value
			continue
		}

		if col.IsAutoIncrement || col.IsAutoRandom {
			if autoCol != nil {
				return nil, fmt.Errorf("multiple auto-generated primary keys on table %s", table.Name)
			}
			autoCol = col
			continue
		}

		return nil, fmt.Errorf("missing value for primary key column %s", col.Name)
	}

	if autoCol != nil {
		lastID, err := result.LastInsertId()
		if err != nil {
			return nil, err
		}
		values[autoCol.Name] = lastID
	}

	return values, nil
}

func isRequiredInsertColumn(col introspection.Column) bool {
	if col.IsGenerated || col.IsNullable || col.HasDefault || col.IsAutoIncrement || col.IsAutoRandom {
		return false
	}
	return true
}

func missingRequiredInsertColumns(table introspection.Table, allowed map[string]bool) bool {
	for _, col := range table.Columns {
		if !isRequiredInsertColumn(col) {
			continue
		}
		if !allowed[col.Name] {
			return true
		}
	}
	return false
}

func columnNameSet(columns []introspection.Column) map[string]bool {
	set := make(map[string]bool, len(columns))
	for _, col := range columns {
		set[col.Name] = true
	}
	return set
}

type mutationError struct {
	message   string
	code      string
	mysqlCode uint16
}

func (e *mutationError) Error() string {
	return e.message
}

func (e *mutationError) Extensions() map[string]interface{} {
	extensions := map[string]interface{}{
		"code": e.code,
	}
	if e.mysqlCode != 0 {
		extensions["mysql_code"] = e.mysqlCode
	}
	return extensions
}

func newMutationError(message, code string, mysqlCode uint16) error {
	return &mutationError{
		message:   message,
		code:      code,
		mysqlCode: mysqlCode,
	}
}

func normalizeMutationError(err error) error {
	if err == nil {
		return nil
	}
	var mysqlErr *mysql.MySQLError
	if !errors.As(err, &mysqlErr) {
		return err
	}

	switch mysqlErr.Number {
	case mysqlErrDBAccessDenied, mysqlErrTableAccessDenied, mysqlErrColumnAccessDenied:
		return newMutationError(mysqlErr.Message, "access_denied", mysqlErr.Number)
	case 1062:
		return newMutationError(mysqlErr.Message, "unique_violation", mysqlErr.Number)
	case 1451, 1452:
		return newMutationError(mysqlErr.Message, "foreign_key_violation", mysqlErr.Number)
	case 1048, 1364:
		return newMutationError(mysqlErr.Message, "not_null_violation", mysqlErr.Number)
	default:
		return err
	}
}
