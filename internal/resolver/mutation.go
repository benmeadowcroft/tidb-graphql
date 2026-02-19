package resolver

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"unicode"

	"github.com/go-sql-driver/mysql"
	"github.com/graphql-go/graphql"
	"go.opentelemetry.io/otel/attribute"

	"tidb-graphql/internal/dbexec"
	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/nodeid"
	"tidb-graphql/internal/planner"
	"tidb-graphql/internal/schemafilter"
	"tidb-graphql/internal/setutil"
	"tidb-graphql/internal/sqltype"
	"tidb-graphql/internal/uuidutil"
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
		createSuccess := r.createSuccessType(table, tableType)
		createResult := r.createResultUnion(table, createSuccess)
		fields["create"+typeName] = &graphql.Field{
			Type: graphql.NewNonNull(createResult),
			Args: graphql.FieldConfigArgument{
				"input": &graphql.ArgumentConfig{
					Type: graphql.NewNonNull(createInput),
				},
			},
			Resolve: r.makeCreateResolver(table, insertableMap, createSuccess),
		}
	}

	updatableCols := r.mutationUpdatableColumns(table)
	updatableMap := columnNameSet(updatableCols)
	if hasPK && len(updatableCols) > 0 {
		updateInput := r.updateSetInputType(table, updatableCols)
		updateSuccess := r.updateSuccessType(table, tableType)
		updateResult := r.updateResultUnion(table, updateSuccess)
		args := r.primaryKeyArgs()
		args["set"] = &graphql.ArgumentConfig{
			Type: updateInput,
		}
		fields["update"+typeName] = &graphql.Field{
			Type:    graphql.NewNonNull(updateResult),
			Args:    args,
			Resolve: r.makeUpdateResolver(table, updatableMap, pkCols, updateSuccess),
		}
	}

	if hasPK {
		deleteSuccess := r.deleteSuccessType(table, pkCols)
		deleteResult := r.deleteResultUnion(table, deleteSuccess)
		args := r.primaryKeyArgs()
		fields["delete"+typeName] = &graphql.Field{
			Type:    graphql.NewNonNull(deleteResult),
			Args:    args,
			Resolve: r.makeDeleteResolver(table, pkCols, deleteSuccess),
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
		fieldType := r.mapColumnTypeToGraphQLInput(table, &col)
		if isRequiredInsertColumn(col) {
			fieldType = graphql.NewNonNull(fieldType)
		}
		fields[introspection.GraphQLFieldName(col)] = &graphql.InputObjectFieldConfig{
			Type:        fieldType,
			Description: col.Comment,
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
		fieldType := r.mapColumnTypeToGraphQLInput(table, &col)
		fields[introspection.GraphQLFieldName(col)] = &graphql.InputObjectFieldConfig{
			Type:        fieldType,
			Description: col.Comment,
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
	fields["id"] = &graphql.Field{
		Type: graphql.NewNonNull(graphql.ID),
	}
	for _, col := range pkCols {
		fieldType := r.mapColumnTypeToGraphQL(table, &col)
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

func (r *Resolver) sharedMutationErrorInterface() *graphql.Interface {
	r.mu.RLock()
	cached := r.mutationErrorInterface
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	iface := graphql.NewInterface(graphql.InterfaceConfig{
		Name: "MutationError",
		Fields: graphql.Fields{
			"message": &graphql.Field{
				Type: graphql.NewNonNull(graphql.String),
			},
		},
	})

	r.mu.Lock()
	if r.mutationErrorInterface == nil {
		r.mutationErrorInterface = iface
	}
	cached = r.mutationErrorInterface
	r.mu.Unlock()
	return cached
}

func (r *Resolver) sharedValidationErrorType() *graphql.Object {
	r.mu.RLock()
	cached := r.validationErrorType
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	obj := graphql.NewObject(graphql.ObjectConfig{
		Name: "InputValidationError",
		Fields: graphql.Fields{
			"message": &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
			"field":   &graphql.Field{Type: graphql.String},
		},
		Interfaces: []*graphql.Interface{r.sharedMutationErrorInterface()},
	})

	r.mu.Lock()
	if r.validationErrorType == nil {
		r.validationErrorType = obj
	}
	cached = r.validationErrorType
	r.mu.Unlock()
	return cached
}

func (r *Resolver) sharedConflictErrorType() *graphql.Object {
	r.mu.RLock()
	cached := r.conflictErrorType
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	obj := graphql.NewObject(graphql.ObjectConfig{
		Name: "ConflictError",
		Fields: graphql.Fields{
			"message":          &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
			"conflictingField": &graphql.Field{Type: graphql.String},
		},
		Interfaces: []*graphql.Interface{r.sharedMutationErrorInterface()},
	})

	r.mu.Lock()
	if r.conflictErrorType == nil {
		r.conflictErrorType = obj
	}
	cached = r.conflictErrorType
	r.mu.Unlock()
	return cached
}

func (r *Resolver) sharedConstraintErrorType() *graphql.Object {
	r.mu.RLock()
	cached := r.constraintErrorType
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	obj := graphql.NewObject(graphql.ObjectConfig{
		Name: "ConstraintError",
		Fields: graphql.Fields{
			"message": &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
		},
		Interfaces: []*graphql.Interface{r.sharedMutationErrorInterface()},
	})

	r.mu.Lock()
	if r.constraintErrorType == nil {
		r.constraintErrorType = obj
	}
	cached = r.constraintErrorType
	r.mu.Unlock()
	return cached
}

func (r *Resolver) sharedPermissionErrorType() *graphql.Object {
	r.mu.RLock()
	cached := r.permissionErrorType
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	obj := graphql.NewObject(graphql.ObjectConfig{
		Name: "PermissionError",
		Fields: graphql.Fields{
			"message": &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
		},
		Interfaces: []*graphql.Interface{r.sharedMutationErrorInterface()},
	})

	r.mu.Lock()
	if r.permissionErrorType == nil {
		r.permissionErrorType = obj
	}
	cached = r.permissionErrorType
	r.mu.Unlock()
	return cached
}

func (r *Resolver) sharedNotFoundErrorType() *graphql.Object {
	r.mu.RLock()
	cached := r.notFoundErrorType
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	obj := graphql.NewObject(graphql.ObjectConfig{
		Name: "NotFoundError",
		Fields: graphql.Fields{
			"message": &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
		},
		Interfaces: []*graphql.Interface{r.sharedMutationErrorInterface()},
	})

	r.mu.Lock()
	if r.notFoundErrorType == nil {
		r.notFoundErrorType = obj
	}
	cached = r.notFoundErrorType
	r.mu.Unlock()
	return cached
}

func (r *Resolver) sharedInternalErrorType() *graphql.Object {
	r.mu.RLock()
	cached := r.internalErrorType
	r.mu.RUnlock()
	if cached != nil {
		return cached
	}

	obj := graphql.NewObject(graphql.ObjectConfig{
		Name: "InternalError",
		Fields: graphql.Fields{
			"message": &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
		},
		Interfaces: []*graphql.Interface{r.sharedMutationErrorInterface()},
	})

	r.mu.Lock()
	if r.internalErrorType == nil {
		r.internalErrorType = obj
	}
	cached = r.internalErrorType
	r.mu.Unlock()
	return cached
}

func (r *Resolver) mutationEntityFieldName(table introspection.Table) string {
	return lowerFirst(r.singularTypeName(table))
}

func lowerFirst(s string) string {
	if s == "" {
		return s
	}
	runes := []rune(s)
	runes[0] = unicode.ToLower(runes[0])
	return string(runes)
}

func (r *Resolver) createSuccessType(table introspection.Table, tableType *graphql.Object) *graphql.Object {
	typeName := "Create" + r.singularTypeName(table) + "Success"
	r.mu.RLock()
	cached, ok := r.createSuccessCache[typeName]
	r.mu.RUnlock()
	if ok {
		return cached
	}

	fieldName := r.mutationEntityFieldName(table)
	objType := graphql.NewObject(graphql.ObjectConfig{
		Name: typeName,
		Fields: graphql.Fields{
			fieldName: &graphql.Field{Type: graphql.NewNonNull(tableType)},
		},
	})

	r.mu.Lock()
	if cached, ok := r.createSuccessCache[typeName]; ok {
		r.mu.Unlock()
		return cached
	}
	r.createSuccessCache[typeName] = objType
	r.mu.Unlock()
	return objType
}

func (r *Resolver) updateSuccessType(table introspection.Table, tableType *graphql.Object) *graphql.Object {
	typeName := "Update" + r.singularTypeName(table) + "Success"
	r.mu.RLock()
	cached, ok := r.updateSuccessCache[typeName]
	r.mu.RUnlock()
	if ok {
		return cached
	}

	fieldName := r.mutationEntityFieldName(table)
	objType := graphql.NewObject(graphql.ObjectConfig{
		Name: typeName,
		Fields: graphql.Fields{
			fieldName: &graphql.Field{Type: tableType},
		},
	})

	r.mu.Lock()
	if cached, ok := r.updateSuccessCache[typeName]; ok {
		r.mu.Unlock()
		return cached
	}
	r.updateSuccessCache[typeName] = objType
	r.mu.Unlock()
	return objType
}

func (r *Resolver) deleteSuccessType(table introspection.Table, pkCols []introspection.Column) *graphql.Object {
	typeName := "Delete" + r.singularTypeName(table) + "Success"
	r.mu.RLock()
	cached, ok := r.deleteSuccessCache[typeName]
	r.mu.RUnlock()
	if ok {
		return cached
	}

	fields := graphql.Fields{
		"id": &graphql.Field{
			Type: graphql.NewNonNull(graphql.ID),
		},
	}
	for _, col := range pkCols {
		fieldType := r.mapColumnTypeToGraphQL(table, &col)
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
	if cached, ok := r.deleteSuccessCache[typeName]; ok {
		r.mu.Unlock()
		return cached
	}
	r.deleteSuccessCache[typeName] = objType
	r.mu.Unlock()
	return objType
}

func (r *Resolver) createResultUnion(table introspection.Table, successType *graphql.Object) *graphql.Union {
	typeName := "Create" + r.singularTypeName(table) + "Result"
	r.mu.RLock()
	cached, ok := r.createResultCache[typeName]
	r.mu.RUnlock()
	if ok {
		return cached
	}

	union := graphql.NewUnion(graphql.UnionConfig{
		Name: typeName,
		Types: []*graphql.Object{
			successType,
			r.sharedValidationErrorType(),
			r.sharedConflictErrorType(),
			r.sharedConstraintErrorType(),
			r.sharedPermissionErrorType(),
			r.sharedInternalErrorType(),
		},
		ResolveType: r.mutationResolveType(successType),
	})

	r.mu.Lock()
	if cached, ok := r.createResultCache[typeName]; ok {
		r.mu.Unlock()
		return cached
	}
	r.createResultCache[typeName] = union
	r.mu.Unlock()
	return union
}

func (r *Resolver) updateResultUnion(table introspection.Table, successType *graphql.Object) *graphql.Union {
	typeName := "Update" + r.singularTypeName(table) + "Result"
	r.mu.RLock()
	cached, ok := r.updateResultCache[typeName]
	r.mu.RUnlock()
	if ok {
		return cached
	}

	union := graphql.NewUnion(graphql.UnionConfig{
		Name: typeName,
		Types: []*graphql.Object{
			successType,
			r.sharedValidationErrorType(),
			r.sharedConflictErrorType(),
			r.sharedConstraintErrorType(),
			r.sharedPermissionErrorType(),
			r.sharedInternalErrorType(),
		},
		ResolveType: r.mutationResolveType(successType),
	})

	r.mu.Lock()
	if cached, ok := r.updateResultCache[typeName]; ok {
		r.mu.Unlock()
		return cached
	}
	r.updateResultCache[typeName] = union
	r.mu.Unlock()
	return union
}

func (r *Resolver) deleteResultUnion(table introspection.Table, successType *graphql.Object) *graphql.Union {
	typeName := "Delete" + r.singularTypeName(table) + "Result"
	r.mu.RLock()
	cached, ok := r.deleteResultCache[typeName]
	r.mu.RUnlock()
	if ok {
		return cached
	}

	union := graphql.NewUnion(graphql.UnionConfig{
		Name: typeName,
		Types: []*graphql.Object{
			successType,
			r.sharedValidationErrorType(),
			r.sharedNotFoundErrorType(),
			r.sharedConstraintErrorType(),
			r.sharedPermissionErrorType(),
			r.sharedInternalErrorType(),
		},
		ResolveType: r.mutationResolveType(successType),
	})

	r.mu.Lock()
	if cached, ok := r.deleteResultCache[typeName]; ok {
		r.mu.Unlock()
		return cached
	}
	r.deleteResultCache[typeName] = union
	r.mu.Unlock()
	return union
}

func (r *Resolver) mutationResolveType(successType *graphql.Object) graphql.ResolveTypeFn {
	return func(p graphql.ResolveTypeParams) *graphql.Object {
		m, ok := p.Value.(map[string]interface{})
		if !ok {
			return successType
		}
		typename, _ := m["__typename"].(string)
		switch typename {
		case "InputValidationError":
			return r.sharedValidationErrorType()
		case "ConflictError":
			return r.sharedConflictErrorType()
		case "ConstraintError":
			return r.sharedConstraintErrorType()
		case "PermissionError":
			return r.sharedPermissionErrorType()
		case "NotFoundError":
			return r.sharedNotFoundErrorType()
		case "InternalError":
			return r.sharedInternalErrorType()
		default:
			return successType
		}
	}
}

func (r *Resolver) primaryKeyArgs() graphql.FieldConfigArgument {
	return graphql.FieldConfigArgument{
		"id": &graphql.ArgumentConfig{
			Type: graphql.NewNonNull(graphql.ID),
		},
	}
}

const (
	mutationResultClassSuccess        = "success"
	mutationResultClassTypedFailure   = "typed_failure"
	mutationResultClassExecutionError = "execution_error"

	mutationResultCodeSuccess             = "success"
	mutationResultCodeInvalidInput        = "invalid_input"
	mutationResultCodeUniqueViolation     = "unique_violation"
	mutationResultCodeForeignKeyViolation = "foreign_key_violation"
	mutationResultCodeNotNullViolation    = "not_null_violation"
	mutationResultCodeAccessDenied        = "access_denied"
	mutationResultCodeNotFound            = "not_found"
	mutationResultCodeInternal            = "internal"
	mutationResultCodeUnknown             = "unknown"

	mutationResolverOutcomeTypedFailure = "typed_failure"
)

type mutationResultTelemetry struct {
	typename string
	class    string
	code     string
	outcome  string
}

func mutationSuccessTelemetry(typename string) mutationResultTelemetry {
	return mutationResultTelemetry{
		typename: typename,
		class:    mutationResultClassSuccess,
		code:     mutationResultCodeSuccess,
		outcome:  "success",
	}
}

func mutationTypedFailureTelemetry(typename, code string) mutationResultTelemetry {
	return mutationResultTelemetry{
		typename: typename,
		class:    mutationResultClassTypedFailure,
		code:     code,
		outcome:  mutationResolverOutcomeTypedFailure,
	}
}

func mutationExecutionErrorTelemetry() mutationResultTelemetry {
	return mutationResultTelemetry{
		typename: "InternalError",
		class:    mutationResultClassExecutionError,
		code:     mutationResultCodeUnknown,
		outcome:  "error",
	}
}

func mutationErrorPayload(typename, message string, extra map[string]interface{}) map[string]interface{} {
	payload := map[string]interface{}{
		"__typename": typename,
		"message":    message,
	}
	for k, v := range extra {
		payload[k] = v
	}
	return payload
}

func mutationErrToPayload(err error) map[string]interface{} {
	payload, _ := mutationErrToPayloadAndTelemetry(err)
	return payload
}

func mutationErrToPayloadAndTelemetry(err error) (map[string]interface{}, mutationResultTelemetry) {
	var me *mutationError
	if !errors.As(err, &me) {
		return mutationErrorPayload("InternalError", "internal server error", nil), mutationExecutionErrorTelemetry()
	}
	switch me.code {
	case "invalid_input":
		return mutationErrorPayload("InputValidationError", me.message, nil), mutationTypedFailureTelemetry("InputValidationError", mutationResultCodeInvalidInput)
	case "unique_violation":
		return mutationErrorPayload("ConflictError", me.message, nil), mutationTypedFailureTelemetry("ConflictError", mutationResultCodeUniqueViolation)
	case "foreign_key_violation":
		return mutationErrorPayload("ConstraintError", me.message, nil), mutationTypedFailureTelemetry("ConstraintError", mutationResultCodeForeignKeyViolation)
	case "not_null_violation":
		return mutationErrorPayload("ConstraintError", me.message, nil), mutationTypedFailureTelemetry("ConstraintError", mutationResultCodeNotNullViolation)
	case "access_denied":
		return mutationErrorPayload("PermissionError", me.message, nil), mutationTypedFailureTelemetry("PermissionError", mutationResultCodeAccessDenied)
	default:
		return mutationErrorPayload("InternalError", "internal server error", nil), mutationTypedFailureTelemetry("InternalError", mutationResultCodeInternal)
	}
}

func withMutationContextUnion(fn func(p graphql.ResolveParams, mc *MutationContext) (interface{}, error)) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		mc := MutationContextFromContext(p.Context)
		if mc == nil || mc.Tx() == nil {
			return mutationErrorPayload("InternalError", "mutation transaction not available", nil), nil
		}
		result, err := fn(p, mc)
		if err != nil {
			mc.MarkError()
			return mutationErrToPayload(err), nil
		}
		return result, nil
	}
}

func (r *Resolver) makeCreateResolver(table introspection.Table, insertable map[string]bool, successType *graphql.Object) graphql.FieldResolveFn {
	return withMutationContextUnion(func(p graphql.ResolveParams, mc *MutationContext) (result interface{}, err error) {
		resultTelemetry := mutationSuccessTelemetry("Create" + r.singularTypeName(table) + "Success")
		if successType != nil && successType.Name() != "" {
			resultTelemetry = mutationSuccessTelemetry(successType.Name())
		}
		ctx, span := startResolverSpan(p.Context, "graphql.mutation.create",
			attribute.String("db.table", table.Name),
			attribute.String("graphql.field.name", p.Info.FieldName),
		)
		p.Context = ctx
		defer func() {
			finishErr := err
			if err != nil {
				_, errTelemetry := mutationErrToPayloadAndTelemetry(err)
				resultTelemetry = errTelemetry
				if resultTelemetry.class == mutationResultClassTypedFailure {
					finishErr = nil
				}
			}
			setMutationResultAttributes(span, resultTelemetry.typename, resultTelemetry.class, resultTelemetry.code)
			finishResolverSpan(span, finishErr, resultTelemetry.outcome)
			span.End()
		}()

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

		execResult, err := mc.Tx().ExecContext(p.Context, query.SQL, query.Args...)
		if err != nil {
			return nil, normalizeMutationError(err)
		}

		pkCols := introspection.PrimaryKeyColumns(table)
		if len(pkCols) == 0 {
			return nil, nil
		}
		pkValues, err := resolveInsertPKValues(table, pkCols, inputArg, execResult)
		if err != nil {
			return nil, err
		}

		row, err := r.selectRowByPK(p, table, pkCols, pkValues, mc.Tx())
		if err != nil {
			return nil, err
		}
		if row == nil {
			return nil, fmt.Errorf("created row could not be loaded")
		}
		return map[string]interface{}{
			r.mutationEntityFieldName(table): row,
		}, nil
	})
}

func (r *Resolver) makeUpdateResolver(table introspection.Table, updatable map[string]bool, pkCols []introspection.Column, successType *graphql.Object) graphql.FieldResolveFn {
	return withMutationContextUnion(func(p graphql.ResolveParams, mc *MutationContext) (result interface{}, err error) {
		resultTelemetry := mutationSuccessTelemetry("Update" + r.singularTypeName(table) + "Success")
		if successType != nil && successType.Name() != "" {
			resultTelemetry = mutationSuccessTelemetry(successType.Name())
		}
		ctx, span := startResolverSpan(p.Context, "graphql.mutation.update",
			attribute.String("db.table", table.Name),
			attribute.String("graphql.field.name", p.Info.FieldName),
		)
		p.Context = ctx
		defer func() {
			finishErr := err
			if err != nil {
				_, errTelemetry := mutationErrToPayloadAndTelemetry(err)
				resultTelemetry = errTelemetry
				if resultTelemetry.class == mutationResultClassTypedFailure {
					finishErr = nil
				}
			}
			setMutationResultAttributes(span, resultTelemetry.typename, resultTelemetry.class, resultTelemetry.code)
			finishResolverSpan(span, finishErr, resultTelemetry.outcome)
			span.End()
		}()

		entityFieldName := r.mutationEntityFieldName(table)

		pkValues, err := pkValuesFromArgs(table, pkCols, p.Args)
		if err != nil {
			return nil, err
		}

		setArg, hasSet := p.Args["set"]
		if !hasSet || setArg == nil {
			row, err := r.selectRowByPK(p, table, pkCols, pkValues, mc.Tx())
			if err != nil {
				return nil, err
			}
			return map[string]interface{}{
				entityFieldName: row,
			}, nil
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
			return map[string]interface{}{
				entityFieldName: row,
			}, nil
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

		execResult, err := mc.Tx().ExecContext(p.Context, planned.SQL, planned.Args...)
		if err != nil {
			return nil, normalizeMutationError(err)
		}

		rowsAffected, err := execResult.RowsAffected()
		if err != nil {
			return nil, err
		}
		if rowsAffected == 0 {
			return map[string]interface{}{
				entityFieldName: nil,
			}, nil
		}

		row, err := r.selectRowByPK(p, table, pkCols, pkValues, mc.Tx())
		if err != nil {
			return nil, err
		}
		return map[string]interface{}{
			entityFieldName: row,
		}, nil
	})
}

func (r *Resolver) makeDeleteResolver(table introspection.Table, pkCols []introspection.Column, successType *graphql.Object) graphql.FieldResolveFn {
	return withMutationContextUnion(func(p graphql.ResolveParams, mc *MutationContext) (result interface{}, err error) {
		resultTelemetry := mutationSuccessTelemetry("Delete" + r.singularTypeName(table) + "Success")
		if successType != nil && successType.Name() != "" {
			resultTelemetry = mutationSuccessTelemetry(successType.Name())
		}
		ctx, span := startResolverSpan(p.Context, "graphql.mutation.delete",
			attribute.String("db.table", table.Name),
			attribute.String("graphql.field.name", p.Info.FieldName),
		)
		p.Context = ctx
		defer func() {
			finishErr := err
			if err != nil {
				_, errTelemetry := mutationErrToPayloadAndTelemetry(err)
				resultTelemetry = errTelemetry
				if resultTelemetry.class == mutationResultClassTypedFailure {
					finishErr = nil
				}
			}
			setMutationResultAttributes(span, resultTelemetry.typename, resultTelemetry.class, resultTelemetry.code)
			finishResolverSpan(span, finishErr, resultTelemetry.outcome)
			span.End()
		}()

		pkValues, err := pkValuesFromArgs(table, pkCols, p.Args)
		if err != nil {
			return nil, err
		}

		planned, err := planner.PlanDelete(table, pkValues)
		if err != nil {
			return nil, err
		}

		execResult, err := mc.Tx().ExecContext(p.Context, planned.SQL, planned.Args...)
		if err != nil {
			return nil, normalizeMutationError(err)
		}

		rowsAffected, err := execResult.RowsAffected()
		if err != nil {
			return nil, err
		}
		if rowsAffected == 0 {
			resultTelemetry = mutationTypedFailureTelemetry("NotFoundError", mutationResultCodeNotFound)
			return mutationErrorPayload("NotFoundError", "row not found", nil), nil
		}

		payload := map[string]interface{}{}
		payload["id"] = encodeNodeID(table, pkCols, pkValues)
		for _, col := range pkCols {
			fieldName := introspection.GraphQLFieldName(col)
			payload[fieldName] = pkValues[col.Name]
		}

		return payload, nil
	})
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
		normalized, err := normalizeMutationInputValue(col, value)
		if err != nil {
			return err
		}
		handle(col, normalized)
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

func normalizeMutationInputValue(col introspection.Column, value interface{}) (interface{}, error) {
	switch introspection.EffectiveGraphQLType(col) {
	case sqltype.TypeSet:
		return normalizeSetInputValue(col, value)
	case sqltype.TypeUUID:
		return normalizeUUIDInputValue(col, value)
	case sqltype.TypeVector:
		return normalizeVectorInputValue(col, value)
	default:
		return value, nil
	}
}

func normalizeSetInputValue(col introspection.Column, value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	csv, err := setutil.CanonicalizeAny(value, col.EnumValues)
	if err != nil {
		return nil, newMutationError(err.Error(), "invalid_input", 0)
	}
	return csv, nil
}

func normalizeUUIDInputValue(col introspection.Column, value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	raw, ok := value.(string)
	if !ok {
		return nil, newMutationError("uuid value must be a string", "invalid_input", 0)
	}

	parsed, canonical, err := uuidutil.ParseString(raw)
	if err != nil {
		return nil, newMutationError(err.Error(), "invalid_input", 0)
	}

	if uuidutil.IsBinaryStorageType(col.DataType) {
		return uuidutil.ToBytes(parsed), nil
	}
	return canonical, nil
}

func normalizeVectorInputValue(col introspection.Column, value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	vector, err := parseMutationVectorValues(value)
	if err != nil {
		return nil, newMutationError(err.Error(), "invalid_input", 0)
	}
	if col.VectorDimension > 0 && len(vector) != col.VectorDimension {
		return nil, newMutationError(
			fmt.Sprintf("vector length %d does not match %s dimension %d", len(vector), introspection.GraphQLFieldName(col), col.VectorDimension),
			"invalid_input",
			0,
		)
	}

	encoded, err := json.Marshal(vector)
	if err != nil {
		return nil, newMutationError("failed to encode vector value", "invalid_input", 0)
	}
	return string(encoded), nil
}

func parseMutationVectorValues(value interface{}) ([]float64, error) {
	switch v := value.(type) {
	case []float64:
		return validateFiniteMutationVector(v)
	case []float32:
		out := make([]float64, len(v))
		for i, n := range v {
			out[i] = float64(n)
		}
		return validateFiniteMutationVector(out)
	case []interface{}:
		out := make([]float64, len(v))
		for i, raw := range v {
			n, err := parseMutationVectorNumber(raw)
			if err != nil {
				return nil, err
			}
			out[i] = n
		}
		return validateFiniteMutationVector(out)
	default:
		return nil, fmt.Errorf("vector must be a list of numbers")
	}
}

func validateFiniteMutationVector(values []float64) ([]float64, error) {
	out := make([]float64, len(values))
	copy(out, values)
	for _, n := range out {
		if math.IsNaN(n) || math.IsInf(n, 0) {
			return nil, fmt.Errorf("vector values must be finite numbers")
		}
	}
	return out, nil
}

func parseMutationVectorNumber(value interface{}) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case string:
		n, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, fmt.Errorf("vector values must be numeric")
		}
		return n, nil
	default:
		return 0, fmt.Errorf("vector values must be numeric")
	}
}

func pkValuesFromArgs(table introspection.Table, pkCols []introspection.Column, args map[string]interface{}) (map[string]interface{}, error) {
	rawID, ok := args["id"]
	if !ok || rawID == nil {
		return nil, newMutationError("missing primary key argument: id", "invalid_input", 0)
	}
	id, ok := rawID.(string)
	if !ok {
		id = fmt.Sprint(rawID)
	}
	typeName, values, err := nodeid.Decode(id)
	if err != nil {
		return nil, newMutationError(err.Error(), "invalid_input", 0)
	}
	expectedType := introspection.GraphQLTypeName(table)
	if typeName != expectedType {
		return nil, newMutationError("invalid id for "+expectedType, "invalid_input", 0)
	}
	if len(values) != len(pkCols) {
		return nil, newMutationError("invalid id for "+expectedType, "invalid_input", 0)
	}
	pkValues := make(map[string]interface{}, len(pkCols))
	for i, col := range pkCols {
		parsed, err := nodeid.ParsePKValue(col, values[i])
		if err != nil {
			return nil, newMutationError(err.Error(), "invalid_input", 0)
		}
		pkValues[col.Name] = parsed
	}
	return pkValues, nil
}

func encodeNodeID(table introspection.Table, pkCols []introspection.Column, pkValues map[string]interface{}) string {
	values := make([]interface{}, len(pkCols))
	for i, col := range pkCols {
		values[i] = pkValues[col.Name]
	}
	return nodeid.Encode(introspection.GraphQLTypeName(table), values...)
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
