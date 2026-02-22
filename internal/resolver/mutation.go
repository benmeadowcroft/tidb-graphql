package resolver

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
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
	connectSatisfiable := r.collectConnectSatisfiableFKs(table)
	if hasPK && len(insertableCols) > 0 && !missingRequiredInsertColumnsWithConnect(table, insertableMap, connectSatisfiable) {
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

	// Pre-compute relationship wiring once for this create input shape.
	plan := r.buildCreateMutationPlan(table)

	// Determine which FK columns have a usable connect field so they can be made optional.
	connectSatisfiable := r.collectConnectSatisfiableFKs(table)

	fields := graphql.InputObjectConfigFieldMap{}
	for _, col := range columns {
		fieldType := r.mapColumnTypeToGraphQLInput(table, &col)
		// A required FK column becomes optional when a connect field can supply its value.
		required := isRequiredInsertColumn(col) && !connectSatisfiable[col.Name]
		if required {
			fieldType = graphql.NewNonNull(fieldType)
		}
		fields[introspection.GraphQLFieldName(col)] = &graphql.InputObjectFieldConfig{
			Type:        fieldType,
			Description: col.Comment,
		}
	}

	// Connect fields for many-to-one relationships.
	for fieldName, rel := range plan.connectFields {
		connectInput := r.connectInputForRel(rel)
		if connectInput == nil {
			continue
		}
		if _, exists := fields[fieldName]; exists {
			continue
		}
		fields[fieldName] = &graphql.InputObjectFieldConfig{
			Type:        connectInput,
			Description: "Connect to an existing " + rel.GraphQLFieldName + " by id or unique field.",
		}
	}

	// Nested create fields for one-to-many and edge-list (attribute junction) relationships.
	for fieldName, rel := range plan.nestedFields {
		nestedInput := r.nestedCreateInputForRel(table, rel)
		if nestedInput == nil {
			continue
		}
		if _, exists := fields[fieldName]; exists {
			continue
		}
		fields[fieldName] = &graphql.InputObjectFieldConfig{
			Type:        graphql.NewList(graphql.NewNonNull(nestedInput)),
			Description: "Inline-create " + rel.GraphQLFieldName + " within this mutation.",
		}
	}

	// Connect fields for pure many-to-many relationships (junction inserts).
	for fieldName, rel := range plan.m2mFields {
		connectInput := r.connectInputForRel(rel)
		if connectInput == nil {
			continue
		}
		if _, exists := fields[fieldName]; exists {
			continue
		}
		fields[fieldName] = &graphql.InputObjectFieldConfig{
			Type:        graphql.NewList(graphql.NewNonNull(connectInput)),
			Description: "Connect existing " + rel.GraphQLFieldName + " within this mutation.",
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

type createMutationPlan struct {
	connectFields map[string]introspection.Relationship
	nestedFields  map[string]introspection.Relationship
	m2mFields     map[string]introspection.Relationship
}

func (r *Resolver) buildCreateMutationPlan(table introspection.Table) createMutationPlan {
	plan := createMutationPlan{
		connectFields: make(map[string]introspection.Relationship),
		nestedFields:  make(map[string]introspection.Relationship),
		m2mFields:     make(map[string]introspection.Relationship),
	}
	for _, rel := range table.Relationships {
		switch {
		case rel.IsManyToOne:
			if !r.localColumnsMutationAllowed(table, rel.LocalColumns, nil) {
				continue
			}
			if r.connectInputForRel(rel) == nil {
				continue
			}
			plan.connectFields[rel.GraphQLFieldName+"Connect"] = rel
		case rel.IsOneToMany || rel.IsEdgeList:
			if r.nestedCreateInputForRel(table, rel) == nil {
				continue
			}
			plan.nestedFields[rel.GraphQLFieldName+"Create"] = rel
		case rel.IsManyToMany:
			if !r.m2mConnectSupported(table, rel) {
				continue
			}
			plan.m2mFields[rel.GraphQLFieldName+"Connect"] = rel
		}
	}
	return plan
}

// connectByUniqueInputTypeName returns the GraphQL type name for a connect-by-unique-index input.
// e.g. "ConnectUserByEmailInput" for the unique email index on users.
func (r *Resolver) connectByUniqueInputTypeName(remoteTable introspection.Table, idx introspection.Index) string {
	return "Connect" + r.singularTypeName(remoteTable) + upperFirst(r.connectFieldKey(remoteTable, idx)) + "Input"
}

// connectByUniqueFieldName returns the GraphQL field name used inside ConnectXxxInput
// for a specific unique index, e.g. "byEmail".
func (r *Resolver) connectByUniqueFieldName(remoteTable introspection.Table, idx introspection.Index) string {
	return r.connectFieldKey(remoteTable, idx)
}

func (r *Resolver) connectFieldKey(remoteTable introspection.Table, idx introspection.Index) string {
	colMap := columnMap(remoteTable)
	var sb strings.Builder
	sb.WriteString("by")
	for _, colName := range idx.Columns {
		if col, ok := colMap[colName]; ok {
			sb.WriteString(upperFirst(introspection.GraphQLFieldName(*col)))
		}
	}
	return sb.String()
}

// connectByUniqueInput builds the input object for a single unique-index connect strategy.
// e.g. ConnectUserByEmailInput { email: String! }
func (r *Resolver) connectByUniqueInput(remoteTable introspection.Table, idx introspection.Index) *graphql.InputObject {
	typeName := r.connectByUniqueInputTypeName(remoteTable, idx)
	r.mu.RLock()
	cached, ok := r.connectInputCache[typeName]
	r.mu.RUnlock()
	if ok {
		return cached
	}

	colMap := columnMap(remoteTable)
	fields := graphql.InputObjectConfigFieldMap{}
	for _, colName := range idx.Columns {
		col, ok := colMap[colName]
		if !ok {
			continue
		}
		fieldType := r.mapColumnTypeToGraphQLInput(remoteTable, col)
		fields[introspection.GraphQLFieldName(*col)] = &graphql.InputObjectFieldConfig{
			Type: graphql.NewNonNull(fieldType),
		}
	}
	if len(fields) == 0 {
		return nil
	}

	obj := graphql.NewInputObject(graphql.InputObjectConfig{
		Name:   typeName,
		Fields: fields,
	})

	r.mu.Lock()
	if c, ok := r.connectInputCache[typeName]; ok {
		r.mu.Unlock()
		return c
	}
	r.connectInputCache[typeName] = obj
	r.mu.Unlock()
	return obj
}

// connectInputForRel builds ConnectXxxInput for a many-to-one relationship.
// The input accepts either id (node ID) or one of the unique-index sub-objects.
// Returns nil if the remote table has no eligible connect strategies.
func (r *Resolver) connectInputForRel(rel introspection.Relationship) *graphql.InputObject {
	remoteTable, err := r.findTable(rel.RemoteTable)
	if err != nil {
		return nil
	}

	typeName := "Connect" + r.singularTypeName(remoteTable) + "Input"
	r.mu.RLock()
	cached, ok := r.connectInputCache[typeName]
	r.mu.RUnlock()
	if ok {
		return cached
	}

	fields := graphql.InputObjectConfigFieldMap{}

	// "id" field accepts an opaque node ID.
	pkCols := introspection.PrimaryKeyColumns(remoteTable)
	if len(pkCols) > 0 {
		fields["id"] = &graphql.InputObjectFieldConfig{
			Type:        graphql.ID,
			Description: "Opaque node ID",
		}
	}

	// One sub-object per non-primary unique index.
	for _, idx := range remoteTable.Indexes {
		if !idx.Unique || idx.Name == "PRIMARY" {
			continue
		}
		subInput := r.connectByUniqueInput(remoteTable, idx)
		if subInput == nil {
			continue
		}
		fieldName := r.connectByUniqueFieldName(remoteTable, idx)
		if _, exists := fields[fieldName]; exists {
			continue
		}
		fields[fieldName] = &graphql.InputObjectFieldConfig{
			Type: subInput,
		}
	}

	if len(fields) == 0 {
		return nil
	}

	obj := graphql.NewInputObject(graphql.InputObjectConfig{
		Name:   typeName,
		Fields: fields,
	})

	r.mu.Lock()
	if c, ok := r.connectInputCache[typeName]; ok {
		r.mu.Unlock()
		return c
	}
	r.connectInputCache[typeName] = obj
	r.mu.Unlock()
	return obj
}

// nestedCreateInputForRel builds "CreateXxxNestedInput" for a one-to-many or edge-list relationship.
// It is like createInputType for the remote table, but omits the FK column(s) that will be
// auto-injected from the parent PK after insert. Connect fields are added for any remaining
// FK columns on the child table, mirroring the pattern used in createInputType.
// Returns nil if the remote table is not mutation-allowed or produces no fields.
func (r *Resolver) nestedCreateInputForRel(parentTable introspection.Table, rel introspection.Relationship) *graphql.InputObject {
	remoteTable, err := r.findTable(rel.RemoteTable)
	if err != nil {
		return nil
	}
	if !schemafilter.MutationTableAllowed(remoteTable.Name, r.filters) {
		return nil
	}

	typeName := "Create" + r.singularTypeName(parentTable) + upperFirst(rel.GraphQLFieldName) + "NestedInput"
	cacheKey := nestedCreateCacheKey(parentTable, rel)
	r.mu.RLock()
	cached, ok := r.nestedCreateCache[cacheKey]
	r.mu.RUnlock()
	if ok {
		return cached
	}

	// Insertable columns for the remote table, minus the FK columns pointing back to the parent.
	insertable := r.mutationInsertableColumns(remoteTable)
	parentFKSet := make(map[string]bool, len(rel.RemoteColumns))
	for _, col := range rel.RemoteColumns {
		parentFKSet[col] = true
	}

	// Determine which remaining FK columns can be satisfied by a connect field.
	childConnectSatisfiable := r.collectConnectSatisfiableFKsExcluding(remoteTable, parentFKSet)

	fields := graphql.InputObjectConfigFieldMap{}
	for _, col := range insertable {
		if parentFKSet[col.Name] {
			continue
		}
		fieldType := r.mapColumnTypeToGraphQLInput(remoteTable, &col)
		// A required FK column becomes optional when a connect field can supply its value.
		required := isRequiredInsertColumn(col) && !childConnectSatisfiable[col.Name]
		if required {
			fieldType = graphql.NewNonNull(fieldType)
		}
		fields[introspection.GraphQLFieldName(col)] = &graphql.InputObjectFieldConfig{
			Type:        fieldType,
			Description: col.Comment,
		}
	}

	// Connect fields for many-to-one relationships on the child table (excluding parent FK).
	for _, childRel := range remoteTable.Relationships {
		if !childRel.IsManyToOne {
			continue
		}
		// Skip if all local columns of this relationship are the parent FK.
		if allInSet(childRel.LocalColumns, parentFKSet) {
			continue
		}
		if !r.localColumnsMutationAllowed(remoteTable, childRel.LocalColumns, parentFKSet) {
			continue
		}
		connectInput := r.connectInputForRel(childRel)
		if connectInput == nil {
			continue
		}
		fieldName := childRel.GraphQLFieldName + "Connect"
		if _, exists := fields[fieldName]; exists {
			continue
		}
		fields[fieldName] = &graphql.InputObjectFieldConfig{
			Type:        connectInput,
			Description: "Connect to an existing " + childRel.GraphQLFieldName + " by id or unique field.",
		}
	}

	if len(fields) == 0 {
		return nil
	}

	obj := graphql.NewInputObject(graphql.InputObjectConfig{
		Name:   typeName,
		Fields: fields,
	})

	r.mu.Lock()
	if c, ok := r.nestedCreateCache[cacheKey]; ok {
		r.mu.Unlock()
		return c
	}
	r.nestedCreateCache[cacheKey] = obj
	r.mu.Unlock()
	return obj
}

// collectConnectSatisfiableFKsExcluding is like collectConnectSatisfiableFKs but skips
// FK columns that are in the excludeSet (e.g. the parent FK columns being auto-injected).
func (r *Resolver) collectConnectSatisfiableFKsExcluding(table introspection.Table, excludeSet map[string]bool) map[string]bool {
	result := make(map[string]bool)
	for _, rel := range table.Relationships {
		if !rel.IsManyToOne {
			continue
		}
		if allInSet(rel.LocalColumns, excludeSet) {
			continue
		}
		if !r.localColumnsMutationAllowed(table, rel.LocalColumns, excludeSet) {
			continue
		}
		if r.connectInputForRel(rel) == nil {
			continue
		}
		for _, col := range rel.LocalColumns {
			if !excludeSet[col] {
				result[col] = true
			}
		}
	}
	return result
}

// allInSet reports whether every element of cols is present in set.
func allInSet(cols []string, set map[string]bool) bool {
	for _, c := range cols {
		if !set[c] {
			return false
		}
	}
	return true
}

// collectConnectSatisfiableFKs returns the set of column names on table that are FK columns
// for a many-to-one relationship that has a valid connect input type.
// These columns can be omitted from the scalar create input because the connect field can supply them.
func (r *Resolver) collectConnectSatisfiableFKs(table introspection.Table) map[string]bool {
	result := make(map[string]bool)
	for _, rel := range table.Relationships {
		if !rel.IsManyToOne {
			continue
		}
		if !r.localColumnsMutationAllowed(table, rel.LocalColumns, nil) {
			continue
		}
		if r.connectInputForRel(rel) == nil {
			continue
		}
		for _, col := range rel.LocalColumns {
			result[col] = true
		}
	}
	return result
}

// missingRequiredInsertColumnsWithConnect is like missingRequiredInsertColumns but also
// considers FK columns that can be satisfied by a connect field as present.
func missingRequiredInsertColumnsWithConnect(table introspection.Table, allowed map[string]bool, connectSatisfiable map[string]bool) bool {
	for _, col := range table.Columns {
		if !isRequiredInsertColumn(col) {
			continue
		}
		if allowed[col.Name] {
			continue
		}
		if connectSatisfiable[col.Name] {
			continue
		}
		return true
	}
	return false
}

func (r *Resolver) localColumnsMutationAllowed(table introspection.Table, cols []string, excludeSet map[string]bool) bool {
	seen := false
	for _, colName := range cols {
		if excludeSet != nil && excludeSet[colName] {
			continue
		}
		seen = true
		if !schemafilter.MutationColumnAllowed(table.Name, colName, r.filters) {
			return false
		}
	}
	return seen
}

func (r *Resolver) m2mConnectSupported(parentTable introspection.Table, rel introspection.Relationship) bool {
	if !rel.IsManyToMany {
		return false
	}
	if r.connectInputForRel(rel) == nil {
		return false
	}
	if !schemafilter.MutationTableAllowed(rel.JunctionTable, r.filters) {
		return false
	}
	junctionTable, err := r.findTable(rel.JunctionTable)
	if err != nil {
		return false
	}
	if !r.localColumnsMutationAllowed(junctionTable, rel.JunctionLocalFKColumns, nil) {
		return false
	}
	if !r.localColumnsMutationAllowed(junctionTable, rel.JunctionRemoteFKColumns, nil) {
		return false
	}
	return true
}

func nestedCreateCacheKey(parentTable introspection.Table, rel introspection.Relationship) string {
	return strings.Join([]string{
		parentTable.Name,
		rel.RemoteTable,
		rel.GraphQLFieldName,
		strings.Join(rel.LocalColumns, ","),
		strings.Join(rel.RemoteColumns, ","),
	}, "|")
}

func columnMap(table introspection.Table) map[string]*introspection.Column {
	colMap := make(map[string]*introspection.Column, len(table.Columns))
	for i := range table.Columns {
		colMap[table.Columns[i].Name] = &table.Columns[i]
	}
	return colMap
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

func upperFirst(s string) string {
	if s == "" {
		return s
	}
	runes := []rune(s)
	runes[0] = unicode.ToUpper(runes[0])
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
	plan := r.buildCreateMutationPlan(table)

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

		// Phase 1: Partition input into scalars / connect fields / nested-create fields / m2m-connect fields.
		partitioned, err := partitionMutationInput(inputArg, plan.connectFields, plan.nestedFields, plan.m2mFields)
		if err != nil {
			return nil, err
		}

		// Phase 2: Resolve connect fields and inject FK column values into scalars.
		for fieldName, rel := range plan.connectFields {
			connectSub, ok := partitioned.connects[fieldName]
			if !ok {
				continue
			}
			if err := validateScalarVsConnectXOR(partitioned.scalars, table, rel.LocalColumns, fieldName); err != nil {
				return nil, err
			}
			fkValues, err := r.resolveConnectField(p.Context, mc.Tx(), rel.RemoteTable, rel.LocalColumns, rel.RemoteColumns, connectSub)
			if err != nil {
				return nil, err
			}
			for localCol, val := range fkValues {
				partitioned.scalars[graphQLFieldNameForColumn(table, localCol)] = val
			}
		}

		// Validate that required FK columns are satisfied (either by scalar or connect) when connect is enabled.
		connectSatisfiable := r.collectConnectSatisfiableFKs(table)
		tableColMap := columnMap(table)
		for _, rel := range table.Relationships {
			if !rel.IsManyToOne {
				continue
			}
			fieldName := rel.GraphQLFieldName + "Connect"
			_, hasConnect := plan.connectFields[fieldName]
			if !hasConnect {
				continue
			}
			_, connectProvided := partitioned.connects[fieldName]
			for _, localCol := range rel.LocalColumns {
				gqlName := graphQLFieldNameForColumn(table, localCol)
				_, scalarProvided := partitioned.scalars[gqlName]
				col := tableColMap[localCol]
				if col != nil && isRequiredInsertColumn(*col) && connectSatisfiable[localCol] {
					if !scalarProvided && !connectProvided {
						return nil, newMutationError(
							"must provide either "+gqlName+" or "+fieldName,
							"invalid_input", 0,
						)
					}
				}
			}
		}

		// Phase 3: Map scalar fields to DB columns and execute the parent INSERT.
		columns, values, err := mapInputColumns(table, partitioned.scalars, insertable)
		if err != nil {
			return nil, err
		}
		if len(inputArg) > 0 && len(columns) == 0 && len(partitioned.connects) == 0 && len(partitioned.nesteds) == 0 && len(partitioned.m2mConnects) == 0 {
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
		// resolveInsertPKValues reads user-supplied PK values from partitioned.scalars
		// (which already contains any connect-injected FK values merged back in).
		pkValues, err := resolveInsertPKValues(table, pkCols, partitioned.scalars, execResult)
		if err != nil {
			return nil, err
		}

		requiredParentColumns := make([]string, 0)
		for fieldName, rel := range plan.nestedFields {
			childRows, ok := partitioned.nesteds[fieldName]
			if !ok || len(childRows) == 0 {
				continue
			}
			requiredParentColumns = append(requiredParentColumns, rel.LocalColumns...)
		}
		for fieldName, rel := range plan.m2mFields {
			connectRows, ok := partitioned.m2mConnects[fieldName]
			if !ok || len(connectRows) == 0 {
				continue
			}
			requiredParentColumns = append(requiredParentColumns, rel.LocalColumns...)
		}

		parentRow, err := r.selectRowByPKWithRequiredColumns(p, table, pkCols, pkValues, requiredParentColumns, mc.Tx())
		if err != nil {
			return nil, err
		}
		if parentRow == nil {
			return nil, fmt.Errorf("created row could not be loaded")
		}

		// Phase 4: Execute nested one-to-many/edge inserts and pure M2M connects using parent row values.
		for fieldName, rel := range plan.nestedFields {
			childRows, ok := partitioned.nesteds[fieldName]
			if !ok || len(childRows) == 0 {
				continue
			}
			if err := r.executeNestedCreate(p.Context, mc.Tx(), table, rel, parentRow, childRows); err != nil {
				return nil, err
			}
		}
		for fieldName, rel := range plan.m2mFields {
			connectRows, ok := partitioned.m2mConnects[fieldName]
			if !ok || len(connectRows) == 0 {
				continue
			}
			if err := r.executeM2MConnect(p.Context, mc.Tx(), table, rel, parentRow, connectRows); err != nil {
				return nil, err
			}
		}

		// Phase 5: Return the loaded parent row.
		return map[string]interface{}{
			r.mutationEntityFieldName(table): parentRow,
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
	return r.selectRowByPKWithRequiredColumns(p, table, pkCols, pkValues, nil, tx)
}

func (r *Resolver) selectRowByPKWithRequiredColumns(p graphql.ResolveParams, table introspection.Table, pkCols []introspection.Column, pkValues map[string]interface{}, requiredColumns []string, tx dbexec.TxExecutor) (map[string]interface{}, error) {
	field := firstFieldAST(p.Info.FieldASTs)
	if field == nil {
		return nil, fmt.Errorf("missing field AST in resolve params")
	}
	selected := planner.SelectedColumns(table, field, p.Info.Fragments)
	selected = planner.EnsureColumns(table, selected, requiredColumns)

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

// resolveConnectField resolves a connect input object to a map of local FK column name → value.
// The connect object may specify { id: "<nodeID>" } or { by<UniqueField>: { ... } }.
// Returned map keys are DB column names (not GraphQL field names).
func (r *Resolver) resolveConnectField(
	ctx context.Context,
	tx dbexec.TxExecutor,
	remoteTableName string,
	localCols []string,
	remoteCols []string,
	connectInput map[string]interface{},
) (map[string]interface{}, error) {
	remoteTable, err := r.findTable(remoteTableName)
	if err != nil {
		return nil, newMutationError("referenced table not found: "+remoteTableName, "invalid_input", 0)
	}
	if len(localCols) == 0 || len(localCols) != len(remoteCols) {
		return nil, fmt.Errorf("invalid relationship mapping for connect to %s", remoteTableName)
	}

	allowedConnectKeys := map[string]bool{"id": true}
	uniqueByField := make(map[string]*introspection.Index)
	for i := range remoteTable.Indexes {
		idx := &remoteTable.Indexes[i]
		if !idx.Unique || idx.Name == "PRIMARY" {
			continue
		}
		fieldName := r.connectByUniqueFieldName(remoteTable, *idx)
		allowedConnectKeys[fieldName] = true
		uniqueByField[fieldName] = idx
	}
	for key := range connectInput {
		if !allowedConnectKeys[key] {
			return nil, newMutationError("unknown connect selector: "+key, "invalid_input", 0)
		}
	}

	rawID, hasID := connectInput["id"]
	idProvided := hasID && rawID != nil

	var selectedUniqueField string
	for fieldName := range uniqueByField {
		subRaw, ok := connectInput[fieldName]
		if !ok || subRaw == nil {
			continue
		}
		if _, ok := subRaw.(map[string]interface{}); !ok {
			return nil, newMutationError("connect field "+fieldName+" must be an object", "invalid_input", 0)
		}
		if selectedUniqueField != "" {
			return nil, newMutationError(
				"connect input matches multiple unique strategies; provide only one",
				"invalid_input", 0,
			)
		}
		selectedUniqueField = fieldName
	}

	strategyCount := 0
	if idProvided {
		strategyCount++
	}
	if selectedUniqueField != "" {
		strategyCount++
	}
	if strategyCount != 1 {
		return nil, newMutationError(
			"connect input must specify exactly one strategy (id or one unique selector)",
			"invalid_input", 0,
		)
	}

	if idProvided {
		pkCols := introspection.PrimaryKeyColumns(remoteTable)
		pkValues, err := decodeNodeIDToPKValues(remoteTable, pkCols, rawID)
		if err != nil {
			return nil, err
		}
		// decodeNodeIDToPKValues keys by DB column name; re-key by GraphQL field name
		// so mapRemoteToLocalFK's primary lookup (which uses GraphQL field names, matching
		// scanned rows) always succeeds without relying on the raw-column-name fallback.
		gqlKeyedPKValues := make(map[string]interface{}, len(pkValues))
		colMap := columnMap(remoteTable)
		for dbName, val := range pkValues {
			if col, ok := colMap[dbName]; ok {
				gqlKeyedPKValues[introspection.GraphQLFieldName(*col)] = val
			} else {
				gqlKeyedPKValues[dbName] = val
			}
		}
		return r.mapRemoteToLocalFK(gqlKeyedPKValues, localCols, remoteCols, remoteTable)
	}

	idx := uniqueByField[selectedUniqueField]
	subRaw := connectInput[selectedUniqueField]
	subMap, ok := subRaw.(map[string]interface{})
	if !ok {
		return nil, newMutationError("connect field "+selectedUniqueField+" must be an object", "invalid_input", 0)
	}

	colMap := columnMap(remoteTable)
	lookupValues := make(map[string]interface{}, len(idx.Columns))
	allowedSubFields := make(map[string]bool, len(idx.Columns))
	for _, colName := range idx.Columns {
		col, exists := colMap[colName]
		if !exists {
			return nil, newMutationError("invalid unique connect selector: "+selectedUniqueField, "invalid_input", 0)
		}
		gqlName := introspection.GraphQLFieldName(*col)
		allowedSubFields[gqlName] = true
		v, present := subMap[gqlName]
		if !present {
			return nil, newMutationError("connect field "+selectedUniqueField+" missing "+gqlName, "invalid_input", 0)
		}
		if v == nil {
			return nil, newMutationError(
				"connect field "+selectedUniqueField+"."+gqlName+" must not be null",
				"invalid_input", 0,
			)
		}
		normalized, err := normalizeMutationInputValue(*col, v)
		if err != nil {
			return nil, err
		}
		lookupValues[colName] = normalized
	}
	for key := range subMap {
		if !allowedSubFields[key] {
			return nil, newMutationError("unknown connect field "+selectedUniqueField+"."+key, "invalid_input", 0)
		}
	}

	// Execute the unique-key SELECT within the mutation transaction.
	query, err := planner.PlanUniqueKeyLookup(remoteTable, remoteTable.Columns, *idx, lookupValues)
	if err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, query.SQL, query.Args...)
	if err != nil {
		return nil, normalizeMutationError(err)
	}
	defer func() { _ = rows.Close() }()

	results, err := scanRows(rows, remoteTable.Columns)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, newMutationError(
			"connected "+introspection.GraphQLTypeName(remoteTable)+" not found",
			"invalid_input", 0,
		)
	}
	return r.mapRemoteToLocalFK(results[0], localCols, remoteCols, remoteTable)
}

// mapRemoteToLocalFK translates a row from the remote table into a map of local FK column name
// -> value, ready for injection into an INSERT.
func (r *Resolver) mapRemoteToLocalFK(
	remoteRow map[string]interface{},
	localCols []string,
	remoteCols []string,
	remoteTable introspection.Table,
) (map[string]interface{}, error) {
	if len(localCols) == 0 || len(localCols) != len(remoteCols) {
		// This is a schema-level invariant violation, not a user input error.
		// Returning an untyped error surfaces as InternalError in the GraphQL response.
		return nil, fmt.Errorf("invalid FK mapping: local/remote column counts differ")
	}
	result := make(map[string]interface{}, len(localCols))
	for i, remoteColName := range remoteCols {
		gqlName := graphQLFieldNameForColumn(remoteTable, remoteColName)
		val, ok := remoteRow[gqlName]
		if !ok {
			val, ok = remoteRow[remoteColName]
		}
		if !ok {
			return nil, fmt.Errorf("resolved connect row missing column %s", remoteColName)
		}
		result[localCols[i]] = val
	}
	return result, nil
}

func validateScalarVsConnectXOR(scalars map[string]interface{}, table introspection.Table, localCols []string, connectFieldName string) error {
	for _, localCol := range localCols {
		gqlName := graphQLFieldNameForColumn(table, localCol)
		if _, hasScalar := scalars[gqlName]; hasScalar {
			return newMutationError(
				"provide either "+gqlName+" or "+connectFieldName+", not both",
				"invalid_input", 0,
			)
		}
	}
	return nil
}

// executeNestedCreate inserts all child rows for a one-to-many or edge-list relationship
// after the parent INSERT. The FK column(s) on the child side are populated automatically.
// Connect fields on the child input (e.g. productConnect) are resolved within the same TX.
func (r *Resolver) executeNestedCreate(
	ctx context.Context,
	tx dbexec.TxExecutor,
	parentTable introspection.Table,
	rel introspection.Relationship,
	parentRow map[string]interface{},
	childRows []map[string]interface{},
) error {
	remoteTable, err := r.findTable(rel.RemoteTable)
	if err != nil {
		return err
	}
	if len(rel.LocalColumns) == 0 || len(rel.LocalColumns) != len(rel.RemoteColumns) {
		return fmt.Errorf("invalid nested relationship mapping for %s", rel.GraphQLFieldName)
	}

	// Insertable columns excluding the FK columns that are auto-injected from the parent.
	insertable := r.mutationInsertableColumns(remoteTable)
	parentFKSet := make(map[string]bool, len(rel.RemoteColumns))
	for _, col := range rel.RemoteColumns {
		parentFKSet[col] = true
	}
	scalarCols := make([]introspection.Column, 0, len(insertable))
	for _, col := range insertable {
		if !parentFKSet[col.Name] {
			scalarCols = append(scalarCols, col)
		}
	}
	scalarAllowed := columnNameSet(scalarCols)

	// Build connect field name maps for child relationships (excluding parent FK).
	childConnectFieldNames := make(map[string]introspection.Relationship)
	for _, childRel := range remoteTable.Relationships {
		if !childRel.IsManyToOne || allInSet(childRel.LocalColumns, parentFKSet) {
			continue
		}
		if !r.localColumnsMutationAllowed(remoteTable, childRel.LocalColumns, parentFKSet) {
			continue
		}
		if r.connectInputForRel(childRel) == nil {
			continue
		}
		childConnectFieldNames[childRel.GraphQLFieldName+"Connect"] = childRel
	}

	// Build the FK values to inject from parent row columns.
	fkInjectionCols := make([]string, len(rel.RemoteColumns))
	fkInjectionVals := make([]interface{}, len(rel.RemoteColumns))
	for i, parentCol := range rel.LocalColumns {
		parentField := graphQLFieldNameForColumn(parentTable, parentCol)
		parentValue, ok := parentRow[parentField]
		if !ok {
			return fmt.Errorf("missing parent value for column %s while building nested create", parentCol)
		}
		fkInjectionCols[i] = rel.RemoteColumns[i]
		fkInjectionVals[i] = parentValue
	}

	for _, childInput := range childRows {
		// Partition into scalars and connect fields (no nested-of-nested for now).
		partitioned, err := partitionMutationInput(childInput, childConnectFieldNames, nil, nil)
		if err != nil {
			return err
		}

		// Resolve connect fields and inject their FK values into scalars.
		for fieldName, childRel := range childConnectFieldNames {
			connectSub, ok := partitioned.connects[fieldName]
			if !ok {
				continue
			}
			if err := validateScalarVsConnectXOR(partitioned.scalars, remoteTable, childRel.LocalColumns, fieldName); err != nil {
				return err
			}
			fkValues, err := r.resolveConnectField(ctx, tx, childRel.RemoteTable, childRel.LocalColumns, childRel.RemoteColumns, connectSub)
			if err != nil {
				return err
			}
			for localCol, val := range fkValues {
				partitioned.scalars[graphQLFieldNameForColumn(remoteTable, localCol)] = val
			}
		}

		columns, values, err := mapInputColumns(remoteTable, partitioned.scalars, scalarAllowed)
		if err != nil {
			return err
		}
		if len(childInput) > 0 && len(columns) == 0 && len(partitioned.connects) == 0 {
			return newMutationError("no insertable columns in nested input", "invalid_input", 0)
		}
		// Append injected parent FK columns in relationship order.
		for i := range fkInjectionCols {
			columns = append(columns, fkInjectionCols[i])
			values = append(values, fkInjectionVals[i])
		}
		query, err := planner.PlanInsert(remoteTable, columns, values)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, query.SQL, query.Args...); err != nil {
			return normalizeMutationError(err)
		}
	}
	return nil
}

// executeM2MConnect inserts rows into the pure junction table for many-to-many connections.
func (r *Resolver) executeM2MConnect(
	ctx context.Context,
	tx dbexec.TxExecutor,
	parentTable introspection.Table,
	rel introspection.Relationship,
	parentRow map[string]interface{},
	connectRows []map[string]interface{},
) error {
	if len(rel.LocalColumns) == 0 || len(rel.LocalColumns) != len(rel.JunctionLocalFKColumns) {
		return fmt.Errorf("invalid many-to-many local mapping for %s", rel.GraphQLFieldName)
	}
	if len(rel.RemoteColumns) == 0 || len(rel.RemoteColumns) != len(rel.JunctionRemoteFKColumns) {
		return fmt.Errorf("invalid many-to-many remote mapping for %s", rel.GraphQLFieldName)
	}

	junctionTable, err := r.findTable(rel.JunctionTable)
	if err != nil {
		return err
	}

	parentFKValues := make([]interface{}, len(rel.LocalColumns))
	for i, parentCol := range rel.LocalColumns {
		parentField := graphQLFieldNameForColumn(parentTable, parentCol)
		parentValue, ok := parentRow[parentField]
		if !ok {
			return fmt.Errorf("missing parent value for column %s while building many-to-many connect", parentCol)
		}
		parentFKValues[i] = parentValue
	}

	for _, connectInput := range connectRows {
		if len(connectInput) == 0 {
			return newMutationError("many-to-many connect item must not be empty", "invalid_input", 0)
		}
		// Pass rel.RemoteColumns for both localCols and remoteCols so that
		// mapRemoteToLocalFK maps each remote PK column to itself. The returned
		// remoteValues is therefore keyed by rel.RemoteColumns (DB column names),
		// which is exactly what the junction insert loop below expects.
		remoteValues, err := r.resolveConnectField(ctx, tx, rel.RemoteTable, rel.RemoteColumns, rel.RemoteColumns, connectInput)
		if err != nil {
			return err
		}

		columns := make([]string, 0, len(rel.JunctionLocalFKColumns)+len(rel.JunctionRemoteFKColumns))
		values := make([]interface{}, 0, len(rel.JunctionLocalFKColumns)+len(rel.JunctionRemoteFKColumns))
		for i, junctionCol := range rel.JunctionLocalFKColumns {
			columns = append(columns, junctionCol)
			values = append(values, parentFKValues[i])
		}
		for i, remoteCol := range rel.RemoteColumns {
			columns = append(columns, rel.JunctionRemoteFKColumns[i])
			values = append(values, remoteValues[remoteCol])
		}

		query, err := planner.PlanInsert(junctionTable, columns, values)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, query.SQL, query.Args...); err != nil {
			return normalizeMutationError(err)
		}
	}
	return nil
}

func decodeNodeIDToPKValues(table introspection.Table, pkCols []introspection.Column, rawID interface{}) (map[string]interface{}, error) {
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
	return decodeNodeIDToPKValues(table, pkCols, rawID)
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

// partitionedMutationInput separates a raw GraphQL create input map into three disjoint buckets.
type partitionedMutationInput struct {
	// scalars contains plain column fields to be passed directly to mapInputColumns.
	scalars map[string]interface{}
	// connects maps "<relFieldName>Connect" -> connect sub-object.
	connects map[string]map[string]interface{}
	// nesteds maps "<relFieldName>Create" -> list of child input objects.
	nesteds map[string][]map[string]interface{}
	// m2mConnects maps "<relFieldName>Connect" (many-to-many) -> list of connect objects.
	m2mConnects map[string][]map[string]interface{}
}

// partitionMutationInput splits raw into scalars, connect fields, and nested-create fields.
// connectFieldNames, nestedFieldNames, and m2mFieldNames identify which keys belong to each category.
func partitionMutationInput(
	raw map[string]interface{},
	connectFieldNames map[string]introspection.Relationship,
	nestedFieldNames map[string]introspection.Relationship,
	m2mFieldNames map[string]introspection.Relationship,
) (partitionedMutationInput, error) {
	result := partitionedMutationInput{
		scalars:     make(map[string]interface{}, len(raw)),
		connects:    make(map[string]map[string]interface{}),
		nesteds:     make(map[string][]map[string]interface{}),
		m2mConnects: make(map[string][]map[string]interface{}),
	}
	for k, v := range raw {
		if _, ok := connectFieldNames[k]; ok {
			m, ok := v.(map[string]interface{})
			if !ok {
				return partitionedMutationInput{}, newMutationError("connect field "+k+" must be an object", "invalid_input", 0)
			}
			result.connects[k] = m
			continue
		}
		if _, ok := nestedFieldNames[k]; ok {
			items, ok := v.([]interface{})
			if !ok {
				return partitionedMutationInput{}, newMutationError("nested field "+k+" must be a list of objects", "invalid_input", 0)
			}
			rows := make([]map[string]interface{}, 0, len(items))
			for _, item := range items {
				m, ok := item.(map[string]interface{})
				if !ok {
					return partitionedMutationInput{}, newMutationError("nested field "+k+" must be a list of objects", "invalid_input", 0)
				}
				rows = append(rows, m)
			}
			result.nesteds[k] = rows
			continue
		}
		if _, ok := m2mFieldNames[k]; ok {
			items, ok := v.([]interface{})
			if !ok {
				return partitionedMutationInput{}, newMutationError("many-to-many field "+k+" must be a list of connect objects", "invalid_input", 0)
			}
			rows := make([]map[string]interface{}, 0, len(items))
			for _, item := range items {
				m, ok := item.(map[string]interface{})
				if !ok {
					return partitionedMutationInput{}, newMutationError("many-to-many field "+k+" must be a list of connect objects", "invalid_input", 0)
				}
				rows = append(rows, m)
			}
			result.m2mConnects[k] = rows
			continue
		}
		result.scalars[k] = v
	}
	return result, nil
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
