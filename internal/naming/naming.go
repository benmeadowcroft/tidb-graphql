package naming

import (
	"log/slog"
	"strings"
)

// Namer provides all name transformation functions for converting SQL names
// to GraphQL names. It handles pluralization, reserved words, and collisions.
type Namer struct {
	config   Config
	logger   *slog.Logger
	resolver *CollisionResolver
}

// New creates a Namer with the given configuration
func New(cfg Config, logger *slog.Logger) *Namer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Namer{
		config:   cfg,
		logger:   logger,
		resolver: NewCollisionResolver(logger),
	}
}

// Default returns a Namer with default configuration
func Default() *Namer {
	return New(DefaultConfig(), nil)
}

// Reset clears the collision resolver state, allowing the namer to be reused
// for a new schema build.
func (n *Namer) Reset() {
	n.resolver = NewCollisionResolver(n.logger)
}

// ToGraphQLTypeName converts a table name to GraphQL type (PascalCase)
// Example: "user_profiles" -> "UserProfiles"
func (n *Namer) ToGraphQLTypeName(tableName string) string {
	// Check reserved patterns on original name (before case conversion)
	// because patterns like "_aggregate" are lost after PascalCase conversion
	if isReservedPattern(strings.ToLower(tableName)) {
		name := toPascalCase(tableName)
		n.logger.Warn("GraphQL name conflicts with reserved pattern, auto-suffixed",
			slog.String("original", name),
			slog.String("renamed", name+"_"),
		)
		return name + "_"
	}

	name := toPascalCase(tableName)
	return n.validateTypeAndSuffix(name)
}

// ToGraphQLFieldName converts a column/table name to GraphQL field (camelCase)
// Example: "user_name" -> "userName"
func (n *Namer) ToGraphQLFieldName(columnName string) string {
	return toCamelCase(columnName)
}

// ManyToOneFieldName generates the GraphQL field name for a many-to-one relationship
// based on the FK column name with common suffixes stripped.
// Example: "author_id" -> "author", "created_by_user_id" -> "createdByUser"
func (n *Namer) ManyToOneFieldName(fkColumn string) string {
	name := fkColumn
	// Strip common FK suffixes
	for _, suffix := range []string{"_id", "_fk"} {
		if strings.HasSuffix(strings.ToLower(name), suffix) {
			name = name[:len(name)-len(suffix)]
			break
		}
	}
	return n.ToGraphQLFieldName(name)
}

// OneToManyFieldName generates the GraphQL field name for a one-to-many relationship.
// If isOnlyFK is true (single FK from source table), uses pluralized table name.
// Otherwise, prefixes with the FK column name for disambiguation.
// Example: isOnlyFK=true: "comments" -> "comments"
// Example: isOnlyFK=false, fkColumn="author_id": "posts" -> "authorPosts"
func (n *Namer) OneToManyFieldName(sourceTable, fkColumn string, isOnlyFK bool) string {
	tablePlural := n.Pluralize(n.ToGraphQLFieldName(sourceTable))

	if isOnlyFK {
		return tablePlural
	}

	// Multiple FKs: prefix with FK column name (minus _id)
	prefix := n.ManyToOneFieldName(fkColumn)
	// Capitalize first letter of table for camelCase: authorPosts
	if len(tablePlural) > 0 {
		return prefix + strings.ToUpper(tablePlural[:1]) + tablePlural[1:]
	}
	return prefix
}

// JunctionFieldName generates the field name for junction relationships.
// For attribute junctions, it always uses the junction table name.
// For pure junctions, it uses the target table name only when the junction name
// is a simple combination of the two table names; otherwise it uses the junction name.
func (n *Namer) JunctionFieldName(junctionTable, leftTable, rightTable, targetTable string, isAttribute bool) string {
	if isAttribute || !n.isSimpleJunctionName(junctionTable, leftTable, rightTable) {
		fieldName := n.ToGraphQLFieldName(junctionTable)
		return n.Pluralize(fieldName)
	}
	return n.ManyToManyFieldName(targetTable)
}

// JunctionEdgeRefFieldName generates the field name for M2O references from
// an attribute junction (edge) back to a base table.
// Example: "employees" -> "employee".
func (n *Namer) JunctionEdgeRefFieldName(targetTable string) string {
	return n.Singularize(n.ToGraphQLFieldName(targetTable))
}

func (n *Namer) isSimpleJunctionName(junctionTable, leftTable, rightTable string) bool {
	junctionTokens := splitTokens(junctionTable)
	if len(junctionTokens) == 0 {
		return false
	}

	allowed := make(map[string]struct{})
	n.addNameTokens(allowed, leftTable)
	n.addNameTokens(allowed, rightTable)

	for _, token := range junctionTokens {
		if _, ok := allowed[token]; !ok {
			return false
		}
	}
	return true
}

func (n *Namer) addNameTokens(set map[string]struct{}, name string) {
	for _, token := range splitTokens(name) {
		set[token] = struct{}{}
		set[n.Singularize(token)] = struct{}{}
		set[n.Pluralize(token)] = struct{}{}
	}
}

func splitTokens(name string) []string {
	tokens := strings.Split(strings.ToLower(name), "_")
	out := make([]string, 0, len(tokens))
	for _, token := range tokens {
		if token == "" {
			continue
		}
		out = append(out, token)
	}
	return out
}

// EdgeTypeName generates the GraphQL type name for a junction/edge type.
// Combines both related type names alphabetically by original table name.
// Each type is singularized to represent a single relationship instance.
// Example: ("departments", "employees") -> "DepartmentEmployee"
// Example: ("employees", "departments") -> "DepartmentEmployee" (same - alphabetical)
func (n *Namer) EdgeTypeName(leftTable, rightTable string) string {
	// Sort alphabetically by original table name
	if leftTable > rightTable {
		leftTable, rightTable = rightTable, leftTable
	}

	// Convert each to singular PascalCase
	leftType := n.Singularize(n.ToGraphQLTypeName(leftTable))
	rightType := n.Singularize(n.ToGraphQLTypeName(rightTable))

	// Concatenate
	name := leftType + rightType

	return n.validateTypeAndSuffix(name)
}

// EdgeFieldName generates the field name for edge list access on parent types.
// Returns pluralized edge type name in camelCase.
// Example: ("departments", "employees") -> "departmentEmployees"
func (n *Namer) EdgeFieldName(leftTable, rightTable string) string {
	edgeType := n.EdgeTypeName(leftTable, rightTable)

	// Convert PascalCase to camelCase
	if len(edgeType) > 0 {
		edgeType = strings.ToLower(edgeType[:1]) + edgeType[1:]
	}

	// Pluralize
	return n.Pluralize(edgeType)
}

// ManyToManyFieldName generates the field name for direct M2M access (pure junctions).
// Returns pluralized target table name in camelCase.
// Example: "employees" -> "employees", "role" -> "roles"
func (n *Namer) ManyToManyFieldName(targetTable string) string {
	fieldName := n.ToGraphQLFieldName(targetTable)
	return n.Pluralize(fieldName)
}

// RegisterEdgeField registers an edge list field and returns the resolved name.
// Handles collisions with existing column/relationship fields.
func (n *Namer) RegisterEdgeField(typeName, leftTable, rightTable string) string {
	fieldName := n.EdgeFieldName(leftTable, rightTable)
	fieldName = n.validateFieldAndSuffix(fieldName)

	// Check for collision with existing fields
	if n.resolver.FieldExists(typeName, fieldName) {
		fieldName = fieldName + "Edge"
	}

	source := "edge:" + leftTable + "+" + rightTable
	return n.resolver.RegisterField(typeName, fieldName, source)
}

// RegisterManyToManyField registers a direct M2M field and returns the resolved name.
// Uses "Via{JunctionType}" suffix if the base field name is already taken.
func (n *Namer) RegisterManyToManyField(typeName, targetTable, junctionTable string) string {
	fieldName := n.ManyToManyFieldName(targetTable)
	fieldName = n.validateFieldAndSuffix(fieldName)

	// Check for collision with existing fields
	if n.resolver.FieldExists(typeName, fieldName) {
		// Use Via{JunctionType} suffix for disambiguation
		junctionTypeName := n.ToGraphQLTypeName(junctionTable)
		fieldName = fieldName + "Via" + junctionTypeName
	}

	source := "m2m:" + junctionTable + "->" + targetTable
	return n.resolver.RegisterField(typeName, fieldName, source)
}

// RegisterType registers a table name and returns the resolved GraphQL type name.
// If a collision occurs, returns a suffixed name and logs a warning.
func (n *Namer) RegisterType(tableName string) string {
	graphqlName := n.ToGraphQLTypeName(tableName)
	return n.resolver.RegisterType(graphqlName, tableName)
}

// RegisterColumnField registers a column field and returns the resolved field name.
// Columns always win in precedence, so this establishes the field name.
func (n *Namer) RegisterColumnField(typeName, columnName string) string {
	fieldName := n.validateFieldAndSuffix(n.ToGraphQLFieldName(columnName))
	return n.resolver.RegisterField(typeName, fieldName, "column:"+columnName)
}

// RegisterRelationshipField registers a relationship field and returns the resolved name.
// If the field collides with a column, applies appropriate suffix (Rel/Ref).
func (n *Namer) RegisterRelationshipField(typeName, fieldName, source string, isManyToOne bool) string {
	fieldName = n.validateFieldAndSuffix(fieldName)
	// Check if field already exists (column collision)
	if n.resolver.FieldExists(typeName, fieldName) {
		// Apply suffix based on relationship type
		if isManyToOne {
			fieldName = fieldName + "Ref"
		} else {
			fieldName = fieldName + "Rel"
		}
	}
	fieldName = n.validateFieldAndSuffix(fieldName)
	return n.resolver.RegisterField(typeName, fieldName, "relationship:"+source)
}

// RegisterQueryField registers a query field and returns the resolved name.
func (n *Namer) RegisterQueryField(tableName string) string {
	if isReservedPattern(strings.ToLower(tableName)) {
		fieldName := n.ToGraphQLFieldName(tableName) + "_"
		n.logger.Warn("GraphQL name conflicts with reserved pattern, auto-suffixed",
			slog.String("original", n.ToGraphQLFieldName(tableName)),
			slog.String("renamed", fieldName),
		)
		return n.resolver.RegisterQuery(fieldName, tableName)
	}
	fieldName := n.validateFieldAndSuffix(n.ToGraphQLFieldName(tableName))
	return n.resolver.RegisterQuery(fieldName, tableName)
}

func (n *Namer) validateTypeAndSuffix(name string) string {
	if isReservedTypeName(name) {
		safeName := name + "_"
		n.logger.Warn("GraphQL name conflicts with reserved word, auto-suffixed",
			slog.String("original", name),
			slog.String("renamed", safeName),
		)
		return safeName
	}
	return name
}

func (n *Namer) validateFieldAndSuffix(name string) string {
	if isReservedFieldName(name) {
		safeName := name + "_"
		n.logger.Warn("GraphQL name conflicts with reserved word, auto-suffixed",
			slog.String("original", name),
			slog.String("renamed", safeName),
		)
		return safeName
	}
	return name
}

// toPascalCase converts snake_case to PascalCase
func toPascalCase(s string) string {
	parts := strings.Split(s, "_")
	for i, part := range parts {
		if len(part) > 0 {
			parts[i] = strings.ToUpper(part[:1]) + part[1:]
		}
	}
	return strings.Join(parts, "")
}

// toCamelCase converts snake_case to camelCase
func toCamelCase(s string) string {
	parts := strings.Split(s, "_")
	for i := 1; i < len(parts); i++ {
		if len(parts[i]) > 0 {
			parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
		}
	}
	return strings.Join(parts, "")
}
