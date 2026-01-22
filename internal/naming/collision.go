package naming

import (
	"fmt"
	"log/slog"
)

// CollisionResolver tracks registered names and resolves collisions
// by applying numeric suffixes when duplicates are detected.
type CollisionResolver struct {
	seenTypes   map[string]string              // GraphQL type name → source table
	seenFields  map[string]map[string]string   // type name → field name → source
	seenQueries map[string]string              // query field name → source table
	logger      *slog.Logger
}

// NewCollisionResolver creates a new collision resolver.
func NewCollisionResolver(logger *slog.Logger) *CollisionResolver {
	if logger == nil {
		logger = slog.Default()
	}
	return &CollisionResolver{
		seenTypes:   make(map[string]string),
		seenFields:  make(map[string]map[string]string),
		seenQueries: make(map[string]string),
		logger:      logger,
	}
}

// RegisterType registers a GraphQL type name and returns the resolved name.
// If a collision occurs, applies a numeric suffix and logs a warning.
func (c *CollisionResolver) RegisterType(graphqlName, tableName string) string {
	return c.resolveCollision(graphqlName, c.seenTypes, "table:"+tableName)
}

// RegisterField registers a field name within a type and returns the resolved name.
// If a collision occurs, applies a numeric suffix and logs a warning.
func (c *CollisionResolver) RegisterField(typeName, fieldName, source string) string {
	if c.seenFields[typeName] == nil {
		c.seenFields[typeName] = make(map[string]string)
	}
	return c.resolveCollision(fieldName, c.seenFields[typeName], source)
}

// FieldExists checks if a field name already exists for a type.
func (c *CollisionResolver) FieldExists(typeName, fieldName string) bool {
	if fields, ok := c.seenFields[typeName]; ok {
		_, exists := fields[fieldName]
		return exists
	}
	return false
}

// RegisterQuery registers a query field name and returns the resolved name.
// If a collision occurs, applies a numeric suffix and logs a warning.
func (c *CollisionResolver) RegisterQuery(fieldName, tableName string) string {
	return c.resolveCollision(fieldName, c.seenQueries, "table:"+tableName)
}

// resolveCollision attempts to register a name in the given map.
// If the name already exists, finds the next available numeric suffix.
func (c *CollisionResolver) resolveCollision(name string, seen map[string]string, source string) string {
	if _, exists := seen[name]; !exists {
		seen[name] = source
		return name
	}

	// Collision detected - find next available suffix
	existingSource := seen[name]
	c.logger.Warn("naming collision detected, applying suffix",
		slog.String("name", name),
		slog.String("existing_source", existingSource),
		slog.String("new_source", source),
	)

	for i := 2; ; i++ {
		suffixed := fmt.Sprintf("%s%d", name, i)
		if _, exists := seen[suffixed]; !exists {
			seen[suffixed] = source
			return suffixed
		}
	}
}
