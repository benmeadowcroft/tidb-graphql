package naming

import "strings"

// graphqlReservedTypeWords contains GraphQL keywords and built-in types
// that should not be used as type names.
var graphqlReservedTypeWords = map[string]bool{
	// GraphQL language keywords
	"query":        true,
	"mutation":     true,
	"subscription": true,
	"type":         true,
	"schema":       true,
	"scalar":       true,
	"enum":         true,
	"input":        true,
	"interface":    true,
	"union":        true,
	"fragment":     true,
	"directive":    true,
	"extend":       true,
	"implements":   true,
	"on":           true,

	// Built-in scalar types
	"int":     true,
	"float":   true,
	"string":  true,
	"boolean": true,
	"id":      true,

	// Boolean literals
	"true":  true,
	"false": true,
	"null":  true,
}

// isReservedTypeName checks if a type name is reserved.
func isReservedTypeName(name string) bool {
	lowerName := strings.ToLower(name)
	if strings.HasPrefix(lowerName, "__") {
		return true
	}
	if graphqlReservedTypeWords[lowerName] {
		return true
	}
	return isReservedPattern(lowerName)
}

// isReservedFieldName checks if a field name is reserved.
func isReservedFieldName(name string) bool {
	lowerName := strings.ToLower(name)
	if strings.HasPrefix(lowerName, "__") {
		return true
	}
	return isReservedPattern(lowerName)
}

// isReservedPattern checks if a name matches patterns reserved for future features.
func isReservedPattern(name string) bool {
	// Reserve _aggregate suffix for future aggregation queries
	if strings.HasSuffix(name, "_aggregate") {
		return true
	}
	return false
}
