// Package naming provides centralized naming logic for converting SQL schema
// names to GraphQL schema names, including pluralization, collision detection,
// and reserved word handling.
package naming

// Config holds naming customization options
type Config struct {
	// PluralOverrides maps singular -> custom plural
	// Example: {"person": "people", "status": "statuses"}
	PluralOverrides map[string]string `mapstructure:"plural_overrides"`

	// SingularOverrides maps plural -> custom singular
	// Example: {"people": "person", "data": "datum"}
	SingularOverrides map[string]string `mapstructure:"singular_overrides"`

	// TypeOverrides maps SQL table name -> explicit GraphQL type name.
	// Matching is case-insensitive at lookup time.
	TypeOverrides map[string]string `mapstructure:"type_overrides"`
}

// DefaultConfig returns sensible defaults
func DefaultConfig() Config {
	return Config{
		PluralOverrides:   make(map[string]string),
		SingularOverrides: make(map[string]string),
		TypeOverrides:     make(map[string]string),
	}
}
