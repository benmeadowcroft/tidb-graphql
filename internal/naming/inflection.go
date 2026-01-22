package naming

import (
	"github.com/jinzhu/inflection"
)

// Pluralize converts a singular word to its plural form.
// Checks custom overrides first, then falls back to the inflection library.
func (n *Namer) Pluralize(word string) string {
	// Check custom overrides first
	if override, ok := n.config.PluralOverrides[word]; ok {
		return override
	}
	return inflection.Plural(word)
}

// Singularize converts a plural word to its singular form.
// Checks custom overrides first, then falls back to the inflection library.
func (n *Namer) Singularize(word string) string {
	// Check custom overrides first
	if override, ok := n.config.SingularOverrides[word]; ok {
		return override
	}
	return inflection.Singular(word)
}
