package introspection

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseEnumValuesSimple(t *testing.T) {
	values, err := parseEnumValues("enum('a','b','c')")
	assert.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, values)
}

func TestParseEnumValuesEscapes(t *testing.T) {
	values, err := parseEnumValues("ENUM('in\\'progress','it''s','back\\\\slash','a,b')")
	assert.NoError(t, err)
	assert.Equal(t, []string{"in'progress", "it's", "back\\slash", "a,b"}, values)
}

func TestParseEnumValuesEmptyString(t *testing.T) {
	values, err := parseEnumValues("ENUM('')")
	assert.NoError(t, err)
	assert.Equal(t, []string{""}, values)
}

func TestParseEnumValuesSingleValue(t *testing.T) {
	values, err := parseEnumValues("ENUM('only')")
	assert.NoError(t, err)
	assert.Equal(t, []string{"only"}, values)
}

func TestParseEnumValuesWhitespace(t *testing.T) {
	values, err := parseEnumValues("ENUM( 'a' , 'b' )")
	assert.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, values)
}
