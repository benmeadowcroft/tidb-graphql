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
