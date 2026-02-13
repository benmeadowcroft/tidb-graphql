package setutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCanonicalize(t *testing.T) {
	allowed := []string{"featured", "new", "clearance", "seasonal", "limited"}

	csv, err := Canonicalize([]string{"seasonal", "featured", "seasonal"}, allowed)
	require.NoError(t, err)
	assert.Equal(t, "featured,seasonal", csv)
}

func TestCanonicalize_EmptySet(t *testing.T) {
	allowed := []string{"featured", "new"}
	csv, err := Canonicalize([]string{}, allowed)
	require.NoError(t, err)
	assert.Equal(t, "", csv)
}

func TestCanonicalize_InvalidValue(t *testing.T) {
	allowed := []string{"featured", "new"}
	_, err := Canonicalize([]string{"featured", "invalid"}, allowed)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid set value")
}

func TestCanonicalizeAny(t *testing.T) {
	allowed := []string{"featured", "new", "clearance"}
	csv, err := CanonicalizeAny([]interface{}{"new", "featured"}, allowed)
	require.NoError(t, err)
	assert.Equal(t, "featured,new", csv)
}

func TestCanonicalizeMany(t *testing.T) {
	allowed := []string{"featured", "new", "clearance"}
	values, err := CanonicalizeMany([]interface{}{
		[]interface{}{"new"},
		[]interface{}{"new", "featured"},
		[]interface{}{},
	}, allowed)
	require.NoError(t, err)
	assert.Equal(t, []string{"new", "featured,new", ""}, values)
}
