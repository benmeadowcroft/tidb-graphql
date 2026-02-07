package resolver

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNormalizeEnumValueName(t *testing.T) {
	assert.Equal(t, "IN_PROGRESS", normalizeEnumValueName("in-progress"))
	assert.Equal(t, "VALUE_123", normalizeEnumValueName("123"))
	assert.Equal(t, "VALUE", normalizeEnumValueName("   "))
	assert.Equal(t, "READY_SET_GO", normalizeEnumValueName("ready,set go"))
	assert.Equal(t, "U00E0_LA_CARTE", normalizeEnumValueName("à-la-carte"))
	assert.Equal(t, "U914D_U9001_U4E2D", normalizeEnumValueName("配送中"))
	assert.Equal(t, "U01F63A", normalizeEnumValueName("😺"))
}

func TestUniqueEnumValueName(t *testing.T) {
	used := make(map[string]int)
	assert.Equal(t, "READY", uniqueEnumValueName("READY", used))
	assert.Equal(t, "READY_2", uniqueEnumValueName("READY", used))
	assert.Equal(t, "READY_3", uniqueEnumValueName("READY", used))
}
