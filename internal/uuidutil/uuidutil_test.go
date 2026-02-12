package uuidutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseString(t *testing.T) {
	u, canonical, err := ParseString("550E8400-E29B-41D4-A716-446655440000")
	require.NoError(t, err)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", canonical)
	assert.Equal(t, canonical, u.String())

	_, _, err = ParseString("not-a-uuid")
	require.Error(t, err)
}

func TestParseBytes(t *testing.T) {
	_, canonical, err := ParseBytes([]byte{
		0x55, 0x0e, 0x84, 0x00,
		0xe2, 0x9b,
		0x41, 0xd4,
		0xa7, 0x16,
		0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
	})
	require.NoError(t, err)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", canonical)

	_, _, err = ParseBytes([]byte{0x01, 0x02})
	require.Error(t, err)
}

func TestIsBinaryStorageType(t *testing.T) {
	assert.True(t, IsBinaryStorageType("binary"))
	assert.True(t, IsBinaryStorageType("VARBINARY"))
	assert.False(t, IsBinaryStorageType("blob"))
	assert.False(t, IsBinaryStorageType("char"))
}
