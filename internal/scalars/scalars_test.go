package scalars

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/graphql-go/graphql/language/ast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBigIntScalar(t *testing.T) {
	scalar := BigInt()

	serialized := scalar.Serialize(int64(9223372036854775807))
	assert.Equal(t, "9223372036854775807", serialized)

	parsed := scalar.ParseValue("42")
	require.IsType(t, int64(0), parsed)
	assert.Equal(t, int64(42), parsed)

	invalid := scalar.ParseValue("not-a-number")
	assert.Nil(t, invalid)
}

func TestDecimalScalar(t *testing.T) {
	scalar := Decimal()

	serialized := scalar.Serialize("12345.67")
	assert.Equal(t, "12345.67", serialized)

	parsed := scalar.ParseValue("98.76")
	assert.Equal(t, "98.76", parsed)

	literal := scalar.ParseLiteral(&ast.FloatValue{Value: "10.5"})
	assert.Equal(t, "10.5", literal)
}

func TestDateScalar(t *testing.T) {
	scalar := Date()

	input := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	serialized := scalar.Serialize(input)
	assert.Equal(t, "2024-01-15", serialized)

	parsed := scalar.ParseValue("2024-01-02")
	require.IsType(t, time.Time{}, parsed)
	parsedTime := parsed.(time.Time)
	assert.Equal(t, "2024-01-02", parsedTime.Format("2006-01-02"))

	parsedRFC := scalar.ParseValue("2024-01-02T11:12:13Z")
	require.IsType(t, time.Time{}, parsedRFC)
	parsedRFCTime := parsedRFC.(time.Time)
	assert.Equal(t, "2024-01-02", parsedRFCTime.Format("2006-01-02"))
	assert.Equal(t, 0, parsedRFCTime.Hour())
}

func TestJSONScalar(t *testing.T) {
	scalar := JSON()

	input := map[string]interface{}{"name": "ava", "active": true}
	serialized := scalar.Serialize(input)
	require.IsType(t, "", serialized)
	var decoded map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(serialized.(string)), &decoded))
	assert.Equal(t, "ava", decoded["name"])
	assert.Equal(t, true, decoded["active"])

	parsed := scalar.ParseValue(`{"ok":true}`)
	assert.Equal(t, `{"ok":true}`, parsed)
}

func TestNonNegativeIntScalar(t *testing.T) {
	scalar := NonNegativeInt()

	assert.Equal(t, 3, scalar.Serialize(3))
	assert.Nil(t, scalar.Serialize(-1))

	assert.Equal(t, 4, scalar.ParseValue("4"))
	assert.Nil(t, scalar.ParseValue("-2"))

	literal := scalar.ParseLiteral(&ast.IntValue{Value: "7"})
	assert.Equal(t, 7, literal)
}
