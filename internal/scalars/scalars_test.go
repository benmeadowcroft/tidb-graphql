package scalars

import (
	"encoding/base64"
	"encoding/json"
	"math"
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

	assert.Nil(t, scalar.Serialize(float64(math.MaxInt64)*2))
	assert.Nil(t, scalar.ParseValue(float64(math.MaxInt64)*2))
}

func TestDecimalScalar(t *testing.T) {
	scalar := Decimal()

	serialized := scalar.Serialize("12345.67")
	assert.Equal(t, "12345.67", serialized)

	parsed := scalar.ParseValue("98.76")
	assert.Equal(t, "98.76", parsed)
	assert.Equal(t, ".5", scalar.ParseValue(".5"))
	assert.Equal(t, "1e3", scalar.ParseValue("1e3"))
	assert.Nil(t, scalar.ParseValue("not-a-decimal"))
	assert.Nil(t, scalar.ParseValue("1/2"))
	assert.Nil(t, scalar.ParseValue(""))

	literal := scalar.ParseLiteral(&ast.FloatValue{Value: "10.5"})
	assert.Equal(t, "10.5", literal)
	assert.Nil(t, scalar.ParseLiteral(&ast.StringValue{Value: "abc"}))
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

	minDate := scalar.ParseValue("0000-01-01")
	require.IsType(t, time.Time{}, minDate)
	assert.Equal(t, "0000-01-01", minDate.(time.Time).Format("2006-01-02"))

	maxDate := scalar.ParseValue("9999-12-31")
	require.IsType(t, time.Time{}, maxDate)
	assert.Equal(t, "9999-12-31", maxDate.(time.Time).Format("2006-01-02"))

	assert.Nil(t, scalar.ParseValue("10000-01-01"))
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

func TestTimeScalar(t *testing.T) {
	scalar := Time()

	assert.Equal(t, "11:12:00", scalar.ParseValue("11:12"))
	assert.Equal(t, "00:11:12", scalar.ParseValue("1112"))
	assert.Equal(t, "-838:59:59.000000", scalar.ParseValue("-838:59:59.000000"))
	assert.Equal(t, "05:06:07.89", scalar.ParseValue("05:06:07.89"))
	assert.Equal(t, "01:01:01", scalar.Serialize([]byte("1:1:1")))

	assert.Nil(t, scalar.ParseValue("839:00:00"))
	assert.Nil(t, scalar.ParseValue("12:60:00"))
	assert.Nil(t, scalar.ParseValue("25:00:00.1234567"))
	assert.Nil(t, scalar.ParseValue("not-a-time"))
}

func TestYearScalar(t *testing.T) {
	scalar := Year()

	assert.Equal(t, "2026", scalar.ParseValue("2026"))
	assert.Equal(t, "0000", scalar.ParseValue(0))
	assert.Equal(t, "2155", scalar.Serialize(int64(2155)))

	assert.Nil(t, scalar.ParseValue("99"))
	assert.Nil(t, scalar.ParseValue(2156))
	assert.Nil(t, scalar.ParseValue(-1))
	assert.Nil(t, scalar.ParseValue("abcd"))
}

func TestBytesScalar(t *testing.T) {
	scalar := Bytes()

	serialized := scalar.Serialize([]byte("hello"))
	assert.Equal(t, base64.StdEncoding.EncodeToString([]byte("hello")), serialized)

	parsed := scalar.ParseValue(base64.StdEncoding.EncodeToString([]byte("world")))
	require.IsType(t, []byte{}, parsed)
	assert.Equal(t, []byte("world"), parsed)

	assert.Equal(t, "", scalar.Serialize([]byte{}))
	require.IsType(t, []byte{}, scalar.ParseValue(""))
	assert.Equal(t, []byte{}, scalar.ParseValue(""))

	assert.Nil(t, scalar.ParseValue("not-base64@@"))
	assert.Nil(t, scalar.ParseLiteral(&ast.StringValue{Value: "not-base64@@"}))
	assert.Equal(t, []byte("ok"), scalar.ParseLiteral(&ast.StringValue{Value: base64.StdEncoding.EncodeToString([]byte("ok"))}))
}

func TestUUIDScalar(t *testing.T) {
	scalar := UUID()

	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", scalar.ParseValue("550E8400-E29B-41D4-A716-446655440000"))
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", scalar.ParseLiteral(&ast.StringValue{Value: "550E8400-E29B-41D4-A716-446655440000"}))
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", scalar.Serialize("550E8400-E29B-41D4-A716-446655440000"))

	assert.Equal(t,
		"550e8400-e29b-41d4-a716-446655440000",
		scalar.Serialize([]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}),
	)

	assert.Nil(t, scalar.ParseValue("not-a-uuid"))
	assert.Nil(t, scalar.Serialize([]byte{0x01, 0x02}))
	assert.Nil(t, scalar.ParseLiteral(&ast.IntValue{Value: "42"}))
}

func TestVectorScalar(t *testing.T) {
	scalar := Vector()

	parsed := scalar.ParseValue([]interface{}{1.0, 2, "3.5"})
	require.IsType(t, []float64{}, parsed)
	assert.Equal(t, []float64{1, 2, 3.5}, parsed)

	serialized := scalar.Serialize("[1,2,3]")
	require.IsType(t, []float64{}, serialized)
	assert.Equal(t, []float64{1, 2, 3}, serialized)

	literal := scalar.ParseLiteral(&ast.ListValue{
		Values: []ast.Value{
			&ast.IntValue{Value: "1"},
			&ast.FloatValue{Value: "2.5"},
		},
	})
	require.IsType(t, []float64{}, literal)
	assert.Equal(t, []float64{1, 2.5}, literal)

	assert.Nil(t, scalar.ParseValue([]interface{}{1, "x"}))
	assert.Nil(t, scalar.ParseValue([]interface{}{math.Inf(1)}))
	assert.Nil(t, scalar.ParseValue("not-json"))
	assert.Nil(t, scalar.ParseLiteral(&ast.StringValue{Value: "nope"}))
}
