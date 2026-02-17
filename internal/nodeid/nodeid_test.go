package nodeid

import (
	"encoding/base64"
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/sqltype"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	encoded := Encode("User", int64(42))
	typeName, values, err := Decode(encoded)
	require.NoError(t, err)
	assert.Equal(t, "User", typeName)
	require.Len(t, values, 1)
	assert.Equal(t, json.Number("42"), values[0])
}

func TestEncodeDecodeComposite(t *testing.T) {
	encoded := Encode("OrderItem", "A-1", int64(7))
	typeName, values, err := Decode(encoded)
	require.NoError(t, err)
	assert.Equal(t, "OrderItem", typeName)
	require.Len(t, values, 2)
	assert.Equal(t, "A-1", values[0])
	assert.Equal(t, json.Number("7"), values[1])
}

func TestEncodeDecodeRoundTrip_LargeIntPK(t *testing.T) {
	const largeID = int64(5188146770730811493)

	encoded := Encode("User", largeID)
	typeName, values, err := Decode(encoded)
	require.NoError(t, err)
	assert.Equal(t, "User", typeName)
	require.Len(t, values, 1)
	assert.Equal(t, json.Number("5188146770730811493"), values[0])

	col := introspection.Column{Name: "id", DataType: "bigint"}
	parsed, err := ParsePKValue(col, values[0])
	require.NoError(t, err)
	assert.Equal(t, largeID, parsed)
}

func TestDecodeErrors(t *testing.T) {
	_, _, err := Decode("not-base64")
	require.Error(t, err)

	_, _, err = Decode(Encode(""))
	require.Error(t, err)

	_, _, err = Decode(Encode("User"))
	require.Error(t, err)
}

func TestParsePKValue_Int(t *testing.T) {
	col := introspection.Column{Name: "id", DataType: "int"}
	value, err := ParsePKValue(col, 12.0)
	require.NoError(t, err)
	assert.EqualValues(t, 12, value)

	_, err = ParsePKValue(col, 12.5)
	require.Error(t, err)

	_, err = ParsePKValue(col, float64(math.MaxInt64)*2)
	require.Error(t, err)

	_, err = ParsePKValue(col, uint64(math.MaxInt64)+1)
	require.Error(t, err)
}

func TestParsePKValue_BooleanNumeric(t *testing.T) {
	col := introspection.Column{Name: "is_active", DataType: "tinyint", ColumnType: "tinyint(1)"}

	value, err := ParsePKValue(col, 0.0)
	require.NoError(t, err)
	assert.Equal(t, false, value)

	value, err = ParsePKValue(col, 2.0)
	require.NoError(t, err)
	assert.Equal(t, true, value)

	value, err = ParsePKValue(col, "0")
	require.NoError(t, err)
	assert.Equal(t, false, value)

	value, err = ParsePKValue(col, "2")
	require.NoError(t, err)
	assert.Equal(t, true, value)
}

func TestParsePKValue_String(t *testing.T) {
	col := introspection.Column{Name: "code", DataType: "varchar(10)"}
	value, err := ParsePKValue(col, "abc")
	require.NoError(t, err)
	assert.Equal(t, "abc", value)

	_, err = ParsePKValue(col, 12.0)
	require.Error(t, err)
}

func TestParsePKValue_Date(t *testing.T) {
	col := introspection.Column{Name: "day", DataType: "date"}
	value, err := ParsePKValue(col, "2024-01-15")
	require.NoError(t, err)
	assert.Equal(t, time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC), value)

	value, err = ParsePKValue(col, "2024-01-15T10:30:00Z")
	require.NoError(t, err)
	assert.Equal(t, time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC), value)
}

func TestParsePKValue_DateTime(t *testing.T) {
	col := introspection.Column{Name: "ts", DataType: "datetime"}
	value, err := ParsePKValue(col, "2024-01-15T10:30:00Z")
	require.NoError(t, err)
	assert.Equal(t, time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC), value)

	_, err = ParsePKValue(col, "2024-01-15")
	require.Error(t, err)
}

func TestParsePKValue_Bytes(t *testing.T) {
	col := introspection.Column{Name: "key", DataType: "blob"}
	encoded := base64.StdEncoding.EncodeToString([]byte("abc"))
	value, err := ParsePKValue(col, encoded)
	require.NoError(t, err)
	assert.Equal(t, []byte("abc"), value)

	_, err = ParsePKValue(col, "%%%")
	require.Error(t, err)
}

func TestParsePKValue_UUIDText(t *testing.T) {
	col := introspection.Column{
		Name:            "id",
		DataType:        "char",
		ColumnType:      "char(36)",
		OverrideType:    sqltype.TypeUUID,
		HasOverrideType: true,
	}

	value, err := ParsePKValue(col, "550E8400-E29B-41D4-A716-446655440000")
	require.NoError(t, err)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", value)
}

func TestParsePKValue_UUIDBinary(t *testing.T) {
	col := introspection.Column{
		Name:            "id",
		DataType:        "binary",
		ColumnType:      "binary(16)",
		OverrideType:    sqltype.TypeUUID,
		HasOverrideType: true,
	}

	value, err := ParsePKValue(col, "550e8400-e29b-41d4-a716-446655440000")
	require.NoError(t, err)
	require.IsType(t, []byte{}, value)
	assert.Len(t, value.([]byte), 16)
}

func TestEncodeDecodeRoundTrip_UUIDPK(t *testing.T) {
	colText := introspection.Column{
		Name:            "id",
		DataType:        "char",
		ColumnType:      "char(36)",
		OverrideType:    sqltype.TypeUUID,
		HasOverrideType: true,
	}

	id := Encode("Order", "550e8400-e29b-41d4-a716-446655440000")
	typeName, values, err := Decode(id)
	require.NoError(t, err)
	assert.Equal(t, "Order", typeName)
	require.Len(t, values, 1)

	parsed, err := ParsePKValue(colText, values[0])
	require.NoError(t, err)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", parsed)

	colBinary := introspection.Column{
		Name:            "id",
		DataType:        "binary",
		ColumnType:      "binary(16)",
		OverrideType:    sqltype.TypeUUID,
		HasOverrideType: true,
	}
	parsedBinary, err := ParsePKValue(colBinary, values[0])
	require.NoError(t, err)
	require.IsType(t, []byte{}, parsedBinary)
	assert.Len(t, parsedBinary.([]byte), 16)
}
