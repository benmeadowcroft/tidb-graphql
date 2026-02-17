package introspection

import (
	"testing"

	"tidb-graphql/internal/sqltype"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplyUUIDTypeOverrides(t *testing.T) {
	schema := &Schema{
		Tables: []Table{
			{
				Name: "orders",
				Columns: []Column{
					{Name: "id", DataType: "binary", ColumnType: "binary(16)"},
					{Name: "customer_uuid", DataType: "varchar", ColumnType: "varchar(36)"},
					{Name: "notes", DataType: "varchar", ColumnType: "varchar(255)"},
				},
			},
			{
				Name: "events",
				Columns: []Column{
					{Name: "event_uuid", DataType: "char", ColumnType: "char(36)"},
				},
			},
		},
	}

	err := ApplyUUIDTypeOverrides(schema, map[string][]string{
		"*":      {"*_uuid"},
		"orders": {"id"},
	})
	require.NoError(t, err)

	orders := schema.Tables[0]
	assert.True(t, orders.Columns[0].HasOverrideType)
	assert.Equal(t, sqltype.TypeUUID, orders.Columns[0].OverrideType)
	assert.True(t, orders.Columns[1].HasOverrideType)
	assert.Equal(t, sqltype.TypeUUID, orders.Columns[1].OverrideType)
	assert.False(t, orders.Columns[2].HasOverrideType)

	events := schema.Tables[1]
	assert.True(t, events.Columns[0].HasOverrideType)
	assert.Equal(t, sqltype.TypeUUID, events.Columns[0].OverrideType)
}

func TestApplyUUIDTypeOverrides_TablePatternCaseInsensitive(t *testing.T) {
	schema := &Schema{
		Tables: []Table{
			{
				Name: "Orders",
				Columns: []Column{
					{Name: "id", DataType: "binary", ColumnType: "binary(16)"},
				},
			},
		},
	}

	err := ApplyUUIDTypeOverrides(schema, map[string][]string{
		"orders": {"id"},
	})
	require.NoError(t, err)
	require.True(t, schema.Tables[0].Columns[0].HasOverrideType)
	assert.Equal(t, sqltype.TypeUUID, schema.Tables[0].Columns[0].OverrideType)
}

func TestApplyUUIDTypeOverrides_TableGlobPattern(t *testing.T) {
	schema := &Schema{
		Tables: []Table{
			{
				Name: "order_events",
				Columns: []Column{
					{Name: "event_uuid", DataType: "char", ColumnType: "char(36)"},
				},
			},
		},
	}

	err := ApplyUUIDTypeOverrides(schema, map[string][]string{
		"order_*": {"*_uuid"},
	})
	require.NoError(t, err)
	require.True(t, schema.Tables[0].Columns[0].HasOverrideType)
	assert.Equal(t, sqltype.TypeUUID, schema.Tables[0].Columns[0].OverrideType)
}

func TestApplyUUIDTypeOverrides_InvalidType(t *testing.T) {
	schema := &Schema{
		Tables: []Table{
			{
				Name: "files",
				Columns: []Column{
					{Name: "file_uuid", DataType: "blob", ColumnType: "blob"},
				},
			},
		},
	}

	err := ApplyUUIDTypeOverrides(schema, map[string][]string{
		"*": {"*_uuid"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported SQL type")
}

func TestApplyUUIDTypeOverrides_InvalidBinaryLength(t *testing.T) {
	schema := &Schema{
		Tables: []Table{
			{
				Name: "orders",
				Columns: []Column{
					{Name: "id", DataType: "binary", ColumnType: "binary(8)"},
				},
			},
		},
	}

	err := ApplyUUIDTypeOverrides(schema, map[string][]string{
		"orders": {"id"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "length 16")
}

func TestApplyUUIDTypeOverrides_InvalidTextLength(t *testing.T) {
	schema := &Schema{
		Tables: []Table{
			{
				Name: "orders",
				Columns: []Column{
					{Name: "id", DataType: "char", ColumnType: "char(10)"},
				},
			},
		},
	}

	err := ApplyUUIDTypeOverrides(schema, map[string][]string{
		"orders": {"id"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "length >= 36")
}

func TestEffectiveGraphQLType(t *testing.T) {
	col := Column{DataType: "varchar"}
	assert.Equal(t, sqltype.TypeString, EffectiveGraphQLType(col))

	col = Column{DataType: "tinyint", ColumnType: "tinyint(1)"}
	assert.Equal(t, sqltype.TypeBoolean, EffectiveGraphQLType(col))

	col = Column{DataType: "tinyint", ColumnType: "tinyint(2)"}
	assert.Equal(t, sqltype.TypeInt, EffectiveGraphQLType(col))

	col.OverrideType = sqltype.TypeUUID
	col.HasOverrideType = true
	assert.Equal(t, sqltype.TypeUUID, EffectiveGraphQLType(col))
}

func TestApplyTinyInt1TypeOverrides(t *testing.T) {
	schema := &Schema{
		Tables: []Table{
			{
				Name: "users",
				Columns: []Column{
					{Name: "is_active", DataType: "tinyint", ColumnType: "tinyint(1)"},
					{Name: "tiny_flag", DataType: "tinyint", ColumnType: "tinyint(1)"},
					{Name: "small_flag", DataType: "tinyint", ColumnType: "tinyint(2)"},
				},
			},
		},
	}

	err := ApplyTinyInt1TypeOverrides(schema,
		map[string][]string{"*": {"is_*", "tiny_*"}},
		map[string][]string{"users": {"tiny_*"}},
	)
	require.NoError(t, err)

	cols := schema.Tables[0].Columns
	assert.True(t, cols[0].HasOverrideType)
	assert.Equal(t, sqltype.TypeBoolean, cols[0].OverrideType)

	// int override wins when both bool and int patterns match.
	assert.True(t, cols[1].HasOverrideType)
	assert.Equal(t, sqltype.TypeInt, cols[1].OverrideType)

	assert.False(t, cols[2].HasOverrideType)
}

func TestApplyTinyInt1TypeOverrides_InvalidTarget(t *testing.T) {
	schema := &Schema{
		Tables: []Table{
			{
				Name: "users",
				Columns: []Column{
					{Name: "status", DataType: "tinyint", ColumnType: "tinyint(2)"},
				},
			},
		},
	}

	err := ApplyTinyInt1TypeOverrides(schema,
		map[string][]string{"users": {"status"}},
		nil,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected tinyint(1)")
}

func TestMergePatterns_DeduplicatesNonAdjacent(t *testing.T) {
	patterns := map[string][]string{
		"*":      {"*_uuid", "id"},
		"orders": {"id", "customer_uuid"},
	}

	merged := mergePatterns(patterns, "orders")
	assert.Equal(t, []string{"*_uuid", "id", "customer_uuid"}, merged)
}
