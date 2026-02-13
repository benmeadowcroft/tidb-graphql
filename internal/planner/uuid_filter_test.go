package planner

import (
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/sqltype"
)

func TestBuildColumnFilter_UUIDBinaryEq(t *testing.T) {
	col := introspection.Column{
		Name:            "id",
		DataType:        "binary",
		ColumnType:      "binary(16)",
		OverrideType:    sqltype.TypeUUID,
		HasOverrideType: true,
	}
	conds, err := buildColumnFilter(col, "", map[string]interface{}{
		"eq": "550E8400-E29B-41D4-A716-446655440000",
	})
	require.NoError(t, err)
	require.Len(t, conds, 1)

	_, args, err := sq.Select("1").From("t").Where(conds[0]).ToSql()
	require.NoError(t, err)
	require.Len(t, args, 1)
	require.IsType(t, []byte{}, args[0])
	assert.Len(t, args[0].([]byte), 16)
}

func TestBuildColumnFilter_UUIDTextIn(t *testing.T) {
	col := introspection.Column{
		Name:            "id_text",
		DataType:        "varchar",
		ColumnType:      "varchar(36)",
		OverrideType:    sqltype.TypeUUID,
		HasOverrideType: true,
	}
	conds, err := buildColumnFilter(col, "", map[string]interface{}{
		"in": []interface{}{
			"550E8400-E29B-41D4-A716-446655440000",
			"123e4567-e89b-12d3-a456-426614174000",
		},
	})
	require.NoError(t, err)
	require.Len(t, conds, 1)

	_, args, err := sq.Select("1").From("t").Where(conds[0]).ToSql()
	require.NoError(t, err)
	require.Len(t, args, 2)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", args[0])
	assert.Equal(t, "123e4567-e89b-12d3-a456-426614174000", args[1])
}

func TestBuildColumnFilter_UUIDTextNe(t *testing.T) {
	col := introspection.Column{
		Name:            "id_text",
		DataType:        "varchar",
		ColumnType:      "varchar(36)",
		OverrideType:    sqltype.TypeUUID,
		HasOverrideType: true,
	}
	conds, err := buildColumnFilter(col, "", map[string]interface{}{
		"ne": "550E8400-E29B-41D4-A716-446655440000",
	})
	require.NoError(t, err)
	require.Len(t, conds, 1)

	_, args, err := sq.Select("1").From("t").Where(conds[0]).ToSql()
	require.NoError(t, err)
	require.Len(t, args, 1)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", args[0])
}

func TestBuildColumnFilter_UUIDTextNotIn(t *testing.T) {
	col := introspection.Column{
		Name:            "id_text",
		DataType:        "varchar",
		ColumnType:      "varchar(36)",
		OverrideType:    sqltype.TypeUUID,
		HasOverrideType: true,
	}
	conds, err := buildColumnFilter(col, "", map[string]interface{}{
		"notIn": []interface{}{
			"550E8400-E29B-41D4-A716-446655440000",
			"123e4567-e89b-12d3-a456-426614174000",
		},
	})
	require.NoError(t, err)
	require.Len(t, conds, 1)

	_, args, err := sq.Select("1").From("t").Where(conds[0]).ToSql()
	require.NoError(t, err)
	require.Len(t, args, 2)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", args[0])
	assert.Equal(t, "123e4567-e89b-12d3-a456-426614174000", args[1])
}

func TestBuildColumnFilter_UUIDUnsupportedOperator(t *testing.T) {
	col := introspection.Column{
		Name:            "id",
		DataType:        "binary",
		ColumnType:      "binary(16)",
		OverrideType:    sqltype.TypeUUID,
		HasOverrideType: true,
	}
	_, err := buildColumnFilter(col, "", map[string]interface{}{
		"like": "550e8400-e29b-41d4-a716-446655440000",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not supported for UUID")
}

func TestBuildColumnFilter_UUIDInvalidValue(t *testing.T) {
	col := introspection.Column{
		Name:            "id",
		DataType:        "varchar",
		ColumnType:      "varchar(36)",
		OverrideType:    sqltype.TypeUUID,
		HasOverrideType: true,
	}
	_, err := buildColumnFilter(col, "", map[string]interface{}{
		"eq": "not-a-uuid",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid UUID value")
}
