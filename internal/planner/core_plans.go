package planner

import (
	"errors"
	"fmt"

	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/sqlutil"

	sq "github.com/Masterminds/squirrel"
)

// ErrNoPrimaryKey indicates a required primary key is missing for a batch plan.
var ErrNoPrimaryKey = errors.New("no primary key")

// BatchParentAlias is the column alias used to return parent keys in batch queries.
const BatchParentAlias = "__batch_parent_id"

const batchParentAliasPrefix = "__batch_parent_"

// ParentTuple represents an ordered composite parent key used in batch plans.
type ParentTuple struct {
	Values []interface{}
}

// BatchParentAliases returns the extra scan aliases emitted by batch SQL.
func BatchParentAliases(columnCount int) []string {
	if columnCount <= 1 {
		return []string{BatchParentAlias}
	}
	aliases := make([]string, columnCount)
	for i := 0; i < columnCount; i++ {
		aliases[i] = batchParentAliasPrefix + fmt.Sprint(i)
	}
	return aliases
}

// SQLQuery represents a planned SQL statement with bound args.
type SQLQuery struct {
	SQL  string
	Args []interface{}
}

// PlanTableByPK builds the SQL for a single-column primary key lookup.
func PlanTableByPK(table introspection.Table, columns []introspection.Column, pk *introspection.Column, pkValue interface{}) (SQLQuery, error) {
	query, args, err := sq.Select(columnNames(table, columns)...).
		From(sqlutil.QuoteIdentifier(table.Name)).
		Where(sq.Eq{sqlutil.QuoteIdentifier(pk.Name): pkValue}).
		PlaceholderFormat(sq.Question).
		ToSql()
	if err != nil {
		return SQLQuery{}, err
	}

	return SQLQuery{SQL: query, Args: args}, nil
}

// PlanTableByPKColumns builds the SQL for a composite primary key lookup.
func PlanTableByPKColumns(table introspection.Table, columns []introspection.Column, pkCols []introspection.Column, values map[string]interface{}) (SQLQuery, error) {
	whereClause := sq.Eq{}
	for _, pk := range pkCols {
		value, ok := values[pk.Name]
		if !ok {
			return SQLQuery{}, fmt.Errorf("missing value for primary key column %s", pk.Name)
		}
		whereClause[sqlutil.QuoteIdentifier(pk.Name)] = value
	}

	query, args, err := sq.Select(columnNames(table, columns)...).
		From(sqlutil.QuoteIdentifier(table.Name)).
		Where(whereClause).
		PlaceholderFormat(sq.Question).
		ToSql()
	if err != nil {
		return SQLQuery{}, err
	}

	return SQLQuery{SQL: query, Args: args}, nil
}

// PlanUniqueKeyLookup builds the SQL for a unique index lookup.
func PlanUniqueKeyLookup(table introspection.Table, columns []introspection.Column, idx introspection.Index, values map[string]interface{}) (SQLQuery, error) {
	// Build WHERE clause for all columns in the unique index
	whereClause := sq.Eq{}
	for _, colName := range idx.Columns {
		value, ok := values[colName]
		if !ok {
			return SQLQuery{}, fmt.Errorf("missing value for unique key column %s", colName)
		}
		whereClause[sqlutil.QuoteIdentifier(colName)] = value
	}

	query, args, err := sq.Select(columnNames(table, columns)...).
		From(sqlutil.QuoteIdentifier(table.Name)).
		Where(whereClause).
		PlaceholderFormat(sq.Question).
		ToSql()
	if err != nil {
		return SQLQuery{}, err
	}

	return SQLQuery{SQL: query, Args: args}, nil
}

// PlanManyToOne builds SQL for a many-to-one lookup (FK -> parent table), including composite mappings.
func PlanManyToOne(relatedTable introspection.Table, columns []introspection.Column, remoteColumns []string, fkValues []interface{}) (SQLQuery, error) {
	if len(remoteColumns) == 0 || len(remoteColumns) != len(fkValues) {
		return SQLQuery{}, fmt.Errorf("many-to-one mapping requires equal remote columns and values")
	}

	builder := sq.Select(columnNames(relatedTable, columns)...).
		From(sqlutil.QuoteIdentifier(relatedTable.Name))
	if len(remoteColumns) == 1 {
		builder = builder.Where(sq.Eq{sqlutil.QuoteIdentifier(remoteColumns[0]): fkValues[0]})
	} else {
		whereClause := sq.Eq{}
		for i, col := range remoteColumns {
			whereClause[sqlutil.QuoteIdentifier(col)] = fkValues[i]
		}
		builder = builder.Where(whereClause)
	}

	query, args, err := builder.PlaceholderFormat(sq.Question).ToSql()
	if err != nil {
		return SQLQuery{}, err
	}
	return SQLQuery{SQL: query, Args: args}, nil
}
