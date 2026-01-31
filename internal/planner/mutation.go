package planner

import (
	"fmt"

	sq "github.com/Masterminds/squirrel"

	"tidb-graphql/internal/introspection"
	"tidb-graphql/internal/sqlutil"
)

// PlanInsert builds SQL for inserting a single row with the provided columns.
func PlanInsert(table introspection.Table, columns []string, values []interface{}) (SQLQuery, error) {
	if len(columns) == 0 {
		query := fmt.Sprintf("INSERT INTO %s () VALUES ()", sqlutil.QuoteIdentifier(table.Name))
		return SQLQuery{SQL: query, Args: nil}, nil
	}

	quotedCols := make([]string, len(columns))
	for i, col := range columns {
		quotedCols[i] = sqlutil.QuoteIdentifier(col)
	}

	builder := sq.Insert(sqlutil.QuoteIdentifier(table.Name)).
		Columns(quotedCols...).
		Values(values...).
		PlaceholderFormat(sq.Question)

	query, args, err := builder.ToSql()
	if err != nil {
		return SQLQuery{}, err
	}

	return SQLQuery{SQL: query, Args: args}, nil
}

// PlanUpdate builds SQL for updating a single row by primary key.
func PlanUpdate(table introspection.Table, set map[string]interface{}, pkValues map[string]interface{}) (SQLQuery, error) {
	if len(set) == 0 {
		return SQLQuery{}, fmt.Errorf("update set cannot be empty")
	}

	if err := validatePKValues(table, pkValues); err != nil {
		return SQLQuery{}, err
	}

	update := sq.Update(sqlutil.QuoteIdentifier(table.Name))
	setMap := make(map[string]interface{}, len(set))
	for col, val := range set {
		setMap[sqlutil.QuoteIdentifier(col)] = val
	}
	update = update.SetMap(setMap)

	where := sq.Eq{}
	for col, val := range pkValues {
		where[sqlutil.QuoteIdentifier(col)] = val
	}
	update = update.Where(where)

	query, args, err := update.PlaceholderFormat(sq.Question).ToSql()
	if err != nil {
		return SQLQuery{}, err
	}

	return SQLQuery{SQL: query, Args: args}, nil
}

// PlanDelete builds SQL for deleting a single row by primary key.
func PlanDelete(table introspection.Table, pkValues map[string]interface{}) (SQLQuery, error) {
	if err := validatePKValues(table, pkValues); err != nil {
		return SQLQuery{}, err
	}

	deleteBuilder := sq.Delete(sqlutil.QuoteIdentifier(table.Name))
	where := sq.Eq{}
	for col, val := range pkValues {
		where[sqlutil.QuoteIdentifier(col)] = val
	}
	deleteBuilder = deleteBuilder.Where(where)

	query, args, err := deleteBuilder.PlaceholderFormat(sq.Question).ToSql()
	if err != nil {
		return SQLQuery{}, err
	}

	return SQLQuery{SQL: query, Args: args}, nil
}

func validatePKValues(table introspection.Table, pkValues map[string]interface{}) error {
	// Defensive check: ensure all PK columns are provided to prevent partial WHERE clauses
	pkCols := introspection.PrimaryKeyColumns(table)
	if len(pkValues) != len(pkCols) {
		return fmt.Errorf("pkValues count (%d) does not match primary key column count (%d)", len(pkValues), len(pkCols))
	}
	for _, col := range pkCols {
		if _, ok := pkValues[col.Name]; !ok {
			return fmt.Errorf("missing primary key column %q in pkValues", col.Name)
		}
	}
	return nil
}
