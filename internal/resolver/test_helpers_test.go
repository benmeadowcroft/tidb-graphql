package resolver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"

	"tidb-graphql/internal/dbexec"
)

type fakeRows struct {
	rows [][]any
	idx  int
	err  error
}

func (r *fakeRows) Next() bool {
	if r.idx >= len(r.rows) {
		return false
	}
	r.idx++
	return true
}

func (r *fakeRows) Scan(dest ...any) error {
	if r.idx == 0 || r.idx > len(r.rows) {
		return errors.New("scan called without advancing rows")
	}
	row := r.rows[r.idx-1]
	if len(row) != len(dest) {
		return fmt.Errorf("scan row has %d values, dest has %d", len(row), len(dest))
	}
	for i, value := range row {
		if err := assignScanValue(dest[i], value); err != nil {
			return err
		}
	}
	return nil
}

func (r *fakeRows) Err() error {
	return r.err
}

func (r *fakeRows) Close() error {
	return nil
}

func assignScanValue(dest any, value any) error {
	switch d := dest.(type) {
	case *interface{}:
		*d = value
		return nil
	case *sql.NullInt64:
		if value == nil {
			d.Valid = false
			d.Int64 = 0
			return nil
		}
		intValue, err := toInt64(value)
		if err != nil {
			return err
		}
		d.Valid = true
		d.Int64 = intValue
		return nil
	case *sql.NullFloat64:
		if value == nil {
			d.Valid = false
			d.Float64 = 0
			return nil
		}
		floatValue, err := toFloat64(value)
		if err != nil {
			return err
		}
		d.Valid = true
		d.Float64 = floatValue
		return nil
	default:
		rv := reflect.ValueOf(dest)
		if rv.Kind() != reflect.Ptr {
			return fmt.Errorf("scan dest must be pointer, got %T", dest)
		}
		if value == nil {
			rv.Elem().Set(reflect.Zero(rv.Elem().Type()))
			return nil
		}
		vv := reflect.ValueOf(value)
		if vv.Type().AssignableTo(rv.Elem().Type()) {
			rv.Elem().Set(vv)
			return nil
		}
		if vv.Type().ConvertibleTo(rv.Elem().Type()) {
			rv.Elem().Set(vv.Convert(rv.Elem().Type()))
			return nil
		}
		return fmt.Errorf("cannot assign %T to %T", value, dest)
	}
}

func toInt64(value any) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		if v > uint64(^uint64(0)>>1) {
			return 0, fmt.Errorf("uint64 out of int64 range: %d", v)
		}
		return int64(v), nil
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	default:
		return 0, fmt.Errorf("unsupported int conversion from %T", value)
	}
}

func toFloat64(value any) (float64, error) {
	switch v := value.(type) {
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	default:
		return 0, fmt.Errorf("unsupported float conversion from %T", value)
	}
}

type fakeExecutor struct {
	responses [][][]any
	calls     int
	args      [][]any
}

func (e *fakeExecutor) QueryContext(_ context.Context, _ string, args ...any) (dbexec.Rows, error) {
	e.calls++
	e.args = append(e.args, args)
	idx := e.calls - 1
	if idx >= len(e.responses) {
		return &fakeRows{}, nil
	}
	return &fakeRows{rows: e.responses[idx]}, nil
}

func (e *fakeExecutor) ExecContext(_ context.Context, _ string, _ ...any) (sql.Result, error) {
	return nil, nil
}

func (e *fakeExecutor) BeginTx(_ context.Context) (dbexec.TxExecutor, error) {
	return nil, errors.New("not implemented")
}
