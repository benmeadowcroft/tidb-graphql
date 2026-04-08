package resolver

import (
	"context"
	"fmt"
	"time"

	"tidb-graphql/internal/asof"
	"tidb-graphql/internal/dbexec"

	"github.com/graphql-go/graphql"
)

const snapshotRowField = "__snapshot_read"

func asOfDirective() *graphql.Directive {
	return graphql.NewDirective(graphql.DirectiveConfig{
		Name:        asof.DirectiveName,
		Description: "Reads the selected root field subtree from an exact TiDB historical snapshot.",
		Locations:   []string{graphql.DirectiveLocationField},
		Args: graphql.FieldConfigArgument{
			asof.ArgTime: &graphql.ArgumentConfig{
				Type: graphql.DateTime,
			},
			asof.ArgOffsetSeconds: &graphql.ArgumentConfig{
				Type: graphql.Int,
			},
		},
	})
}

func (r *Resolver) withSnapshotContext(p graphql.ResolveParams) (graphql.ResolveParams, error) {
	ctx := p.Context
	if inherited, ok := snapshotReadFromSource(p.Source); ok {
		p.Context = dbexec.WithSnapshotRead(ctx, inherited)
		return p, nil
	}

	parentType := p.Info.ParentType
	if parentType == nil || parentType.Name() != "Query" {
		return p, nil
	}

	field := firstFieldAST(p.Info.FieldASTs)
	spec, err := asof.ResolveFieldDirective(field, p.Info.VariableValues, time.Now().UTC())
	if err != nil {
		return p, err
	}
	if spec == nil {
		return p, nil
	}

	p.Context = dbexec.WithSnapshotRead(ctx, dbexec.SnapshotRead{Time: spec.Time})
	return p, nil
}

func snapshotReadFromSource(source any) (dbexec.SnapshotRead, bool) {
	row, ok := source.(map[string]any)
	if !ok {
		return dbexec.SnapshotRead{}, false
	}
	snapshot, ok := row[snapshotRowField].(dbexec.SnapshotRead)
	return snapshot, ok
}

func annotateRowsWithSnapshot(ctx context.Context, rows []map[string]any) {
	if len(rows) == 0 {
		return
	}
	snapshot, ok := dbexec.SnapshotReadFromContext(ctx)
	if !ok {
		return
	}
	for _, row := range rows {
		if row != nil {
			row[snapshotRowField] = snapshot
		}
	}
}

func annotateRowWithSnapshot(ctx context.Context, row map[string]any) map[string]any {
	if row == nil {
		return nil
	}
	snapshot, ok := dbexec.SnapshotReadFromContext(ctx)
	if !ok {
		return row
	}
	row[snapshotRowField] = snapshot
	return row
}

func snapshotIdentityFromContext(ctx context.Context) string {
	snapshot, ok := dbexec.SnapshotReadFromContext(ctx)
	if !ok {
		return ""
	}
	return snapshot.Identity()
}

func snapshotIdentityFromSource(source any) string {
	snapshot, ok := snapshotReadFromSource(source)
	if !ok {
		return ""
	}
	return snapshot.Identity()
}

func snapshotKeyPart(ctx context.Context) string {
	identity := snapshotIdentityFromContext(ctx)
	if identity == "" {
		return "current"
	}
	return fmt.Sprintf("snapshot:%s", identity)
}
