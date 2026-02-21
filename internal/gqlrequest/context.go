package gqlrequest

import "context"

type analysisContextKey struct{}
type execMetaContextKey struct{}

// ExecMeta captures request-scoped execution metadata.
type ExecMeta struct {
	Role        string
	Fingerprint string

	OperationName string
	OperationType string
	OperationHash string
}

// WithAnalysis stores GraphQL request analysis in context.
func WithAnalysis(ctx context.Context, analysis *Analysis) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, analysisContextKey{}, analysis)
}

// AnalysisFromContext retrieves GraphQL request analysis from context.
func AnalysisFromContext(ctx context.Context) *Analysis {
	if ctx == nil {
		return nil
	}
	analysis, _ := ctx.Value(analysisContextKey{}).(*Analysis)
	return analysis
}

// WithExecMeta stores immutable execution metadata in context.
func WithExecMeta(ctx context.Context, meta ExecMeta) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, execMetaContextKey{}, meta)
}

// ExecMetaFromContext retrieves execution metadata from context.
func ExecMetaFromContext(ctx context.Context) (ExecMeta, bool) {
	if ctx == nil {
		return ExecMeta{}, false
	}
	meta, ok := ctx.Value(execMetaContextKey{}).(ExecMeta)
	return meta, ok
}
