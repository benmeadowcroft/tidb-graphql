package asof

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/graphql-go/graphql/language/ast"
)

func TestResolveDirective_Time(t *testing.T) {
	now := time.Date(2026, 4, 7, 12, 0, 0, 0, time.UTC)
	directive := &ast.Directive{
		Name: &ast.Name{Value: DirectiveName},
		Arguments: []*ast.Argument{
			{
				Name:  &ast.Name{Value: ArgTime},
				Value: &ast.StringValue{Value: "2026-04-01T10:00:00Z"},
			},
		},
	}

	spec, err := ResolveDirective(directive, nil, now)
	if err != nil {
		t.Fatalf("ResolveDirective() error = %v", err)
	}
	if spec == nil {
		t.Fatalf("expected spec")
	}
	if got, want := spec.Time, time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("spec.Time = %v, want %v", got, want)
	}
}

func TestResolveDirective_Offset(t *testing.T) {
	now := time.Date(2026, 4, 7, 12, 0, 0, 0, time.UTC)
	directive := &ast.Directive{
		Name: &ast.Name{Value: DirectiveName},
		Arguments: []*ast.Argument{
			{
				Name:  &ast.Name{Value: ArgOffsetSeconds},
				Value: &ast.IntValue{Value: "-600"},
			},
		},
	}

	spec, err := ResolveDirective(directive, nil, now)
	if err != nil {
		t.Fatalf("ResolveDirective() error = %v", err)
	}
	if spec == nil {
		t.Fatalf("expected spec")
	}
	if got, want := spec.Time, now.Add(-10*time.Minute); !got.Equal(want) {
		t.Fatalf("spec.Time = %v, want %v", got, want)
	}
}

func TestResolveDirective_RejectsInvalidUsage(t *testing.T) {
	now := time.Date(2026, 4, 7, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		directive *ast.Directive
		variables map[string]any
		wantErr   string
	}{
		{
			name: "missing args",
			directive: &ast.Directive{
				Name: directiveASTName(),
			},
			wantErr: "@asOf requires exactly one of: time, offsetSeconds",
		},
		{
			name: "multiple args",
			directive: &ast.Directive{
				Name: directiveASTName(),
				Arguments: []*ast.Argument{
					{Name: &ast.Name{Value: ArgTime}, Value: &ast.StringValue{Value: "2026-04-01T10:00:00Z"}},
					{Name: &ast.Name{Value: ArgOffsetSeconds}, Value: &ast.IntValue{Value: "-10"}},
				},
			},
			wantErr: "@asOf requires exactly one of: time, offsetSeconds",
		},
		{
			name: "future time",
			directive: &ast.Directive{
				Name: directiveASTName(),
				Arguments: []*ast.Argument{
					{Name: &ast.Name{Value: ArgTime}, Value: &ast.StringValue{Value: "2026-04-08T10:00:00Z"}},
				},
			},
			wantErr: "@asOf time must not be in the future",
		},
		{
			name: "positive offset",
			directive: &ast.Directive{
				Name: directiveASTName(),
				Arguments: []*ast.Argument{
					{Name: &ast.Name{Value: ArgOffsetSeconds}, Value: &ast.IntValue{Value: "10"}},
				},
			},
			wantErr: "@asOf offsetSeconds must be less than or equal to 0",
		},
		{
			name: "variable time",
			directive: &ast.Directive{
				Name: directiveASTName(),
				Arguments: []*ast.Argument{
					{Name: &ast.Name{Value: ArgTime}, Value: &ast.Variable{Name: &ast.Name{Value: "t"}}},
				},
			},
			variables: map[string]any{"t": "2026-04-08T10:00:00Z"},
			wantErr:   "@asOf time must not be in the future",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ResolveDirective(tt.directive, tt.variables, now)
			if err == nil || err.Error() != tt.wantErr {
				t.Fatalf("ResolveDirective() error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestValidateOperation_RootOnly(t *testing.T) {
	now := time.Date(2026, 4, 7, 12, 0, 0, 0, time.UTC)
	directive := &ast.Directive{
		Name: directiveASTName(),
		Arguments: []*ast.Argument{
			{Name: &ast.Name{Value: ArgTime}, Value: &ast.StringValue{Value: "2026-04-01T10:00:00Z"}},
		},
	}

	queryOp := &ast.OperationDefinition{
		Operation: "query",
		SelectionSet: &ast.SelectionSet{
			Selections: []ast.Selection{
				&ast.Field{Name: &ast.Name{Value: "orders"}, Directives: []*ast.Directive{directive}},
			},
		},
	}
	if err := ValidateOperation(queryOp, nil, nil, now); err != nil {
		t.Fatalf("ValidateOperation(query) error = %v", err)
	}

	nestedOp := &ast.OperationDefinition{
		Operation: "query",
		SelectionSet: &ast.SelectionSet{
			Selections: []ast.Selection{
				&ast.Field{
					Name: &ast.Name{Value: "orders"},
					SelectionSet: &ast.SelectionSet{
						Selections: []ast.Selection{
							&ast.Field{Name: &ast.Name{Value: "items"}, Directives: []*ast.Directive{directive}},
						},
					},
				},
			},
		},
	}
	if err := ValidateOperation(nestedOp, nil, nil, now); err == nil || err.Error() != "@asOf is only allowed on root query fields" {
		t.Fatalf("ValidateOperation(nested) error = %v", err)
	}

	mutationOp := &ast.OperationDefinition{
		Operation: "mutation",
		SelectionSet: &ast.SelectionSet{
			Selections: []ast.Selection{
				&ast.Field{Name: &ast.Name{Value: "createOrder"}, Directives: []*ast.Directive{directive}},
			},
		},
	}
	if err := ValidateOperation(mutationOp, nil, nil, now); err == nil || err.Error() != "@asOf is only allowed on root query fields" {
		t.Fatalf("ValidateOperation(mutation) error = %v", err)
	}
}

func TestDecodeVariables(t *testing.T) {
	raw := json.RawMessage(`{"offset":-600}`)
	variables, err := DecodeVariables(raw)
	if err != nil {
		t.Fatalf("DecodeVariables() error = %v", err)
	}
	if got := variables["offset"]; got == nil {
		t.Fatalf("expected decoded offset variable")
	}
}

func directiveASTName() *ast.Name {
	return &ast.Name{Value: DirectiveName}
}
