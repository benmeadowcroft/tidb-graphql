package planner

import (
	"testing"

	"github.com/graphql-go/graphql/language/ast"
	"github.com/stretchr/testify/require"
)

func TestEstimateCostNestedLists(t *testing.T) {
	// users(limit:2) { posts(limit:3) { comments(limit:4) { id } } }
	field := &ast.Field{
		Name: &ast.Name{Value: "users"},
		Arguments: []*ast.Argument{
			{
				Name:  &ast.Name{Value: "limit"},
				Value: &ast.IntValue{Value: "2"},
			},
		},
		SelectionSet: &ast.SelectionSet{
			Selections: []ast.Selection{
				&ast.Field{
					Name: &ast.Name{Value: "posts"},
					Arguments: []*ast.Argument{
						{
							Name:  &ast.Name{Value: "limit"},
							Value: &ast.IntValue{Value: "3"},
						},
					},
					SelectionSet: &ast.SelectionSet{
						Selections: []ast.Selection{
							&ast.Field{
								Name: &ast.Name{Value: "comments"},
								Arguments: []*ast.Argument{
									{
										Name:  &ast.Name{Value: "limit"},
										Value: &ast.IntValue{Value: "4"},
									},
								},
								SelectionSet: &ast.SelectionSet{
									Selections: []ast.Selection{
										&ast.Field{Name: &ast.Name{Value: "id"}},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	cost := EstimateCost(field, nil, DefaultListLimit)
	require.Equal(t, 4, cost.Depth)
	require.Equal(t, 56, cost.Rows)
	require.Equal(t, 33, cost.Complexity)
}

func TestEstimateCostScalarOnly(t *testing.T) {
	field := &ast.Field{
		Name: &ast.Name{Value: "version"},
		SelectionSet: &ast.SelectionSet{
			Selections: []ast.Selection{
				&ast.Field{Name: &ast.Name{Value: "id"}},
			},
		},
	}

	cost := EstimateCost(field, nil, DefaultListLimit)
	require.Equal(t, 2, cost.Depth)
	require.Equal(t, 2, cost.Rows)
	require.Equal(t, 2, cost.Complexity)
}
