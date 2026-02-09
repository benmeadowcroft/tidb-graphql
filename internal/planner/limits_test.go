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

func TestEstimateCostConnection_EdgesNode(t *testing.T) {
	// conn(first:2) { edges { node { id } } }
	// Should match: list(limit:2) { id } → depth=2, rows=4, complexity=3
	field := connectionField("conn", "2",
		edgesWithNode(&ast.Field{Name: &ast.Name{Value: "id"}}),
	)

	cost := EstimateCost(field, nil, DefaultListLimit)
	require.Equal(t, 2, cost.Depth)
	require.Equal(t, 4, cost.Rows)
	require.Equal(t, 3, cost.Complexity)
}

func TestEstimateCostConnection_Nodes(t *testing.T) {
	// conn(first:2) { nodes { id } }
	// Should match: list(limit:2) { id } → depth=2, rows=4, complexity=3
	field := connectionField("conn", "2",
		nodesField(&ast.Field{Name: &ast.Name{Value: "id"}}),
	)

	cost := EstimateCost(field, nil, DefaultListLimit)
	require.Equal(t, 2, cost.Depth)
	require.Equal(t, 4, cost.Rows)
	require.Equal(t, 3, cost.Complexity)
}

func TestEstimateCostConnection_PageInfoIgnored(t *testing.T) {
	// conn(first:2) { pageInfo { hasNextPage } totalCount edges { node { id } } }
	// pageInfo and totalCount should not affect cost.
	field := connectionField("conn", "2",
		&ast.Field{
			Name: &ast.Name{Value: "pageInfo"},
			SelectionSet: &ast.SelectionSet{
				Selections: []ast.Selection{
					&ast.Field{Name: &ast.Name{Value: "hasNextPage"}},
				},
			},
		},
		&ast.Field{Name: &ast.Name{Value: "totalCount"}},
		edgesWithNode(&ast.Field{Name: &ast.Name{Value: "id"}}),
	)

	cost := EstimateCost(field, nil, DefaultListLimit)
	require.Equal(t, 2, cost.Depth)
	require.Equal(t, 4, cost.Rows)
	require.Equal(t, 3, cost.Complexity)
}

func TestEstimateCostConnection_NestedConnections(t *testing.T) {
	// Nested connections should produce the same cost as nested lists.
	// usersConnection(first:2) { edges { node {
	//   postsConnection(first:3) { edges { node {
	//     commentsConnection(first:4) { edges { node { id } } }
	//   } } }
	// } } }
	// Equivalent list: users(limit:2) { posts(limit:3) { comments(limit:4) { id } } }
	// Expected: depth=4, rows=56, complexity=33
	comments := connectionField("commentsConnection", "4",
		edgesWithNode(&ast.Field{Name: &ast.Name{Value: "id"}}),
	)
	posts := connectionField("postsConnection", "3",
		edgesWithNode(comments),
	)
	field := connectionField("usersConnection", "2",
		edgesWithNode(posts),
	)

	cost := EstimateCost(field, nil, DefaultListLimit)
	require.Equal(t, 4, cost.Depth)
	require.Equal(t, 56, cost.Rows)
	require.Equal(t, 33, cost.Complexity)
}

func TestEstimateCostConnection_OnlyPageInfo(t *testing.T) {
	// conn(first:2) { pageInfo { hasNextPage } }
	// No data fields selected — treated as a leaf.
	field := connectionField("conn", "2",
		&ast.Field{
			Name: &ast.Name{Value: "pageInfo"},
			SelectionSet: &ast.SelectionSet{
				Selections: []ast.Selection{
					&ast.Field{Name: &ast.Name{Value: "hasNextPage"}},
				},
			},
		},
	)

	cost := EstimateCost(field, nil, DefaultListLimit)
	require.Equal(t, 1, cost.Depth)
	require.Equal(t, 2, cost.Rows)
	require.Equal(t, 2, cost.Complexity)
}

func TestEstimateCostConnection_EdgesCursorIgnored(t *testing.T) {
	// conn(first:2) { edges { cursor node { id } } }
	// cursor field inside edges should be ignored.
	field := connectionField("conn", "2",
		&ast.Field{
			Name: &ast.Name{Value: "edges"},
			SelectionSet: &ast.SelectionSet{
				Selections: []ast.Selection{
					&ast.Field{Name: &ast.Name{Value: "cursor"}},
					&ast.Field{
						Name: &ast.Name{Value: "node"},
						SelectionSet: &ast.SelectionSet{
							Selections: []ast.Selection{
								&ast.Field{Name: &ast.Name{Value: "id"}},
							},
						},
					},
				},
			},
		},
	)

	cost := EstimateCost(field, nil, DefaultListLimit)
	require.Equal(t, 2, cost.Depth)
	require.Equal(t, 4, cost.Rows)
	require.Equal(t, 3, cost.Complexity)
}

// --- test helpers for building connection AST nodes ---

// connectionField builds an ast.Field with a "first" argument and the given child selections.
func connectionField(name, first string, children ...ast.Selection) *ast.Field {
	return &ast.Field{
		Name: &ast.Name{Value: name},
		Arguments: []*ast.Argument{
			{
				Name:  &ast.Name{Value: "first"},
				Value: &ast.IntValue{Value: first},
			},
		},
		SelectionSet: &ast.SelectionSet{
			Selections: children,
		},
	}
}

// edgesWithNode builds an "edges { node { ...dataFields } }" selection.
func edgesWithNode(dataFields ...ast.Selection) *ast.Field {
	return &ast.Field{
		Name: &ast.Name{Value: "edges"},
		SelectionSet: &ast.SelectionSet{
			Selections: []ast.Selection{
				&ast.Field{
					Name: &ast.Name{Value: "node"},
					SelectionSet: &ast.SelectionSet{
						Selections: dataFields,
					},
				},
			},
		},
	}
}

// nodesField builds a "nodes { ...dataFields }" selection.
func nodesField(dataFields ...ast.Selection) *ast.Field {
	return &ast.Field{
		Name: &ast.Name{Value: "nodes"},
		SelectionSet: &ast.SelectionSet{
			Selections: dataFields,
		},
	}
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
