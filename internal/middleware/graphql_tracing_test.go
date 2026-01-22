package middleware

import (
	"testing"

	"github.com/graphql-go/graphql/language/ast"
)

func TestExtractQueryMetadata(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		operationName string
		want          *queryMetadata
		wantErr       bool
	}{
		{
			name: "simple query with single field",
			query: `query {
				user {
					id
					name
				}
			}`,
			operationName: "",
			want: &queryMetadata{
				operationType:  "query",
				fieldCount:     3, // user, id, name
				selectionDepth: 2,
				variableCount:  0,
			},
			wantErr: false,
		},
		{
			name: "query with variables",
			query: `query GetUser($id: ID!, $includeEmail: Boolean) {
				user(id: $id) {
					id
					name
				}
			}`,
			operationName: "GetUser",
			want: &queryMetadata{
				operationType:  "query",
				fieldCount:     3, // user, id, name
				selectionDepth: 2,
				variableCount:  2, // $id, $includeEmail
			},
			wantErr: false,
		},
		{
			name: "deeply nested query",
			query: `query {
				user {
					id
					posts {
						id
						title
						comments {
							id
							text
							author {
								id
								name
							}
						}
					}
				}
			}`,
			operationName: "",
			want: &queryMetadata{
				operationType:  "query",
				fieldCount:     11, // user, id, posts, id, title, comments, id, text, author, id, name
				selectionDepth: 5,  // user -> posts -> comments -> author -> name
				variableCount:  0,
			},
			wantErr: false,
		},
		{
			name: "query with inline fragment",
			query: `query {
				search {
					... on User {
						id
						name
					}
					... on Post {
						id
						title
					}
				}
			}`,
			operationName: "",
			want: &queryMetadata{
				operationType:  "query",
				fieldCount:     5, // search, id, name, id, title
				selectionDepth: 2,
				variableCount:  0,
			},
			wantErr: false,
		},
		{
			name: "query with fragment spread",
			query: `
				fragment UserFields on User {
					id
					name
					email
				}

				query {
					user {
						...UserFields
						posts {
							id
						}
					}
				}
			`,
			operationName: "",
			want: &queryMetadata{
				operationType:  "query",
				fieldCount:     6, // user, id, name, email, posts, id
				selectionDepth: 3,
				variableCount:  0,
			},
			wantErr: false,
		},
		{
			name: "mutation",
			query: `mutation CreateUser($name: String!) {
				createUser(name: $name) {
					id
					name
				}
			}`,
			operationName: "CreateUser",
			want: &queryMetadata{
				operationType:  "mutation",
				fieldCount:     3, // createUser, id, name
				selectionDepth: 2,
				variableCount:  1,
			},
			wantErr: false,
		},
		{
			name: "subscription",
			query: `subscription {
				userUpdated {
					id
					name
				}
			}`,
			operationName: "",
			want: &queryMetadata{
				operationType:  "subscription",
				fieldCount:     3, // userUpdated, id, name
				selectionDepth: 2,
				variableCount:  0,
			},
			wantErr: false,
		},
		{
			name: "multiple operations - select by name",
			query: `
				query GetUser {
					user {
						id
					}
				}

				query GetPosts {
					posts {
						id
						title
					}
				}
			`,
			operationName: "GetPosts",
			want: &queryMetadata{
				operationType:  "query",
				fieldCount:     3, // posts, id, title
				selectionDepth: 2,
				variableCount:  0,
			},
			wantErr: false,
		},
		{
			name: "query with nested fragments",
			query: `
				fragment AuthorInfo on User {
					id
					name
				}

				fragment PostDetails on Post {
					id
					title
					author {
						...AuthorInfo
					}
				}

				query {
					posts {
						...PostDetails
					}
				}
			`,
			operationName: "",
			want: &queryMetadata{
				operationType:  "query",
				fieldCount:     6, // posts, id, title, author, id, name
				selectionDepth: 3,
				variableCount:  0,
			},
			wantErr: false,
		},
		{
			name: "query with cyclic fragments",
			query: `
				fragment A on User {
					id
					...B
				}

				fragment B on User {
					name
					...A
				}

				query {
					user {
						...A
					}
				}
			`,
			operationName: "",
			want: &queryMetadata{
				operationType:  "query",
				fieldCount:     3, // user, id, name
				selectionDepth: 2,
				variableCount:  0,
			},
			wantErr: false,
		},
		{
			name:          "malformed query",
			query:         `query { user { `,
			operationName: "",
			want:          nil,
			wantErr:       true,
		},
		{
			name:          "empty query",
			query:         "",
			operationName: "",
			want:          nil,
			wantErr:       false, // Parser returns nil for empty query without error
		},
		{
			name: "query with no selections",
			query: `query {
			}`,
			operationName: "",
			want:          nil,
			wantErr:       true, // GraphQL syntax error - empty selection set
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := extractQueryMetadata(tt.query, tt.operationName)
			if (err != nil) != tt.wantErr {
				t.Errorf("extractQueryMetadata() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if got == nil && tt.want != nil {
				t.Errorf("extractQueryMetadata() got = nil, want %+v", tt.want)
				return
			}
			if got != nil && tt.want == nil {
				t.Errorf("extractQueryMetadata() got = %+v, want nil", got)
				return
			}
			if got != nil && tt.want != nil {
				if got.operationType != tt.want.operationType {
					t.Errorf("extractQueryMetadata() operationType = %v, want %v", got.operationType, tt.want.operationType)
				}
				if got.fieldCount != tt.want.fieldCount {
					t.Errorf("extractQueryMetadata() fieldCount = %v, want %v", got.fieldCount, tt.want.fieldCount)
				}
				if got.selectionDepth != tt.want.selectionDepth {
					t.Errorf("extractQueryMetadata() selectionDepth = %v, want %v", got.selectionDepth, tt.want.selectionDepth)
				}
				if got.variableCount != tt.want.variableCount {
					t.Errorf("extractQueryMetadata() variableCount = %v, want %v", got.variableCount, tt.want.variableCount)
				}
			}
		})
	}
}

func TestCountFieldsAndDepth(t *testing.T) {
	tests := []struct {
		name       string
		query      string
		wantFields int
		wantDepth  int
	}{
		{
			name: "flat selection",
			query: `query {
				user {
					id
					name
					email
				}
			}`,
			wantFields: 4, // user, id, name, email
			wantDepth:  2,
		},
		{
			name: "nested selection",
			query: `query {
				user {
					id
					posts {
						id
						title
					}
				}
			}`,
			wantFields: 5, // user, id, posts, id, title
			wantDepth:  3,
		},
		{
			name: "deeply nested",
			query: `query {
				a {
					b {
						c {
							d {
								e
							}
						}
					}
				}
			}`,
			wantFields: 5, // a, b, c, d, e
			wantDepth:  5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metadata, err := extractQueryMetadata(tt.query, "")
			if err != nil {
				t.Fatalf("extractQueryMetadata() error = %v", err)
			}
			if metadata.fieldCount != tt.wantFields {
				t.Errorf("countFieldsAndDepth() fieldCount = %v, want %v", metadata.fieldCount, tt.wantFields)
			}
			if metadata.selectionDepth != tt.wantDepth {
				t.Errorf("countFieldsAndDepth() depth = %v, want %v", metadata.selectionDepth, tt.wantDepth)
			}
		})
	}
}

func TestCountFieldsAndDepthWithFragments(t *testing.T) {
	// Test that fragment spreads don't cause infinite loops
	query := `
		fragment A on User {
			id
			...B
		}

		fragment B on User {
			name
		}

		query {
			user {
				...A
			}
		}
	`

	metadata, err := extractQueryMetadata(query, "")
	if err != nil {
		t.Fatalf("extractQueryMetadata() error = %v", err)
	}

	// Should have: user, id, name
	if metadata.fieldCount != 3 {
		t.Errorf("fieldCount with fragments = %v, want 3", metadata.fieldCount)
	}
}

func TestCountFieldsAndDepthNilSelectionSet(t *testing.T) {
	// Create an empty selection set
	fragments := make(map[string]*ast.FragmentDefinition)
	fields, depth := countFieldsAndDepth(nil, fragments, 1, map[string]bool{}, map[string]bool{})

	if fields != 0 {
		t.Errorf("countFieldsAndDepth(nil) fields = %v, want 0", fields)
	}
	if depth != 0 {
		t.Errorf("countFieldsAndDepth(nil) depth = %v, want 0", depth)
	}
}
