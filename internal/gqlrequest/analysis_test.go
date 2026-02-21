package gqlrequest

import "testing"

func TestAnalyzeEnvelope_Metadata(t *testing.T) {
	tests := []struct {
		name              string
		query             string
		operationName     string
		wantType          string
		wantFields        int
		wantDepth         int
		wantVars          int
		wantParseErr      bool
		wantSelectionErr  bool
		wantResolvedName  string
		wantOperationHash bool
	}{
		{
			name: "simple query",
			query: `query {
				user {
					id
					name
				}
			}`,
			wantType:          "query",
			wantFields:        3,
			wantDepth:         2,
			wantVars:          0,
			wantResolvedName:  "<anonymous>",
			wantOperationHash: true,
		},
		{
			name: "named operation with variables",
			query: `query GetUser($id: ID!, $includeEmail: Boolean) {
				user(id: $id) {
					id
					name
				}
			}`,
			operationName:     "GetUser",
			wantType:          "query",
			wantFields:        3,
			wantDepth:         2,
			wantVars:          2,
			wantResolvedName:  "GetUser",
			wantOperationHash: true,
		},
		{
			name: "mutation",
			query: `mutation CreateUser($name: String!) {
				createUser(name: $name) {
					id
					name
				}
			}`,
			operationName:     "CreateUser",
			wantType:          "mutation",
			wantFields:        3,
			wantDepth:         2,
			wantVars:          1,
			wantResolvedName:  "CreateUser",
			wantOperationHash: true,
		},
		{
			name: "multiple operations without name is unresolved",
			query: `
				query GetUser { user { id } }
				query GetPosts { posts { id title } }
			`,
			wantSelectionErr: true,
		},
		{
			name:         "malformed query",
			query:        `query { user { `,
			wantParseErr: true,
		},
		{
			name:         "empty query",
			query:        "",
			wantParseErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			analysis := AnalyzeEnvelope(Envelope{
				Query:         tt.query,
				OperationName: tt.operationName,
			})
			if (analysis.ParseError != nil) != tt.wantParseErr {
				t.Fatalf("ParseError presence = %v, want %v (err=%v)", analysis.ParseError != nil, tt.wantParseErr, analysis.ParseError)
			}
			if (analysis.SelectionError != nil) != tt.wantSelectionErr {
				t.Fatalf("SelectionError presence = %v, want %v (err=%v)", analysis.SelectionError != nil, tt.wantSelectionErr, analysis.SelectionError)
			}
			if tt.wantParseErr || tt.wantSelectionErr {
				return
			}
			if analysis.OperationType != tt.wantType {
				t.Fatalf("OperationType = %q, want %q", analysis.OperationType, tt.wantType)
			}
			if analysis.FieldCount != tt.wantFields {
				t.Fatalf("FieldCount = %d, want %d", analysis.FieldCount, tt.wantFields)
			}
			if analysis.SelectionDepth != tt.wantDepth {
				t.Fatalf("SelectionDepth = %d, want %d", analysis.SelectionDepth, tt.wantDepth)
			}
			if analysis.VariableCount != tt.wantVars {
				t.Fatalf("VariableCount = %d, want %d", analysis.VariableCount, tt.wantVars)
			}
			if analysis.OperationName != tt.wantResolvedName {
				t.Fatalf("OperationName = %q, want %q", analysis.OperationName, tt.wantResolvedName)
			}
			if (analysis.OperationHash != "") != tt.wantOperationHash {
				t.Fatalf("OperationHash presence = %v, want %v", analysis.OperationHash != "", tt.wantOperationHash)
			}
		})
	}
}

func TestAnalyzeEnvelope_FragmentCycleSafe(t *testing.T) {
	query := `
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
	`
	analysis := AnalyzeEnvelope(Envelope{Query: query})
	if analysis.ParseError != nil || analysis.SelectionError != nil {
		t.Fatalf("unexpected parse/selection errors: parse=%v selection=%v", analysis.ParseError, analysis.SelectionError)
	}
	if analysis.FieldCount != 3 {
		t.Fatalf("FieldCount = %d, want %d", analysis.FieldCount, 3)
	}
}

func TestOperationHash_WhitespaceAndCommentsInsensitive(t *testing.T) {
	query1 := `
		query GetUsers {
			users { id name }
		}
	`
	query2 := `
		# comment
		query GetUsers { users { id name } }
	`

	a := AnalyzeEnvelope(Envelope{Query: query1, OperationName: "GetUsers"})
	b := AnalyzeEnvelope(Envelope{Query: query2, OperationName: "GetUsers"})
	if a.OperationHash == "" || b.OperationHash == "" {
		t.Fatalf("expected non-empty operation hashes")
	}
	if a.OperationHash != b.OperationHash {
		t.Fatalf("hash mismatch for semantically equivalent queries: %q vs %q", a.OperationHash, b.OperationHash)
	}
}

func TestOperationHash_MultiOperationSelection(t *testing.T) {
	query := `
		query A { users { id } }
		query B { posts { id title } }
	`
	a := AnalyzeEnvelope(Envelope{Query: query, OperationName: "A"})
	b := AnalyzeEnvelope(Envelope{Query: query, OperationName: "B"})
	if a.OperationHash == "" || b.OperationHash == "" {
		t.Fatalf("expected non-empty hashes for selected operations")
	}
	if a.OperationHash == b.OperationHash {
		t.Fatalf("expected different hashes for different selected operations")
	}
}

func TestFramedHashDisambiguatesTuples(t *testing.T) {
	hashA := framedSHA256("ab", "c")
	hashB := framedSHA256("a", "bc")
	if hashA == hashB {
		t.Fatalf("expected framed hash to disambiguate tuple boundaries")
	}
}
