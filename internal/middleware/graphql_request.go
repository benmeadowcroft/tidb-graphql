package middleware

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/graphql-go/graphql/language/ast"
	"github.com/graphql-go/graphql/language/parser"
	"github.com/graphql-go/graphql/language/source"
)

type graphQLRequest struct {
	Query         string `json:"query"`
	OperationName string `json:"operationName"`
}

type queryMetadata struct {
	operationType  string
	fieldCount     int
	selectionDepth int
	variableCount  int
}

func extractGraphQLRequest(r *http.Request) (string, string) {
	if r.Method == http.MethodGet {
		return r.URL.Query().Get("query"), r.URL.Query().Get("operationName")
	}

	if r.Method != http.MethodPost {
		return "", ""
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return "", ""
	}
	r.Body = io.NopCloser(bytes.NewReader(body))

	contentType := r.Header.Get("Content-Type")
	if strings.Contains(contentType, "application/graphql") {
		return string(body), ""
	}

	var payload graphQLRequest
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", ""
	}

	return payload.Query, payload.OperationName
}

func extractQueryMetadata(query, operationName string) (*queryMetadata, error) {
	if query == "" {
		return nil, nil
	}

	doc, err := parser.Parse(parser.ParseParams{
		Source: source.NewSource(&source.Source{
			Body: []byte(query),
			Name: "graphql",
		}),
	})
	if err != nil {
		return nil, err
	}

	// Build fragment map for resolving fragment spreads
	fragments := make(map[string]*ast.FragmentDefinition)
	for _, def := range doc.Definitions {
		if frag, ok := def.(*ast.FragmentDefinition); ok {
			fragments[frag.Name.Value] = frag
		}
	}

	// Find the target operation
	var targetOp *ast.OperationDefinition
	var first *ast.OperationDefinition
	for _, def := range doc.Definitions {
		op, ok := def.(*ast.OperationDefinition)
		if !ok {
			continue
		}
		if first == nil {
			first = op
		}
		if operationName != "" && op.Name != nil && op.Name.Value == operationName {
			targetOp = op
			break
		}
	}

	// Use first operation if no name specified or operation not found
	if targetOp == nil && operationName == "" && first != nil {
		targetOp = first
	}

	if targetOp == nil {
		return nil, nil
	}

	// Extract metadata
	metadata := &queryMetadata{
		operationType: string(targetOp.Operation),
		variableCount: len(targetOp.VariableDefinitions),
	}

	// Count fields and depth
	if targetOp.SelectionSet != nil {
		fields, depth := countFieldsAndDepth(targetOp.SelectionSet, fragments, 1, map[string]bool{}, map[string]bool{})
		metadata.fieldCount = fields
		metadata.selectionDepth = depth
	}

	return metadata, nil
}

func countFieldsAndDepth(selectionSet *ast.SelectionSet, fragments map[string]*ast.FragmentDefinition, currentDepth int, visited, inFlight map[string]bool) (fields, maxDepth int) {
	if selectionSet == nil {
		return 0, currentDepth - 1
	}

	maxDepth = currentDepth

	for _, selection := range selectionSet.Selections {
		switch sel := selection.(type) {
		case *ast.Field:
			fields++
			if sel.SelectionSet != nil {
				nestedFields, nestedDepth := countFieldsAndDepth(sel.SelectionSet, fragments, currentDepth+1, visited, inFlight)
				fields += nestedFields
				if nestedDepth > maxDepth {
					maxDepth = nestedDepth
				}
			}

		case *ast.InlineFragment:
			if sel.SelectionSet != nil {
				nestedFields, nestedDepth := countFieldsAndDepth(sel.SelectionSet, fragments, currentDepth, visited, inFlight)
				fields += nestedFields
				if nestedDepth > maxDepth {
					maxDepth = nestedDepth
				}
			}

		case *ast.FragmentSpread:
			fragName := sel.Name.Value
			// Guard against cyclic fragment spreads and double-counting across the traversal.
			if inFlight[fragName] || visited[fragName] {
				continue
			}
			// Track expansion to avoid re-entering the same fragment during recursion.
			inFlight[fragName] = true
			visited[fragName] = true
			if frag, ok := fragments[fragName]; ok && frag.SelectionSet != nil {
				nestedFields, nestedDepth := countFieldsAndDepth(frag.SelectionSet, fragments, currentDepth, visited, inFlight)
				fields += nestedFields
				if nestedDepth > maxDepth {
					maxDepth = nestedDepth
				}
			}
			delete(inFlight, fragName)
		}
	}

	return fields, maxDepth
}
