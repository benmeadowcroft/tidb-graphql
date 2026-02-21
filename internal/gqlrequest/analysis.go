package gqlrequest

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/graphql-go/graphql/language/ast"
	"github.com/graphql-go/graphql/language/parser"
	"github.com/graphql-go/graphql/language/source"
)

// Analysis stores parsed and derived GraphQL request metadata.
type Analysis struct {
	Envelope               Envelope
	RequestedOperationName string

	Document  *ast.Document
	Fragments map[string]*ast.FragmentDefinition
	Operation *ast.OperationDefinition

	OperationName string
	OperationType string

	FieldCount     int
	SelectionDepth int
	VariableCount  int

	CanonicalOperation string
	OperationHash      string

	DecodeError     error
	ParseError      error
	SelectionError  error
	CanonicalizeErr error
}

// AnalyzeRequest decodes and analyzes a GraphQL request payload.
func AnalyzeRequest(r *http.Request) *Analysis {
	envelope, err := DecodeEnvelope(r)
	analysis := AnalyzeEnvelope(envelope)
	if err != nil {
		analysis.DecodeError = err
	}
	return analysis
}

// AnalyzeEnvelope parses and analyzes a normalized request envelope.
func AnalyzeEnvelope(env Envelope) *Analysis {
	analysis := &Analysis{
		Envelope:               env,
		RequestedOperationName: env.OperationName,
		Fragments:              map[string]*ast.FragmentDefinition{},
	}

	if strings.TrimSpace(env.Query) == "" {
		return analysis
	}

	doc, err := parser.Parse(parser.ParseParams{
		Source: source.NewSource(&source.Source{
			Body: []byte(env.Query),
			Name: "graphql",
		}),
	})
	if err != nil {
		analysis.ParseError = err
		return analysis
	}

	analysis.Document = doc
	analysis.Fragments = buildFragmentMap(doc)

	op, selectionErr := selectOperation(doc, env.OperationName)
	if selectionErr != nil {
		analysis.SelectionError = selectionErr
		return analysis
	}
	if op == nil {
		analysis.SelectionError = fmt.Errorf("no operation selected")
		return analysis
	}

	analysis.Operation = op
	analysis.OperationName = effectiveOperationName(op)
	analysis.OperationType = string(op.Operation)
	analysis.VariableCount = len(op.VariableDefinitions)

	fields, depth := countFieldsAndDepth(op.SelectionSet, analysis.Fragments, 1, map[string]bool{}, map[string]bool{})
	analysis.FieldCount = fields
	analysis.SelectionDepth = depth

	canonical, hash, canonicalErr := canonicalOperationAndHash(op, analysis.Fragments)
	if canonicalErr != nil {
		analysis.CanonicalizeErr = canonicalErr
		return analysis
	}
	analysis.CanonicalOperation = canonical
	analysis.OperationHash = hash

	return analysis
}

func buildFragmentMap(doc *ast.Document) map[string]*ast.FragmentDefinition {
	fragments := map[string]*ast.FragmentDefinition{}
	if doc == nil {
		return fragments
	}
	for _, def := range doc.Definitions {
		fragment, ok := def.(*ast.FragmentDefinition)
		if !ok || fragment == nil || fragment.Name == nil || fragment.Name.Value == "" {
			continue
		}
		fragments[fragment.Name.Value] = fragment
	}
	return fragments
}

func selectOperation(doc *ast.Document, operationName string) (*ast.OperationDefinition, error) {
	if doc == nil {
		return nil, fmt.Errorf("document is nil")
	}

	operations := make([]*ast.OperationDefinition, 0)
	for _, def := range doc.Definitions {
		op, ok := def.(*ast.OperationDefinition)
		if ok && op != nil {
			operations = append(operations, op)
		}
	}

	if operationName != "" {
		for _, op := range operations {
			if op.Name != nil && op.Name.Value == operationName {
				return op, nil
			}
		}
		return nil, fmt.Errorf("unknown operation named %q", operationName)
	}

	if len(operations) == 1 {
		return operations[0], nil
	}
	if len(operations) == 0 {
		return nil, fmt.Errorf("request does not include an operation")
	}
	return nil, fmt.Errorf("operationName is required when request has multiple operations")
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
			nestedFields, nestedDepth := countFieldsAndDepth(sel.SelectionSet, fragments, currentDepth, visited, inFlight)
			fields += nestedFields
			if nestedDepth > maxDepth {
				maxDepth = nestedDepth
			}
		case *ast.FragmentSpread:
			name := ""
			if sel.Name != nil {
				name = sel.Name.Value
			}
			if name == "" || inFlight[name] || visited[name] {
				continue
			}
			inFlight[name] = true
			visited[name] = true
			if fragment, ok := fragments[name]; ok && fragment != nil {
				nestedFields, nestedDepth := countFieldsAndDepth(fragment.SelectionSet, fragments, currentDepth, visited, inFlight)
				fields += nestedFields
				if nestedDepth > maxDepth {
					maxDepth = nestedDepth
				}
			}
			delete(inFlight, name)
		}
	}

	return fields, maxDepth
}
