package gqlrequest

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/graphql-go/graphql/language/ast"
	"github.com/graphql-go/graphql/language/printer"
)

const anonymousOperationName = "<anonymous>"

func canonicalOperationAndHash(op *ast.OperationDefinition, fragments map[string]*ast.FragmentDefinition) (string, string, error) {
	if op == nil {
		return "", "", fmt.Errorf("operation is nil")
	}

	fragmentNames := referencedFragmentNames(op.SelectionSet, fragments)
	definitions := make([]ast.Node, 0, 1+len(fragmentNames))
	definitions = append(definitions, op)
	for _, name := range fragmentNames {
		fragment, ok := fragments[name]
		if !ok || fragment == nil {
			return "", "", fmt.Errorf("fragment %q not found", name)
		}
		definitions = append(definitions, fragment)
	}

	printed := printer.Print(ast.NewDocument(&ast.Document{Definitions: definitions}))
	canonicalDoc, ok := printed.(string)
	if !ok {
		return "", "", fmt.Errorf("unexpected canonical document type %T", printed)
	}
	hash := framedSHA256(canonicalDoc, effectiveOperationName(op))
	return canonicalDoc, hash, nil
}

func referencedFragmentNames(root *ast.SelectionSet, fragments map[string]*ast.FragmentDefinition) []string {
	if root == nil || len(fragments) == 0 {
		return nil
	}

	visited := map[string]bool{}
	collectReferencedFragments(root, fragments, visited)

	names := make([]string, 0, len(visited))
	for name := range visited {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func collectReferencedFragments(selectionSet *ast.SelectionSet, fragments map[string]*ast.FragmentDefinition, visited map[string]bool) {
	if selectionSet == nil {
		return
	}

	for _, selection := range selectionSet.Selections {
		switch sel := selection.(type) {
		case *ast.Field:
			collectReferencedFragments(sel.SelectionSet, fragments, visited)
		case *ast.InlineFragment:
			collectReferencedFragments(sel.SelectionSet, fragments, visited)
		case *ast.FragmentSpread:
			name := ""
			if sel.Name != nil {
				name = sel.Name.Value
			}
			if name == "" || visited[name] {
				continue
			}
			visited[name] = true
			if fragment, ok := fragments[name]; ok && fragment != nil {
				collectReferencedFragments(fragment.SelectionSet, fragments, visited)
			}
		}
	}
}

func effectiveOperationName(op *ast.OperationDefinition) string {
	if op == nil || op.Name == nil || op.Name.Value == "" {
		return anonymousOperationName
	}
	return op.Name.Value
}

func framedSHA256(parts ...string) string {
	hash := sha256.New()
	for _, part := range parts {
		_, _ = fmt.Fprintf(hash, "%d:%s|", len(part), part)
	}
	return hex.EncodeToString(hash.Sum(nil))
}
