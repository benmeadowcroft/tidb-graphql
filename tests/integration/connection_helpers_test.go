//go:build integration
// +build integration

package integration

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func requireCollectionNodes(t *testing.T, data map[string]interface{}, field string) []interface{} {
	t.Helper()

	raw, ok := data[field]
	require.True(t, ok, "expected field %q in response data", field)

	switch value := raw.(type) {
	case []interface{}:
		// Backward-compatible fallback for any non-connection list responses.
		return value
	case map[string]interface{}:
		nodes, ok := value["nodes"].([]interface{})
		require.True(t, ok, "expected %q connection to include nodes[]", field)
		return nodes
	default:
		require.FailNowf(t, "invalid collection payload", "field %q had unexpected type %T", field, raw)
		return nil
	}
}
