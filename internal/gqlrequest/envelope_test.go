package gqlrequest

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDecodeEnvelope_GET(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/graphql?query=query%20%7B%20users%20%7B%20id%20%7D%20%7D&operationName=GetUsers", nil)
	env, err := DecodeEnvelope(req)
	if err != nil {
		t.Fatalf("DecodeEnvelope() error = %v", err)
	}
	if env.Query == "" {
		t.Fatalf("expected query from URL")
	}
	if env.OperationName != "GetUsers" {
		t.Fatalf("operationName = %q, want %q", env.OperationName, "GetUsers")
	}
	if env.DocumentSizeBytes != len(env.Query) {
		t.Fatalf("document_size_bytes = %d, want %d", env.DocumentSizeBytes, len(env.Query))
	}
}

func TestDecodeEnvelope_PostApplicationGraphQL_RewindsBody(t *testing.T) {
	body := "query { users { id } }"
	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/graphql")

	env, err := DecodeEnvelope(req)
	if err != nil {
		t.Fatalf("DecodeEnvelope() error = %v", err)
	}
	if env.Query != body {
		t.Fatalf("query = %q, want %q", env.Query, body)
	}

	rewound, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatalf("failed reading rewound body: %v", err)
	}
	if string(rewound) != body {
		t.Fatalf("rewound body = %q, want %q", string(rewound), body)
	}
}

func TestDecodeEnvelope_PostJSON(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query":"query GetUsers { users { id } }","operationName":"GetUsers","variables":{"limit":5}}`))
	req.Header.Set("Content-Type", "application/json")

	env, err := DecodeEnvelope(req)
	if err != nil {
		t.Fatalf("DecodeEnvelope() error = %v", err)
	}
	if env.Query == "" {
		t.Fatalf("expected query")
	}
	if env.OperationName != "GetUsers" {
		t.Fatalf("operationName = %q, want %q", env.OperationName, "GetUsers")
	}
	if len(env.VariablesRaw) == 0 {
		t.Fatalf("expected variables raw JSON to be captured")
	}
}

func TestDecodeEnvelope_PostMalformedJSON(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query":`))
	req.Header.Set("Content-Type", "application/json")

	_, err := DecodeEnvelope(req)
	if err == nil {
		t.Fatalf("expected error for malformed JSON body")
	}
}
