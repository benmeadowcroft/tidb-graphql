package middleware

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"tidb-graphql/internal/gqlrequest"
)

func TestGraphQLRequestValidationMiddleware_RejectsInvalidAsOf(t *testing.T) {
	nextCalled := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nextCalled = true
		w.WriteHeader(http.StatusNoContent)
	})

	handler := GraphQLRequestAnalysisMiddleware(nil)(
		GraphQLRequestValidationMiddleware()(next),
	)

	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query":"query Q { users @asOf(offsetSeconds: 10) { id } }","operationName":"Q"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if nextCalled {
		t.Fatalf("expected validation middleware to stop request")
	}
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}

	var payload struct {
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode error payload: %v", err)
	}
	if len(payload.Errors) != 1 {
		t.Fatalf("expected one error, got %d", len(payload.Errors))
	}
	if got, want := payload.Errors[0].Message, "@asOf offsetSeconds must be less than or equal to 0"; got != want {
		t.Fatalf("message = %q, want %q", got, want)
	}
}

func TestGraphQLRequestValidationMiddleware_PassesValidRequests(t *testing.T) {
	nextCalled := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nextCalled = true
		w.WriteHeader(http.StatusNoContent)
	})

	analysis := &gqlrequest.Analysis{}
	req := httptest.NewRequest(http.MethodPost, "/graphql", nil).WithContext(
		gqlrequest.WithAnalysis(context.Background(), analysis),
	)
	rec := httptest.NewRecorder()

	handler := GraphQLRequestValidationMiddleware()(next)
	handler.ServeHTTP(rec, req)

	if !nextCalled {
		t.Fatalf("expected next handler to be called")
	}
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
}
