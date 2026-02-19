package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAdminTokenAuthMiddleware_MissingHeaderReturnsUnauthorized(t *testing.T) {
	mw, err := AdminTokenAuthMiddleware(AdminTokenAuthConfig{Token: "secret-token"})
	if err != nil {
		t.Fatalf("unexpected middleware creation error: %v", err)
	}

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodPost, "/admin/reload-schema", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	assert.Equal(t, `{"error":"unauthorized"}`, rec.Body.String())
}

func TestAdminTokenAuthMiddleware_InvalidHeaderReturnsUnauthorized(t *testing.T) {
	mw, err := AdminTokenAuthMiddleware(AdminTokenAuthConfig{Token: "secret-token"})
	if err != nil {
		t.Fatalf("unexpected middleware creation error: %v", err)
	}

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodPost, "/admin/reload-schema", nil)
	req.Header.Set(defaultAdminTokenHeader, "wrong-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Equal(t, `{"error":"unauthorized"}`, rec.Body.String())
}

func TestAdminTokenAuthMiddleware_ValidHeaderInvokesNext(t *testing.T) {
	mw, err := AdminTokenAuthMiddleware(AdminTokenAuthConfig{Token: "secret-token"})
	if err != nil {
		t.Fatalf("unexpected middleware creation error: %v", err)
	}

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodPost, "/admin/reload-schema", nil)
	req.Header.Set(defaultAdminTokenHeader, "secret-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestAdminTokenAuthMiddleware_SetsAuthContextOnSuccess(t *testing.T) {
	mw, err := AdminTokenAuthMiddleware(AdminTokenAuthConfig{Token: "secret-token"})
	if err != nil {
		t.Fatalf("unexpected middleware creation error: %v", err)
	}

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authCtx, ok := AuthFromContext(r.Context())
		assert.True(t, ok)
		assert.Equal(t, "admin_token", authCtx.Subject)
		assert.Equal(t, "admin_token", authCtx.Issuer)
		assert.Equal(t, "admin_token", authCtx.Claims["auth_method"])
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodPost, "/admin/reload-schema", nil)
	req.Header.Set(defaultAdminTokenHeader, "secret-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestAdminTokenAuthMiddleware_RequiresTokenConfig(t *testing.T) {
	_, err := AdminTokenAuthMiddleware(AdminTokenAuthConfig{})
	assert.Error(t, err)
}
