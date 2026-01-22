//go:build integration
// +build integration

package integration

import (
	"bytes"
	"context"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"

	"tidb-graphql/internal/dbexec"
	"tidb-graphql/internal/middleware"
	"tidb-graphql/internal/resolver"
	"tidb-graphql/internal/schemarefresh"
	"tidb-graphql/internal/testutil/tidbcloud"
)

type roleTestRoles struct {
	admin         string
	analyst       string
	viewer        string
	introspection string
	all           []string
}

type graphQLResponse struct {
	Data   json.RawMessage `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

func TestRoleAuthorization(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDB := tidbcloud.NewRoleTestDB(t)
	testDB.LoadSchemaAdmin(t, "../fixtures/role_test_schema.sql")
	testDB.LoadFixturesAdmin(t, "../fixtures/role_test_seed.sql")

	roles := setupTestRoles(t, testDB) // Create/grant roles for the test database user.

	privateKey, publicPath := generateKeypair(t)
	jwksServer := newJWKSServer(t, publicPath, "test-key")
	t.Cleanup(jwksServer.Close)

	handler := buildRoleGraphQLHandler(t, testDB, jwksServer.URL, roles.introspection, roles.all) // OIDC + DB role middleware chain.
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	t.Run("viewer access denied", func(t *testing.T) {
		token := mintTokenWithRole(t, privateKey, jwksServer.URL, "tidb-graphql", "test-key", time.Now().Add(time.Hour), roles.viewer)

		resp := executeGraphQL(t, server.URL, token, `
			{
				auditLogs { id }
			}
		`)
		requireAccessDenied(t, resp)

		resp = executeGraphQL(t, server.URL, token, `
			{
				userAnalytics { id }
			}
		`)
		requireAccessDenied(t, resp)
	})

	t.Run("analyst partial access", func(t *testing.T) {
		token := mintTokenWithRole(t, privateKey, jwksServer.URL, "tidb-graphql", "test-key", time.Now().Add(time.Hour), roles.analyst)

		resp := executeGraphQL(t, server.URL, token, `
			{
				userAnalytics { id }
			}
		`)
		requireNoGraphQLErrors(t, resp)
		requireDataListLen(t, resp, "userAnalytics", 2)

		resp = executeGraphQL(t, server.URL, token, `
			{
				auditLogs { id }
			}
		`)
		requireAccessDenied(t, resp)
	})

	t.Run("admin full access", func(t *testing.T) {
		token := mintTokenWithRole(t, privateKey, jwksServer.URL, "tidb-graphql", "test-key", time.Now().Add(time.Hour), roles.admin)

		resp := executeGraphQL(t, server.URL, token, `
			{
				auditLogs { id }
			}
		`)
		requireNoGraphQLErrors(t, resp)
		requireDataListLen(t, resp, "auditLogs", 2)
	})

	t.Run("concurrent requests keep roles isolated", func(t *testing.T) {
		type testCase struct {
			role      string
			query     string
			expectErr bool
			field     string
			length    int
		}

		cases := []testCase{
			{
				role:      roles.viewer,
				query:     `{ auditLogs { id } }`,
				expectErr: true,
			},
			{
				role:      roles.analyst,
				query:     `{ userAnalytics { id } }`,
				expectErr: false,
				field:     "userAnalytics",
				length:    2,
			},
			{
				role:      roles.admin,
				query:     `{ auditLogs { id } }`,
				expectErr: false,
				field:     "auditLogs",
				length:    2,
			},
			{
				role:      roles.viewer,
				query:     `{ userAnalytics { id } }`,
				expectErr: true,
			},
			{
				role:      roles.admin,
				query:     `{ userAnalytics { id } }`,
				expectErr: false,
				field:     "userAnalytics",
				length:    2,
			},
		}

		var wg sync.WaitGroup
		errCh := make(chan error, len(cases))

		for _, tc := range cases {
			tc := tc
			wg.Add(1)
			go func() {
				defer wg.Done()
				token := mintTokenWithRole(t, privateKey, jwksServer.URL, "tidb-graphql", "test-key", time.Now().Add(time.Hour), tc.role)
				resp := executeGraphQL(t, server.URL, token, tc.query)
				if tc.expectErr {
					if !hasAccessDenied(resp) {
						errCh <- fmt.Errorf("expected access denied for role %s", tc.role)
					}
					return
				}
				if len(resp.Errors) > 0 {
					errCh <- fmt.Errorf("unexpected errors for role %s: %s", tc.role, resp.Errors[0].Message)
					return
				}
				if tc.field != "" {
					if err := assertDataListLen(resp, tc.field, tc.length); err != nil {
						errCh <- err
					}
				}
			}()
		}

		wg.Wait()
		close(errCh)

		for err := range errCh {
			require.NoError(t, err)
		}
	})

	t.Run("sequential requests reset role", func(t *testing.T) {
		adminToken := mintTokenWithRole(t, privateKey, jwksServer.URL, "tidb-graphql", "test-key", time.Now().Add(time.Hour), roles.admin)
		viewerToken := mintTokenWithRole(t, privateKey, jwksServer.URL, "tidb-graphql", "test-key", time.Now().Add(time.Hour), roles.viewer)

		resp := executeGraphQL(t, server.URL, adminToken, `{ auditLogs { id } }`)
		requireNoGraphQLErrors(t, resp)
		requireDataListLen(t, resp, "auditLogs", 2)

		resp = executeGraphQL(t, server.URL, viewerToken, `{ auditLogs { id } }`)
		requireAccessDenied(t, resp)

		resp = executeGraphQL(t, server.URL, adminToken, `{ auditLogs { id } }`)
		requireNoGraphQLErrors(t, resp)
		requireDataListLen(t, resp, "auditLogs", 2)
	})
}

func setupTestRoles(t *testing.T, testDB *tidbcloud.RoleTestDB) roleTestRoles {
	t.Helper()

	suffix := fmt.Sprintf("%d", time.Now().UnixMilli())
	roles := roleTestRoles{
		admin:         "test_admin_" + suffix,
		analyst:       "test_analyst_" + suffix,
		viewer:        "test_viewer_" + suffix,
		introspection: "test_introspect_" + suffix,
	}
	roles.all = []string{roles.admin, roles.analyst, roles.viewer}

	createRole := func(role string) {
		_, err := testDB.AdminDB.Exec(fmt.Sprintf("CREATE ROLE `%s`", role))
		require.NoError(t, err)
	}
	createRole(roles.admin)
	createRole(roles.analyst)
	createRole(roles.viewer)
	createRole(roles.introspection)

	grantSelect := func(role string, table string) {
		stmt := fmt.Sprintf("GRANT SELECT ON `%s`.`%s` TO `%s`", testDB.DatabaseName, table, role)
		_, err := testDB.AdminDB.Exec(stmt)
		require.NoError(t, err)
	}

	grantSelect(roles.admin, "users")
	grantSelect(roles.admin, "posts")
	grantSelect(roles.admin, "user_analytics")
	grantSelect(roles.admin, "audit_logs")

	grantSelect(roles.analyst, "users")
	grantSelect(roles.analyst, "posts")
	grantSelect(roles.analyst, "user_analytics")

	grantSelect(roles.viewer, "users")
	grantSelect(roles.viewer, "posts")

	_, err := testDB.AdminDB.Exec(fmt.Sprintf("GRANT SELECT ON `%s`.* TO `%s`", testDB.DatabaseName, roles.introspection))
	require.NoError(t, err)

	runtimeUser := runtimeUserForGrant(t, testDB)

	for _, role := range roles.all {
		stmt := fmt.Sprintf("GRANT `%s` TO %s", role, runtimeUser)
		_, err := testDB.AdminDB.Exec(stmt)
		require.NoError(t, err)

		// Validate role is usable.
		_, err = testDB.RuntimeDB.Exec(fmt.Sprintf("SET ROLE `%s`", role))
		require.NoError(t, err)
		_, err = testDB.RuntimeDB.Exec("SET ROLE DEFAULT;")
		require.NoError(t, err)
	}

	stmt := fmt.Sprintf("GRANT `%s` TO %s", roles.introspection, runtimeUser)
	_, err = testDB.AdminDB.Exec(stmt)
	require.NoError(t, err)

	t.Cleanup(func() {
		for _, role := range append(roles.all, roles.introspection) {
			_, err := testDB.AdminDB.Exec(fmt.Sprintf("DROP ROLE IF EXISTS `%s`", role))
			if err != nil {
				t.Logf("Warning: failed to drop role %s: %v", role, err)
			}
		}
	})

	return roles
}

func runtimeUserForGrant(t *testing.T, testDB *tidbcloud.RoleTestDB) string {
	t.Helper()
	return fmt.Sprintf("'%s'@'%s'", testDB.RuntimeUser, testDB.RuntimeHost)
}

func buildRoleGraphQLHandler(t *testing.T, testDB *tidbcloud.RoleTestDB, issuerURL, introspectionRole string, availableRoles []string) http.Handler {
	t.Helper()

	executor := dbexec.NewRoleExecutor(dbexec.RoleExecutorConfig{
		DB:           testDB.RuntimeDB,
		DatabaseName: testDB.DatabaseName,
		RoleFromCtx: func(ctx context.Context) (string, bool) {
			role, ok := middleware.DBRoleFromContext(ctx)
			return role.Role, ok && role.Validated
		},
		AllowedRoles: availableRoles,
		ValidateRole: true,
	})

	manager, err := schemarefresh.NewManager(schemarefresh.Config{
		DB:                testDB.RuntimeDB,
		DatabaseName:      testDB.DatabaseName,
		Executor:          executor,
		MinInterval:       time.Minute,
		MaxInterval:       time.Minute * 5,
		IntrospectionRole: introspectionRole,
	})
	require.NoError(t, err)

	baseHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		manager.Handler().ServeHTTP(w, r)
	})

	batchingHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := resolver.NewBatchingContext(r.Context())
		baseHandler.ServeHTTP(w, r.WithContext(ctx))
	})

	authMiddleware, err := middleware.OIDCAuthMiddleware(middleware.OIDCAuthConfig{
		Enabled:       true,
		IssuerURL:     issuerURL,
		Audience:      "tidb-graphql",
		ClockSkew:     time.Minute,
		SkipTLSVerify: true,
	}, nil)
	require.NoError(t, err)

	dbRoleHandler := middleware.DBRoleMiddleware("", true, availableRoles)(batchingHandler)
	return authMiddleware(dbRoleHandler)
}

func executeGraphQL(t *testing.T, serverURL, token, query string) graphQLResponse {
	t.Helper()

	payload, err := json.Marshal(map[string]string{"query": query})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, serverURL, bytes.NewReader(payload))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	var gqlResp graphQLResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&gqlResp))
	return gqlResp
}

func mintTokenWithRole(t *testing.T, privateKey *rsa.PrivateKey, issuer, audience, kid string, expiresAt time.Time, dbRole string) string {
	t.Helper()

	claims := struct {
		jwt.RegisteredClaims
		DBRole string `json:"db_role"`
	}{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    issuer,
			Subject:   "user-1",
			Audience:  jwt.ClaimStrings{audience},
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ExpiresAt: jwt.NewNumericDate(expiresAt),
			NotBefore: jwt.NewNumericDate(time.Now().Add(-time.Minute)),
		},
		DBRole: dbRole,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = kid
	signed, err := token.SignedString(privateKey)
	require.NoError(t, err)
	return signed
}

func requireAccessDenied(t *testing.T, resp graphQLResponse) {
	t.Helper()
	if hasAccessDenied(resp) {
		return
	}
	t.Fatalf("expected access denied error, got %+v", resp.Errors)
}

func hasAccessDenied(resp graphQLResponse) bool {
	for _, err := range resp.Errors {
		if strings.Contains(err.Message, "access denied") {
			return true
		}
	}
	return false
}

func requireNoGraphQLErrors(t *testing.T, resp graphQLResponse) {
	t.Helper()
	require.Empty(t, resp.Errors)
}

func requireDataListLen(t *testing.T, resp graphQLResponse, field string, expected int) {
	t.Helper()
	require.NoError(t, assertDataListLen(resp, field, expected))
}

func assertDataListLen(resp graphQLResponse, field string, expected int) error {
	var data map[string]interface{}
	if len(resp.Data) == 0 {
		return fmt.Errorf("missing data for field %s", field)
	}
	if err := json.Unmarshal(resp.Data, &data); err != nil {
		return fmt.Errorf("failed to decode data: %w", err)
	}
	raw, ok := data[field]
	if !ok {
		return fmt.Errorf("missing data field %s", field)
	}
	list, ok := raw.([]interface{})
	if !ok {
		return fmt.Errorf("unexpected data type for field %s", field)
	}
	if len(list) != expected {
		return fmt.Errorf("expected %d items for %s, got %d", expected, field, len(list))
	}
	return nil
}
