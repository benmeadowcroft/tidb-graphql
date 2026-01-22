package introspection

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestDiscoverRoles(t *testing.T) {
	tests := []struct {
		name          string
		setupMock     func(sqlmock.Sqlmock)
		expectedRoles []string
		expectError   bool
	}{
		{
			name: "successful role discovery from mysql.role_edges",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"role_name"}).
					AddRow("app_admin").
					AddRow("app_analyst").
					AddRow("app_viewer")
				mock.ExpectQuery("SELECT DISTINCT FROM_USER AS role_name FROM mysql.role_edges").
					WillReturnRows(rows)
			},
			expectedRoles: []string{"app_admin", "app_analyst", "app_viewer"},
			expectError:   false,
		},
		{
			name: "no roles granted",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"role_name"})
				mock.ExpectQuery("SELECT DISTINCT FROM_USER AS role_name FROM mysql.role_edges").
					WillReturnRows(rows)
			},
			expectedRoles: []string{},
			expectError:   false,
		},
		{
			name: "fallback to information_schema when role_edges fails",
			setupMock: func(mock sqlmock.Sqlmock) {
				// First query fails (role_edges not accessible)
				mock.ExpectQuery("SELECT DISTINCT FROM_USER AS role_name FROM mysql.role_edges").
					WillReturnError(sql.ErrNoRows)

				// Second query succeeds (information_schema fallback)
				rows := sqlmock.NewRows([]string{"ROLE_NAME"}).
					AddRow("standard_role")
				mock.ExpectQuery("SELECT ROLE_NAME FROM information_schema.applicable_roles").
					WillReturnRows(rows)
			},
			expectedRoles: []string{"standard_role"},
			expectError:   false,
		},
		{
			name: "both queries fail",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT DISTINCT FROM_USER AS role_name FROM mysql.role_edges").
					WillReturnError(sql.ErrConnDone)
				mock.ExpectQuery("SELECT ROLE_NAME FROM information_schema.applicable_roles").
					WillReturnError(sql.ErrConnDone)
			},
			expectedRoles: nil,
			expectError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("failed to create mock db: %v", err)
			}
			defer db.Close()

			tt.setupMock(mock)

			roles, err := DiscoverRoles(context.Background(), db)

			if tt.expectError {
				if err == nil {
					t.Fatal("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(roles) != len(tt.expectedRoles) {
				t.Fatalf("expected %d roles, got %d", len(tt.expectedRoles), len(roles))
			}

			for i, expected := range tt.expectedRoles {
				if roles[i] != expected {
					t.Errorf("role[%d]: expected %q, got %q", i, expected, roles[i])
				}
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unfulfilled expectations: %v", err)
			}
		})
	}
}

func TestDiscoverFromRoleEdges(t *testing.T) {
	tests := []struct {
		name          string
		setupMock     func(sqlmock.Sqlmock)
		expectedRoles []string
		expectError   bool
	}{
		{
			name: "successful query",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"role_name"}).
					AddRow("app_admin").
					AddRow("app_analyst")
				mock.ExpectQuery("SELECT DISTINCT FROM_USER AS role_name FROM mysql.role_edges").
					WillReturnRows(rows)
			},
			expectedRoles: []string{"app_admin", "app_analyst"},
			expectError:   false,
		},
		{
			name: "query execution error",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT DISTINCT FROM_USER AS role_name FROM mysql.role_edges").
					WillReturnError(sql.ErrNoRows)
			},
			expectedRoles: nil,
			expectError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("failed to create mock db: %v", err)
			}
			defer db.Close()

			tt.setupMock(mock)

			roles, err := discoverFromRoleEdges(context.Background(), db)

			if tt.expectError {
				if err == nil {
					t.Fatal("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(roles) != len(tt.expectedRoles) {
				t.Fatalf("expected %d roles, got %d", len(tt.expectedRoles), len(roles))
			}

			for i, expected := range tt.expectedRoles {
				if roles[i] != expected {
					t.Errorf("role[%d]: expected %q, got %q", i, expected, roles[i])
				}
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unfulfilled expectations: %v", err)
			}
		})
	}
}

func TestValidateRoleBasedAuthPrivileges(t *testing.T) {
	tests := []struct {
		name           string
		targetDatabase string
		grants         []string
		expectValid    bool
		expectBroad    bool
	}{
		{
			name:           "no broad privileges - role only",
			targetDatabase: "mydb",
			grants: []string{
				"GRANT USAGE ON *.* TO 'testuser'@'%'",
				"GRANT `app_viewer`@`%` TO 'testuser'@'%'",
			},
			expectValid: true,
			expectBroad: false,
		},
		{
			name:           "SELECT on all databases",
			targetDatabase: "mydb",
			grants: []string{
				"GRANT SELECT ON *.* TO 'testuser'@'%'",
			},
			expectValid: false,
			expectBroad: true,
		},
		{
			name:           "SELECT on target database",
			targetDatabase: "mydb",
			grants: []string{
				"GRANT SELECT ON `mydb`.* TO 'testuser'@'%'",
			},
			expectValid: false,
			expectBroad: true,
		},
		{
			name:           "SELECT on different database is OK",
			targetDatabase: "mydb",
			grants: []string{
				"GRANT SELECT ON `otherdb`.* TO 'testuser'@'%'",
			},
			expectValid: true,
			expectBroad: false,
		},
		{
			name:           "ALL PRIVILEGES on all databases",
			targetDatabase: "mydb",
			grants: []string{
				"GRANT ALL PRIVILEGES ON *.* TO 'testuser'@'%'",
			},
			expectValid: false,
			expectBroad: true,
		},
		{
			name:           "multiple privileges including SELECT on all",
			targetDatabase: "mydb",
			grants: []string{
				"GRANT SELECT,INSERT,UPDATE,DELETE ON *.* TO 'testuser'@'%'",
			},
			expectValid: false,
			expectBroad: true,
		},
		{
			name:           "INSERT only on all is OK",
			targetDatabase: "mydb",
			grants: []string{
				"GRANT INSERT,UPDATE ON *.* TO 'testuser'@'%'",
			},
			expectValid: true,
			expectBroad: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("failed to create mock db: %v", err)
			}
			defer db.Close()

			rows := sqlmock.NewRows([]string{"grant"})
			for _, grant := range tt.grants {
				rows.AddRow(grant)
			}
			mock.ExpectQuery("SHOW GRANTS FOR CURRENT_USER").WillReturnRows(rows)

			result, err := ValidateRoleBasedAuthPrivileges(context.Background(), db, tt.targetDatabase)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if result.Valid != tt.expectValid {
				t.Errorf("expected Valid=%v, got %v", tt.expectValid, result.Valid)
			}

			if result.HasBroadPrivileges != tt.expectBroad {
				t.Errorf("expected HasBroadPrivileges=%v, got %v", tt.expectBroad, result.HasBroadPrivileges)
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unfulfilled expectations: %v", err)
			}
		})
	}
}

func TestContainsSelectPrivilege(t *testing.T) {
	tests := []struct {
		grant    string
		expected bool
	}{
		{"GRANT SELECT ON *.* TO 'user'@'%'", true},
		{"GRANT SELECT, INSERT ON *.* TO 'user'@'%'", true},
		{"GRANT INSERT,SELECT,UPDATE ON *.* TO 'user'@'%'", true},
		{"GRANT INSERT ON *.* TO 'user'@'%'", false},
		{"GRANT UPDATE,DELETE ON *.* TO 'user'@'%'", false},
		{"GRANT USAGE ON *.* TO 'user'@'%'", false},
		{"GRANT `role`@`%` TO 'user'@'%'", false},
	}

	for _, tt := range tests {
		t.Run(tt.grant, func(t *testing.T) {
			result := containsSelectPrivilege(tt.grant)
			if result != tt.expected {
				t.Errorf("containsSelectPrivilege(%q) = %v, want %v", tt.grant, result, tt.expected)
			}
		})
	}
}

func TestDiscoverFromInformationSchema(t *testing.T) {
	tests := []struct {
		name          string
		setupMock     func(sqlmock.Sqlmock)
		expectedRoles []string
		expectError   bool
	}{
		{
			name: "successful query",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"ROLE_NAME"}).
					AddRow("role1").
					AddRow("role2")
				mock.ExpectQuery("SELECT ROLE_NAME FROM information_schema.applicable_roles").
					WillReturnRows(rows)
			},
			expectedRoles: []string{"role1", "role2"},
			expectError:   false,
		},
		{
			name: "empty result",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"ROLE_NAME"})
				mock.ExpectQuery("SELECT ROLE_NAME FROM information_schema.applicable_roles").
					WillReturnRows(rows)
			},
			expectedRoles: []string{},
			expectError:   false,
		},
		{
			name: "query error",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT ROLE_NAME FROM information_schema.applicable_roles").
					WillReturnError(sql.ErrConnDone)
			},
			expectedRoles: nil,
			expectError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("failed to create mock db: %v", err)
			}
			defer db.Close()

			tt.setupMock(mock)

			roles, err := discoverFromInformationSchema(context.Background(), db)

			if tt.expectError {
				if err == nil {
					t.Fatal("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(roles) != len(tt.expectedRoles) {
				t.Fatalf("expected %d roles, got %d", len(tt.expectedRoles), len(roles))
			}

			for i, expected := range tt.expectedRoles {
				if roles[i] != expected {
					t.Errorf("role[%d]: expected %q, got %q", i, expected, roles[i])
				}
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unfulfilled expectations: %v", err)
			}
		})
	}
}
