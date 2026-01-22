package introspection

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
)

// DiscoverRoles returns roles granted to the current database user.
// It attempts the standard MySQL view first and falls back to TiDB's mysql.role_edges table.
func DiscoverRoles(ctx context.Context, db *sql.DB) ([]string, error) {
	roles, err := discoverFromRoleEdges(ctx, db)
	if err == nil {
		return roles, nil
	}

	slog.Debug("role discovery fallback to information_schema",
		slog.String("error", err.Error()),
	)
	roles, fallbackErr := discoverFromInformationSchema(ctx, db)
	if fallbackErr != nil {
		return nil, fmt.Errorf("role discovery failed: %w", fallbackErr)
	}
	return roles, nil
}

func discoverFromInformationSchema(ctx context.Context, db *sql.DB) ([]string, error) {
	query := `
		SELECT ROLE_NAME
		FROM information_schema.applicable_roles
		WHERE GRANTEE = CURRENT_USER()
		ORDER BY ROLE_NAME
	`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	var roles []string
	for rows.Next() {
		var role string
		if err := rows.Scan(&role); err != nil {
			return nil, err
		}
		roles = append(roles, role)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return roles, nil
}

func discoverFromRoleEdges(ctx context.Context, db *sql.DB) ([]string, error) {
	query := `
		SELECT DISTINCT FROM_USER AS role_name
		FROM mysql.role_edges
		WHERE TO_USER = SUBSTRING_INDEX(CURRENT_USER(), '@', 1)
		  AND TO_HOST = SUBSTRING_INDEX(CURRENT_USER(), '@', -1)
		ORDER BY role_name
	`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	var roles []string
	for rows.Next() {
		var role string
		if err := rows.Scan(&role); err != nil {
			return nil, err
		}
		roles = append(roles, role)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return roles, nil
}

// PrivilegeValidationResult contains the results of privilege validation.
type PrivilegeValidationResult struct {
	Valid              bool     // Whether privileges are suitable for role-based authorization
	HasBroadPrivileges bool     // Whether user has overly broad privileges (SELECT on *.* or database.*)
	BroadPrivileges    []string // List of problematic privilege grants
	Warnings           []string // Non-fatal warnings
}

// ValidateRoleBasedAuthPrivileges checks whether the database user's privileges are compatible
// with role-based authorization. For SET ROLE to effectively restrict access, the user should
// NOT have direct SELECT privileges on the target database - only the ability to assume roles.
//
// Returns an error only if the privilege check itself fails, not if privileges are too broad.
func ValidateRoleBasedAuthPrivileges(ctx context.Context, db *sql.DB, targetDatabase string) (*PrivilegeValidationResult, error) {
	rows, err := db.QueryContext(ctx, "SHOW GRANTS FOR CURRENT_USER()")
	if err != nil {
		return nil, fmt.Errorf("failed to query user privileges: %w", err)
	}
	defer rows.Close()

	result := &PrivilegeValidationResult{
		Valid: true,
	}

	for rows.Next() {
		var grant string
		if err := rows.Scan(&grant); err != nil {
			return nil, fmt.Errorf("failed to scan grant: %w", err)
		}

		// Check for SELECT privilege on *.* (all databases)
		if containsSelectPrivilege(grant) && strings.Contains(grant, "ON *.*") {
			result.Valid = false
			result.HasBroadPrivileges = true
			result.BroadPrivileges = append(result.BroadPrivileges, grant)
			continue
		}

		// Check for SELECT privilege on target database.* (all tables in target DB)
		dbPattern := fmt.Sprintf("ON `%s`.*", targetDatabase)
		if containsSelectPrivilege(grant) && strings.Contains(grant, dbPattern) {
			result.Valid = false
			result.HasBroadPrivileges = true
			result.BroadPrivileges = append(result.BroadPrivileges, grant)
			continue
		}

		// Check for ALL PRIVILEGES which includes SELECT
		if strings.Contains(strings.ToUpper(grant), "ALL PRIVILEGES") {
			if strings.Contains(grant, "ON *.*") || strings.Contains(grant, dbPattern) {
				result.Valid = false
				result.HasBroadPrivileges = true
				result.BroadPrivileges = append(result.BroadPrivileges, grant)
			}
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating grants: %w", err)
	}

	return result, nil
}

// containsSelectPrivilege checks if a GRANT statement includes SELECT privilege.
func containsSelectPrivilege(grant string) bool {
	upper := strings.ToUpper(grant)
	// Look for SELECT as a word boundary (not part of another privilege name)
	// GRANT SELECT, ... or GRANT ... SELECT ... ON
	return strings.Contains(upper, "SELECT,") ||
		strings.Contains(upper, "SELECT ") ||
		strings.Contains(upper, " SELECT,") ||
		strings.HasPrefix(upper, "GRANT SELECT")
}
