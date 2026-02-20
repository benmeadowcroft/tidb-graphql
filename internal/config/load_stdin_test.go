package config

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
)

func TestValidateSingleStdinFileSource_AllowsZeroOrOneStdinSource(t *testing.T) {
	t.Run("none", func(t *testing.T) {
		v := viper.New()
		v.Set("database.dsn_file", "/tmp/dsn")
		v.Set("database.mycnf_file", "")
		v.Set("database.password_file", "/tmp/password")
		v.Set("server.admin.auth_token_file", "/tmp/admin-token")

		if err := validateSingleStdinFileSource(v); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("one", func(t *testing.T) {
		v := viper.New()
		v.Set("database.dsn_file", "@-")
		v.Set("database.mycnf_file", "")
		v.Set("database.password_file", "/tmp/password")
		v.Set("server.admin.auth_token_file", "")

		if err := validateSingleStdinFileSource(v); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestValidateSingleStdinFileSource_RejectsMultipleStdinSources(t *testing.T) {
	v := viper.New()
	v.Set("database.dsn_file", "@-")
	v.Set("database.mycnf_file", " @- ")
	v.Set("database.password_file", "/tmp/password")
	v.Set("server.admin.auth_token_file", "@-")

	err := validateSingleStdinFileSource(v)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	msg := err.Error()
	if !strings.Contains(msg, "database.dsn_file") ||
		!strings.Contains(msg, "database.mycnf_file") ||
		!strings.Contains(msg, "server.admin.auth_token_file") {
		t.Fatalf("error message missing expected keys: %s", msg)
	}
}
