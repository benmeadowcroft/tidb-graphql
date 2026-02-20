package config

import (
	"testing"

	"github.com/spf13/viper"
)

func TestSetDefaults_OIDCCAFile(t *testing.T) {
	v := viper.New()
	setDefaults(v)

	if got := v.GetString("server.auth.oidc_ca_file"); got != "" {
		t.Fatalf("expected empty default for server.auth.oidc_ca_file, got %q", got)
	}
}
