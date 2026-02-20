package config

import (
	"strings"
	"testing"

	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
)

func TestUnmarshalExact_RejectsRemovedOIDCSkipTLSVerifyKey(t *testing.T) {
	v := viper.New()
	v.SetConfigType("yaml")

	configYAML := `
server:
  auth:
    oidc_enabled: true
    oidc_issuer_url: https://issuer.example.com
    oidc_audience: tidb-graphql
    oidc_skip_tls_verify: true
`

	if err := v.ReadConfig(strings.NewReader(configYAML)); err != nil {
		t.Fatalf("failed to read config yaml: %v", err)
	}

	var cfg Config
	err := v.UnmarshalExact(
		&cfg,
		viper.DecodeHook(
			mapstructure.ComposeDecodeHookFunc(
				mapstructure.StringToTimeDurationHookFunc(),
				stringToStringSliceHookFunc(","),
			),
		),
	)
	if err == nil {
		t.Fatal("expected unmarshal error for removed oidc_skip_tls_verify key")
	}
	if !strings.Contains(err.Error(), "oidc_skip_tls_verify") {
		t.Fatalf("expected error to mention oidc_skip_tls_verify, got: %v", err)
	}
}
