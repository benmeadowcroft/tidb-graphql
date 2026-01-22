package main

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"fmt"
	"os"
	"os/user"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func main() {
	var currentUser, err = user.Current()
	if err != nil {
		currentUser = &user.User{Username: "user-1"}
	}

	privateKeyPath := flag.String("key", ".auth/jwt_private.pem", "Path to RSA private key (PEM)")
	issuer := flag.String("issuer", "https://localhost:9000", "JWT issuer")
	audience := flag.String("audience", "tidb-graphql", "JWT audience")
	subject := flag.String("subject", currentUser.Username, "JWT subject")
	dbRole := flag.String("db_role", "", "JWT db_role claim (optional)")
	roles := flag.String("roles", "", "JWT roles claim (comma-separated, optional)")
	kid := flag.String("kid", "local-key", "JWT key ID")
	expires := flag.Duration("expires", time.Hour, "Token lifetime (e.g. 1h)")
	flag.Parse()

	privateKey, err := loadPrivateKey(*privateKeyPath)
	if err != nil {
		exitErr(err)
	}

	now := time.Now()
	claims := jwt.MapClaims{
		"iss": *issuer,
		"sub": *subject,
		"aud": splitList(*audience),
		"iat": now.Unix(),
		"exp": now.Add(*expires).Unix(),
		"nbf": now.Add(-1 * time.Minute).Unix(),
	}
	if *dbRole != "" {
		claims["db_role"] = *dbRole
	}
	if *roles != "" {
		claims["roles"] = splitList(*roles)
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = *kid
	signed, err := token.SignedString(privateKey)
	if err != nil {
		exitErr(err)
	}

	fmt.Println(signed)
}

func loadPrivateKey(path string) (*rsa.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read private key: %w", err)
	}

	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("failed to decode private key pem")
	}

	key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err == nil {
		return key, nil
	}

	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %w", err)
	}

	rsaKey, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("unsupported private key type")
	}

	return rsaKey, nil
}

func exitErr(err error) {
	fmt.Fprintln(os.Stderr, err.Error())
	os.Exit(1)
}

func splitList(value string) []string {
	raw := strings.Split(value, ",")
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		trimmed := strings.TrimSpace(item)
		if trimmed == "" {
			continue
		}
		out = append(out, trimmed)
	}
	return out
}
