package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

func main() {
	dir := flag.String("dir", ".auth", "Output directory for keys")
	bits := flag.Int("bits", 2048, "RSA key size")
	flag.Parse()

	if err := os.MkdirAll(*dir, 0o700); err != nil {
		exitErr(fmt.Errorf("failed to create dir: %w", err))
	}

	privateKey, err := rsa.GenerateKey(rand.Reader, *bits)
	if err != nil {
		exitErr(fmt.Errorf("failed to generate key: %w", err))
	}

	privatePath := filepath.Join(*dir, "jwt_private.pem")
	publicPath := filepath.Join(*dir, "jwt_public.pem")

	if err := writePEM(privatePath, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(privateKey), 0o600); err != nil {
		exitErr(err)
	}

	publicBytes, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	if err != nil {
		exitErr(fmt.Errorf("failed to marshal public key: %w", err))
	}

	if err := writePEM(publicPath, "PUBLIC KEY", publicBytes, 0o644); err != nil {
		exitErr(err)
	}

	fmt.Printf("Wrote %s and %s\n", privatePath, publicPath)
}

func writePEM(path, pemType string, bytes []byte, perm os.FileMode) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, perm)
	if err != nil {
		return fmt.Errorf("failed to open %s: %w", path, err)
	}

	if err := pem.Encode(file, &pem.Block{Type: pemType, Bytes: bytes}); err != nil {
		_ = file.Close()
		return fmt.Errorf("failed to write %s: %w", path, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("failed to close %s: %w", path, err)
	}
	return nil
}

func exitErr(err error) {
	fmt.Fprintln(os.Stderr, err.Error())
	os.Exit(1)
}
