package uuidutil

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// ParseString parses common UUID string formats and returns a normalized lower-case UUID.
func ParseString(raw string) (uuid.UUID, string, error) {
	parsed, err := uuid.Parse(strings.TrimSpace(raw))
	if err != nil {
		return uuid.Nil, "", fmt.Errorf("invalid UUID value")
	}
	return parsed, strings.ToLower(parsed.String()), nil
}

// ParseBytes parses RFC-order UUID bytes and returns a normalized lower-case UUID.
func ParseBytes(raw []byte) (uuid.UUID, string, error) {
	parsed, err := uuid.FromBytes(raw)
	if err != nil {
		return uuid.Nil, "", fmt.Errorf("invalid UUID bytes")
	}
	return parsed, strings.ToLower(parsed.String()), nil
}

// ToBytes returns UUID bytes in RFC order.
func ToBytes(u uuid.UUID) []byte {
	out := make([]byte, len(u))
	copy(out, u[:])
	return out
}

// IsBinaryStorageType reports whether a SQL type stores UUID values as raw bytes.
func IsBinaryStorageType(dataType string) bool {
	baseType := strings.ToLower(strings.TrimSpace(dataType))
	return baseType == "binary" || baseType == "varbinary"
}
