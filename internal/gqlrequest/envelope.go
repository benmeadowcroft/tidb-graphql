package gqlrequest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
)

// Envelope stores normalized request payload data used for GraphQL analysis.
type Envelope struct {
	Method      string
	ContentType string

	Query         string
	OperationName string
	VariablesRaw  json.RawMessage

	DocumentSizeBytes int
}

// DecodeEnvelope extracts GraphQL payload fields from an HTTP request and rewinds
// the body so downstream handlers can read it again.
func DecodeEnvelope(r *http.Request) (Envelope, error) {
	if r == nil {
		return Envelope{}, fmt.Errorf("request is nil")
	}

	env := Envelope{
		Method:      r.Method,
		ContentType: r.Header.Get("Content-Type"),
	}

	if r.Method == http.MethodGet {
		env.Query = r.URL.Query().Get("query")
		env.OperationName = r.URL.Query().Get("operationName")
		env.DocumentSizeBytes = len(env.Query)
		return env, nil
	}

	if r.Method != http.MethodPost || r.Body == nil {
		return env, nil
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return env, err
	}
	r.Body = io.NopCloser(bytes.NewReader(body))

	mediaType, _, parseErr := mime.ParseMediaType(env.ContentType)
	if parseErr != nil || mediaType == "" {
		mediaType = strings.TrimSpace(env.ContentType)
	}

	switch mediaType {
	case "application/graphql":
		env.Query = string(body)
	default:
		trimmed := bytes.TrimSpace(body)
		if len(trimmed) == 0 {
			break
		}
		var payload struct {
			Query         string          `json:"query"`
			OperationName string          `json:"operationName"`
			Variables     json.RawMessage `json:"variables"`
		}
		if err := json.Unmarshal(trimmed, &payload); err != nil {
			return env, err
		}
		env.Query = payload.Query
		env.OperationName = payload.OperationName
		if len(payload.Variables) > 0 && !bytes.Equal(bytes.TrimSpace(payload.Variables), []byte("null")) {
			env.VariablesRaw = append(json.RawMessage(nil), payload.Variables...)
		}
	}

	env.DocumentSizeBytes = len(env.Query)
	return env, nil
}
