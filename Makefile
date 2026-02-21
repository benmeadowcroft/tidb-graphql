.PHONY: help build clean test test-unit test-integration test-coverage test-race lint jwt-keys token-viewer token-admin container-build compose-up compose-down compose-reset compose-validate

# Default target
help:
	@echo "Available targets:"
	@echo "  build              - Build the tidb-graphql binary"
	@echo "  test               - Run all tests (unit + integration)"
	@echo "  test-unit          - Run unit tests only (fast)"
	@echo "  test-integration   - Run integration tests (requires TiDB Cloud)"
	@echo "  test-coverage      - Run tests with coverage report"
	@echo "  test-race          - Run tests with race detector"
	@echo "  clean              - Remove build artifacts"
	@echo "  fmt                - Format code"
	@echo "  lint               - Run linter"
	@echo "  jwt-keys           - Generate local JWT keypair in .auth/"
	@echo "  token-viewer       - Mint app_viewer token from scenario JWKS /dev/token"
	@echo "  token-admin        - Mint app_admin token from scenario JWKS /dev/token"
	@echo "  container-build    - Build container image locally (podman/docker)"
	@echo "  compose-up         - Start development environment (TiDB + tidb-graphql)"
	@echo "  compose-down       - Stop development environment"
	@echo "  compose-reset      - Stop and remove all data (fresh start)"
	@echo "  compose-validate   - Validate docker/podman compose files"

# Load test environment variables from .env.test if it exists
-include .env.test

# Keep Go caches in-repo for sandboxed environments
export GOCACHE := $(CURDIR)/.cache/go-build
export GOMODCACHE := $(CURDIR)/.cache/go-mod

# Build metadata (can be overridden by env)
VERSION ?= $(shell cat VERSION 2>/dev/null || echo dev)
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)

# Export variables loaded from .env.test to shell commands.
export

# Build the server binary
build:
	@echo "Building tidb-graphql..."
	@mkdir -p bin
	go build -ldflags "-X main.Version=$(VERSION) -X main.Commit=$(COMMIT)" -o bin/tidb-graphql ./cmd/server

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	rm -rf bin/
	rm -f coverage.out coverage.html

# Run all tests
test: test-unit test-integration

# Run unit tests only (fast, no external dependencies)
test-unit:
	@echo "Running unit tests..."
	go test -short -v ./internal/...

# Run integration tests (requires TiDB Cloud credentials)
test-integration:
	@echo "Checking TiDB Cloud credentials..."
	@if [ -z "$$TIDB_HOST" ]; then \
		echo "Error: TiDB credentials not set."; \
		echo ""; \
		echo "To run integration tests, you need to set TiDB environment variables:"; \
		echo "  export TIDB_HOST=your-cluster.tidbcloud.com"; \
		echo "  export TIDB_USER=your.user"; \
		echo "  export TIDB_PASSWORD=your-password"; \
		echo ""; \
		echo "Or create a .env.test file (see .env.test.example)"; \
		exit 1; \
	fi
	@echo "Running integration tests..."
	go test -v -tags=integration ./tests/integration/...

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Run tests with race detector
test-race:
	@echo "Running tests with race detector..."
	go test -race ./...

# Format code
fmt:
	go fmt ./...

# Run linter
lint:
	@command -v golangci-lint >/dev/null 2>&1 || { \
		echo "golangci-lint is not installed. Install from https://golangci-lint.run/usage/install/"; \
		exit 1; \
	}
	golangci-lint run

# Generate local JWT keypair for auth testing
jwt-keys:
	go run ./scripts/jwt-generate-keys

# Scenario-backed JWT minting defaults
SCENARIO ?= oidc-roles
OIDC_COMPOSE_FILE ?= examples/compose/$(SCENARIO)/docker-compose.yml
OIDC_ENV_FILE ?= examples/compose/$(SCENARIO)/.env
TOKEN_ENDPOINT ?= https://localhost:9000/dev/token
DEFAULT_DEV_ADMIN_TOKEN ?= dev-admin-token
TOKEN_CURL_TLS_FLAGS ?= -k

token-viewer:
	@admin_token="$${DEV_ADMIN_TOKEN:-$$(grep -E '^DEV_ADMIN_TOKEN=' $(OIDC_ENV_FILE) 2>/dev/null | tail -n1 | cut -d= -f2-)}"; \
	admin_token="$${admin_token:-$(DEFAULT_DEV_ADMIN_TOKEN)}"; \
	admin_token="$$(printf '%s' "$$admin_token" | sed -e 's/^"//' -e 's/"$$//' -e "s/^'//" -e "s/'$$//")"; \
	curl $(TOKEN_CURL_TLS_FLAGS) -fsS -X POST $(TOKEN_ENDPOINT) \
		-H "X-Admin-Token: $$admin_token" \
		-H "Content-Type: application/json" \
		-H "Accept: text/plain" \
		-d '{"db_role":"app_viewer"}'

token-admin:
	@admin_token="$${DEV_ADMIN_TOKEN:-$$(grep -E '^DEV_ADMIN_TOKEN=' $(OIDC_ENV_FILE) 2>/dev/null | tail -n1 | cut -d= -f2-)}"; \
	admin_token="$${admin_token:-$(DEFAULT_DEV_ADMIN_TOKEN)}"; \
	admin_token="$$(printf '%s' "$$admin_token" | sed -e 's/^"//' -e 's/"$$//' -e "s/^'//" -e "s/'$$//")"; \
	curl $(TOKEN_CURL_TLS_FLAGS) -fsS -X POST $(TOKEN_ENDPOINT) \
		-H "X-Admin-Token: $$admin_token" \
		-H "Content-Type: application/json" \
		-H "Accept: text/plain" \
		-d '{"db_role":"app_admin"}'

# Auto-detect container tool (podman preferred, docker fallback)
CONTAINER_TOOL ?= $(shell command -v podman 2>/dev/null || command -v docker 2>/dev/null)
QUICKSTART_COMPOSE_FILE ?= examples/compose/quickstart/docker-compose.yml

# Build container image locally (podman or docker)
container-build:
	$(CONTAINER_TOOL) build \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		-t tidb-graphql:local .

# Start development environment (TiDB + tidb-graphql)
compose-up:
	$(CONTAINER_TOOL) compose -f $(QUICKSTART_COMPOSE_FILE) up --build

# Stop development environment
compose-down:
	$(CONTAINER_TOOL) compose -f $(QUICKSTART_COMPOSE_FILE) down

# Stop and remove all data (fresh start)
compose-reset:
	$(CONTAINER_TOOL) compose -f $(QUICKSTART_COMPOSE_FILE) down -v

# Validate compose files for the auto-detected container engine
compose-validate:
	@if [ -z "$(CONTAINER_TOOL)" ]; then \
		echo "Error: No container engine found. Install podman or docker."; \
		exit 1; \
	fi
	@echo "Validating compose files with $(CONTAINER_TOOL)..."
	@$(CONTAINER_TOOL) compose config >/dev/null
	@$(CONTAINER_TOOL) compose -f examples/compose/quickstart/docker-compose.yml config >/dev/null
	@$(CONTAINER_TOOL) compose -f examples/compose/quickstart-db-zero/docker-compose.yml config >/dev/null
	@$(CONTAINER_TOOL) compose -f examples/compose/remote-db/docker-compose.yml config >/dev/null
	@$(CONTAINER_TOOL) compose -f examples/compose/oidc-roles/docker-compose.yml config >/dev/null
	@$(CONTAINER_TOOL) compose -f examples/compose/otel/docker-compose.yml config >/dev/null
	@echo "Compose validation passed."
