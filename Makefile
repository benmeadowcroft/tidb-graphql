.PHONY: help build run clean test test-unit test-integration test-coverage test-race lint jwt-keys jwt-token jwks-server container-build

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
	@echo "  run                - Build and run the server"
	@echo "  deps               - Download and tidy dependencies"
	@echo "  fmt                - Format code"
	@echo "  lint               - Run linter"
	@echo "  jwt-keys           - Generate local JWT keypair in .auth/"
	@echo "  jwt-token          - Mint a JWT token from local keypair"
	@echo "  jwks-server        - Run a local JWKS server for dev auth testing"
	@echo "  container-build    - Build container image locally (podman/docker)"

# Load test environment variables from .env.test if it exists
-include .env.test

# Keep Go caches in-repo for sandboxed environments
export GOCACHE := $(CURDIR)/.cache/go-build
export GOMODCACHE := $(CURDIR)/.cache/go-mod

# Export variables loaded from .env.test to shell commands.
export

# Build the server binary
build:
	@echo "Building tidb-graphql..."
	@mkdir -p bin
	$(eval VERSION := $(shell cat VERSION 2>/dev/null || echo dev))
	$(eval COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo none))
	go build -ldflags "-X main.Version=$(VERSION) -X main.Commit=$(COMMIT)" -o bin/tidb-graphql ./cmd/server

# Run the server (requires database configuration)
run: build
	./bin/tidb-graphql

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
	@if [ -z "$$TIDB_CLOUD_HOST" ]; then \
		echo "Error: TiDB Cloud credentials not set."; \
		echo ""; \
		echo "To run integration tests, you need to set TiDB Cloud environment variables:"; \
		echo "  export TIDB_CLOUD_HOST=your-cluster.tidbcloud.com"; \
		echo "  export TIDB_CLOUD_USER=your.user"; \
		echo "  export TIDB_CLOUD_PASSWORD=your-password"; \
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

# Install dependencies
deps:
	go mod download
	go mod tidy

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

# Mint a JWT token for auth testing
jwt-token:
	go run ./scripts/jwt-mint

# Run a local JWKS server for dev auth testing
jwks-server:
	go run ./scripts/jwks-server

# Build container image locally (podman or docker)
container-build:
	podman build \
		-t tidb-graphql:local .

# Build and run in one command
dev: build run
