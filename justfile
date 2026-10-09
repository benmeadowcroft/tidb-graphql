# tidb-graphql tasks. Run `just` (no args) to list recipes.
# Install just: https://just.systems (e.g. `brew install just`).

set shell := ["bash", "-euo", "pipefail", "-c"]

# Load test environment variables (e.g. TIDB_HOST) from .env.test if it exists.
set dotenv-load
set dotenv-filename := ".env.test"

# Export variables below (and those from .env.test) to recipe shells.
set export

# Only evaluate variables (e.g. container engine probing) when a recipe uses them.
set lazy

# Keep Go caches in-repo for sandboxed environments.
GOCACHE := justfile_directory() / ".cache/go-build"
GOMODCACHE := justfile_directory() / ".cache/go-mod"

# Build metadata (can be overridden by env).
VERSION := env("VERSION", `cat VERSION 2>/dev/null || echo dev`)
COMMIT := env("COMMIT", `git rev-parse --short HEAD 2>/dev/null || echo none`)

# Container engine: override with CONTAINER_TOOL=podman|docker|nerdctl. Otherwise
# pick the first of podman, docker, nerdctl whose engine is reachable (Rancher
# Desktop's containerd mode ships a docker CLI with no daemon behind it), falling
# back to the first one installed so its own error message is shown.
CONTAINER_TOOL := env("CONTAINER_TOOL", ```
    for t in podman docker nerdctl; do
        if command -v "$t" >/dev/null 2>&1 && "$t" info >/dev/null 2>&1; then echo "$t"; exit 0; fi
    done
    for t in podman docker nerdctl; do
        if command -v "$t" >/dev/null 2>&1; then echo "$t"; exit 0; fi
    done
```)
compose_dir := "examples/compose"

# Scenario-backed JWT minting defaults.
SCENARIO := env("SCENARIO", "oidc-roles")
TOKEN_ENDPOINT := env("TOKEN_ENDPOINT", "https://localhost:9000/dev/token")
TOKEN_CURL_TLS_FLAGS := env("TOKEN_CURL_TLS_FLAGS", "-k")
DEFAULT_DEV_ADMIN_TOKEN := env("DEFAULT_DEV_ADMIN_TOKEN", "dev-admin-token")

# List available recipes
default:
    @{{ just_executable() }} --list

# --- Go project ---

# Build the tidb-graphql binary into bin/
build:
    @echo "Building tidb-graphql..."
    @mkdir -p bin
    go build -ldflags "-X main.Version={{ VERSION }} -X main.Commit={{ COMMIT }}" -o bin/tidb-graphql ./cmd/server

# Run the server from source; extra args are passed through:  just run --config tidb-graphql.yaml
run *args:
    go run ./cmd/server {{ args }}

# Remove build artifacts
clean:
    @echo "Cleaning build artifacts..."
    rm -rf bin/
    rm -f coverage.out coverage.html

# Format code
fmt:
    go fmt ./...

# Run go vet
vet:
    go vet ./...

# Tidy go.mod / go.sum
tidy:
    go mod tidy

# Run golangci-lint
lint:
    @command -v golangci-lint >/dev/null 2>&1 || { echo "golangci-lint is not installed. Install from https://golangci-lint.run/usage/install/"; exit 1; }
    golangci-lint run

# Preflight: report which development tools are available
check:
    #!/usr/bin/env bash
    set -uo pipefail
    status=0
    if command -v go >/dev/null 2>&1; then echo "ok    $(go version)"; else echo "MISSING go (required): https://go.dev/dl/"; status=1; fi
    if command -v golangci-lint >/dev/null 2>&1; then echo "ok    golangci-lint $(golangci-lint version --short 2>/dev/null || true)"; else echo "warn  golangci-lint not found (needed for 'just lint')"; fi
    if [ -z "{{ CONTAINER_TOOL }}" ]; then
        echo "warn  no podman/docker/nerdctl found (needed for container/compose recipes)"
    elif {{ CONTAINER_TOOL }} info >/dev/null 2>&1; then
        echo "ok    container engine: {{ CONTAINER_TOOL }} ($({{ CONTAINER_TOOL }} compose version 2>/dev/null | head -n1 || echo 'compose unavailable'))"
    else
        echo "warn  container engine {{ CONTAINER_TOOL }} found but not reachable (is the daemon/VM running?)"
    fi
    if command -v curl >/dev/null 2>&1; then echo "ok    curl"; else echo "warn  curl not found (needed for token recipes)"; fi
    exit $status

# --- Tests ---

# Run all tests (unit + integration)
test: test-unit test-integration

# Run unit tests only (fast, no external dependencies)
test-unit:
    @echo "Running unit tests..."
    go test -short -v ./internal/...

# Run integration tests (requires TiDB credentials via env or .env.test)
test-integration:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "Checking TiDB Cloud credentials..."
    if [ -z "${TIDB_HOST:-}" ]; then
        echo "Error: TiDB credentials not set."
        echo ""
        echo "To run integration tests, you need to set TiDB environment variables:"
        echo "  export TIDB_HOST=your-cluster.tidbcloud.com"
        echo "  export TIDB_USER=your.user"
        echo "  export TIDB_PASSWORD=your-password"
        echo ""
        echo "Or create a .env.test file (see .env.test.example)"
        exit 1
    fi
    echo "Running integration tests..."
    go test -v -tags=integration ./tests/integration/...

# Run one package's tests verbosely:  just test-pkg internal/planner
test-pkg pkg:
    go test ./{{ pkg }}/ -v

# Run a single test:  just test-run internal/cursor TestEncode
test-run pkg run:
    go test ./{{ pkg }}/ -run '{{ run }}' -v -count=1

# Run tests with coverage report (coverage.html)
test-coverage:
    @echo "Running tests with coverage..."
    go test -coverprofile=coverage.out ./...
    go tool cover -html=coverage.out -o coverage.html
    @echo "Coverage report generated: coverage.html"

# Run tests with the race detector
test-race:
    @echo "Running tests with race detector..."
    go test -race ./...

# --- Auth helpers ---

# Generate local JWT keypair in .auth/
jwt-keys:
    go run ./scripts/jwt-generate-keys

# Mint a token for a DB role from a scenario's JWKS /dev/token:  just token app_admin otel
token role="app_viewer" scenario=SCENARIO:
    #!/usr/bin/env bash
    set -euo pipefail
    env_file="{{ compose_dir }}/{{ scenario }}/.env"
    admin_token="${DEV_ADMIN_TOKEN:-$(grep -E '^DEV_ADMIN_TOKEN=' "$env_file" 2>/dev/null | tail -n1 | cut -d= -f2- || true)}"
    admin_token="${admin_token:-{{ DEFAULT_DEV_ADMIN_TOKEN }}}"
    admin_token="$(printf '%s' "$admin_token" | sed -e 's/^"//' -e 's/"$//' -e "s/^'//" -e "s/'$//")"
    curl {{ TOKEN_CURL_TLS_FLAGS }} -fsS -X POST "{{ TOKEN_ENDPOINT }}" \
        -H "X-Admin-Token: $admin_token" \
        -H "Content-Type: application/json" \
        -H "Accept: text/plain" \
        -d '{"db_role":"{{ role }}"}'

# Mint an app_viewer token:  just token-viewer [scenario]
token-viewer scenario=SCENARIO:
    @{{ just_executable() }} token app_viewer {{ scenario }}

# Mint an app_admin token:  just token-admin [scenario]
token-admin scenario=SCENARIO:
    @{{ just_executable() }} token app_admin {{ scenario }}

# --- Containers ---

[private]
require-container:
    @[ -n "{{ CONTAINER_TOOL }}" ] || { echo "Error: No container engine found. Install podman, docker, or nerdctl (e.g. Rancher Desktop)."; exit 1; }

# Build container image tidb-graphql:local (podman/docker/nerdctl)
container-build: require-container
    {{ CONTAINER_TOOL }} build \
        --build-arg VERSION={{ VERSION }} \
        --build-arg COMMIT={{ COMMIT }} \
        -t tidb-graphql:local .

# Start a compose scenario (default: quickstart):  just compose-up oidc-roles
compose-up scenario="quickstart": require-container
    {{ CONTAINER_TOOL }} compose -f {{ compose_dir }}/{{ scenario }}/docker-compose.yml up --build

# Stop a compose scenario
compose-down scenario="quickstart": require-container
    {{ CONTAINER_TOOL }} compose -f {{ compose_dir }}/{{ scenario }}/docker-compose.yml down

# Stop a compose scenario and remove its volumes (fresh start)
compose-reset scenario="quickstart": require-container
    {{ CONTAINER_TOOL }} compose -f {{ compose_dir }}/{{ scenario }}/docker-compose.yml down -v

# Validate all compose files with the detected container engine
compose-validate: require-container
    #!/usr/bin/env bash
    set -euo pipefail
    echo "Validating compose files with {{ CONTAINER_TOOL }}..."
    for f in docker-compose.yml {{ compose_dir }}/*/docker-compose.yml; do
        echo "  $f"
        {{ CONTAINER_TOOL }} compose -f "$f" config >/dev/null
    done
    echo "Compose validation passed."
