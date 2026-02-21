# Stage 1: Build#
ARG GO_VERSION=1.25
FROM --platform=$BUILDPLATFORM golang:${GO_VERSION} AS build

WORKDIR /src

# Build-time version info (passed from CI, falls back to source-derived values)
ARG VERSION
ARG COMMIT
ARG TARGETOS
ARG TARGETARCH

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Build static binary with version info for the target platform.
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build \
    -ldflags "-X main.Version=${VERSION:-$(cat VERSION 2>/dev/null || echo dev)} \
              -X main.Commit=${COMMIT:-none}" \
    -o /tidb-graphql ./cmd/server

# Stage 2: Runtime
FROM gcr.io/distroless/static:nonroot

COPY --from=build /tidb-graphql /tidb-graphql
COPY config/defaults.yaml /etc/tidb-graphql/tidb-graphql.yaml

EXPOSE 8080

ENTRYPOINT ["/tidb-graphql"]
