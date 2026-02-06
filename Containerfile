# Stage 1: Build
FROM golang:1.25 AS build

WORKDIR /src

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Extract version info from source
RUN CGO_ENABLED=0 go build \
    -ldflags "-X main.Version=$(cat VERSION 2>/dev/null || echo dev) \
              -X main.Commit=$(git rev-parse --short HEAD 2>/dev/null || echo none)" \
    -o /tidb-graphql ./cmd/server

# Stage 2: Runtime
FROM gcr.io/distroless/static:nonroot

COPY --from=build /tidb-graphql /tidb-graphql
COPY config/defaults.yaml /etc/tidb-graphql/tidb-graphql.yaml

EXPOSE 8080

ENTRYPOINT ["/tidb-graphql"]
