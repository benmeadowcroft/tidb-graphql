# Stage 1: Build
FROM golang:1.25 AS build

WORKDIR /src

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Build args for version injection
ARG VERSION=dev
ARG COMMIT=none

RUN CGO_ENABLED=0 go build \
    -ldflags "-X main.Version=${VERSION} -X main.Commit=${COMMIT}" \
    -o /tidb-graphql ./cmd/server

# Stage 2: Runtime
FROM gcr.io/distroless/static:nonroot

COPY --from=build /tidb-graphql /tidb-graphql
COPY config/defaults.yaml /etc/tidb-graphql/tidb-graphql.yaml

EXPOSE 8080

ENTRYPOINT ["/tidb-graphql"]
