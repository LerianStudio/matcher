# =============================================================================
# Stage 1: Build
# =============================================================================
FROM --platform=$BUILDPLATFORM golang:1.26.2-alpine AS builder

WORKDIR /matcher-app

COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go mod vendor

ARG TARGETARCH
ARG VERSION=dev
ARG BUILD_TIME

# Build the main application binary (vendor mode — no network needed)
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build \
    -mod=vendor \
    -tags netgo \
    -ldflags '-s -w -extldflags "-static"' \
    -o /app ./cmd/matcher/main.go

# Build a minimal health-probe binary for distroless healthchecks.
# Distroless images have no shell, curl, or wget — this tiny static binary
# performs an HTTP GET against /health and returns exit code 0 or 1.
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build \
    -mod=vendor \
    -tags netgo \
    -ldflags '-s -w -extldflags "-static"' \
    -o /health-probe ./cmd/health-probe/main.go

# =============================================================================
# Stage 2: Runtime
# =============================================================================
FROM gcr.io/distroless/static-debian12:nonroot

# Labels for container metadata
LABEL org.opencontainers.image.title="Matcher" \
    org.opencontainers.image.description="Lerian Studio Reconciliation Engine" \
    org.opencontainers.image.vendor="Lerian Studio" \
    org.opencontainers.image.source="https://github.com/LerianStudio/matcher" \
    org.opencontainers.image.version="${VERSION}" \
    org.opencontainers.image.created="${BUILD_TIME}"

COPY --from=builder /app /app
COPY --from=builder /health-probe /health-probe
# Copy migrations to both paths:
# - /migrations for the app's RunMigrationsWithLogger call
# - /components/matcher/migrations for lib-commons PostgresConnection
COPY --from=builder /matcher-app/migrations /migrations
COPY --from=builder /matcher-app/migrations /components/matcher/migrations

USER nonroot:nonroot

EXPOSE 4018

# Health probe defaults to http://localhost:4018/health.
# Override at runtime via HEALTH_PROBE_URL env var for non-default ports.
HEALTHCHECK --interval=30s --timeout=5s --retries=3 --start-period=10s \
    CMD ["/health-probe"]

ENTRYPOINT ["/app"]


