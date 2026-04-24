# Observability

## Metrics: OTLP Push, not Prometheus Scrape

Matcher ships metrics via **OpenTelemetry Protocol (OTLP) push** to a configured collector. Matcher does **not** expose a Prometheus `/metrics` scrape endpoint.

### Why push, not scrape

1. **Single observability pipeline.** Traces, logs, and metrics all ship through the same OTLP endpoint — configurable via `OTEL_EXPORTER_OTLP_ENDPOINT`. Operators manage one integration point, not two.
2. **Tenant-aware metric enrichment.** The OTLP exporter auto-adds tenant and trace-correlation attributes that a scrape-based setup would have to replicate out-of-band.
3. **No leaked metrics endpoint.** Matcher's HTTP surface stays API-only. There is no `/metrics` route to secure against scrapers that would otherwise need careful tenant-context handling.
4. **No per-pod scrape config.** Collectors pull from one OTLP endpoint regardless of pod count; Prometheus scrape configs would need to keep up with pod lifecycle.

### If a reviewer asks "where's /metrics"

Point them here. A Prometheus-compatible scrape surface CAN be added later — via `otel-collector`'s Prometheus receiver on the push side of the pipeline — but that is a **deployment decision**, not an application-code decision. Matcher itself stays push-only.

### Related configuration keys

| Key | Default | Purpose |
|-----|---------|---------|
| `ENABLE_TELEMETRY` | `false` | Master switch for OTLP exporters |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | OTLP gRPC collector target |
| `OTEL_LIBRARY_NAME` | `github.com/LerianStudio/matcher` | Instrumentation library identifier |
| `OTEL_RESOURCE_SERVICE_NAME` | `matcher` | Service name resource attribute |
| `OTEL_RESOURCE_SERVICE_VERSION` | (git-sha) | Service version resource attribute |
| `OTEL_RESOURCE_DEPLOYMENT_ENVIRONMENT` | (from `ENV_NAME`) | Deployment environment attribute |

See `config/.config-map.example` for the full list of bootstrap-only keys.
