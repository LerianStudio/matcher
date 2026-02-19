# cmd

Application entry points for the Matcher service.

## Binaries

### matcher

The primary service binary. Initializes infrastructure, wires dependencies, and starts the HTTP server with the reconciliation engine.

```bash
go run ./cmd/matcher
# or
make dev    # with live reload
make build  # produces bin/matcher
```

### health-probe

A lightweight Kubernetes health probe binary for liveness and readiness checks. Used in container orchestration environments where `curl`/`wget` are unavailable.

```bash
go run ./cmd/health-probe
```
