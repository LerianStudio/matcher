# Matcher Custom Linters

This directory contains custom Go linters specific to Matcher's architectural patterns.

## Available Linters

### 1. Entity Constructor Linter (`entityconstructor`)

Enforces domain entity constructor patterns:

- Constructor functions must be named `New<EntityName>`
- First parameter must be `context.Context`
- Return type must be `(*EntityName, error)`

**Why?** DDD best practice - entities maintain invariants through validated constructors.

### 2. Observability Linter (`observability`)

Enforces tracing patterns in service methods:

- Service `Execute`/`Run`/`Handle` methods must call `NewTrackingFromContext(ctx)`
- Must create a span with `tracer.Start(ctx, "operation.name")`
- Must defer `span.End()` for proper cleanup

**Why?** Production debugging requires comprehensive tracing of all service operations.

### 3. Repository Transaction Linter (`repositorytx`)

Enforces transaction safety patterns:

- Write methods (`Create`, `Update`, `Delete`, etc.) must have `*WithTx` variants
- Non-WithTx methods should use `common.WithTenantTx` wrapper internally

**Why?** Financial data requires strict transaction safety with tenant isolation.

## Usage

### Run All Custom Linters (Warning Mode)

```bash
make lint-custom
```

This runs all linters but treats violations as warnings. Use during development.

### Run All Custom Linters (Strict Mode)

```bash
make lint-custom-strict
```

This fails on any violation. Use in CI after cleanup is complete.

### Run Standalone

```bash
# Run all analyzers on specific packages
go run ./tools/linters/matcherlint/... ./internal/.../domain/entities/...

# Run on services
go run ./tools/linters/matcherlint/... ./internal/.../services/...

# Run on postgres adapters
go run ./tools/linters/matcherlint/... ./internal/.../adapters/postgres/...
```

## Adding New Linters

1. Create a new package under `tools/linters/`
2. Implement the `*analysis.Analyzer` interface
3. Add test data in `testdata/src/`
4. Register in `matcherlint/main.go`
5. Update this README

## Integration with golangci-lint

These linters can be integrated with golangci-lint using custom plugins:

```bash
# Build plugin
go build -buildmode=plugin -o matcherlint.so ./tools/linters/matcherlint

# Use with golangci-lint (requires golangci-lint with plugin support)
golangci-lint run --custom-linters=matcherlint.so
```

Note: Plugin support requires building golangci-lint from source with CGO enabled.

## Pattern Reference

See [LINT_ENHANCEMENTS.md](../../LINT_ENHANCEMENTS.md) for detailed pattern documentation.
