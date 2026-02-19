# tools

Development tooling and custom linters for the Matcher project.

## Structure

| Path | Description |
|------|-------------|
| `tools.go` | Go tool dependencies (blank imports for `go mod tidy` retention) |
| [linters/](linters/) | Custom Go linters enforcing Matcher-specific architectural patterns |

## Custom Linters

See [linters/README.md](linters/README.md) for details on:
- **entityconstructor**: Enforces `New<Type>(ctx, ...) (*Type, error)` pattern
- **observability**: Requires `NewTrackingFromContext` and span creation in services
- **repositorytx**: Ensures write methods have `*WithTx` variants

```bash
make lint-custom         # Run custom linters (warning mode)
make lint-custom-strict  # Run custom linters (strict mode)
```
