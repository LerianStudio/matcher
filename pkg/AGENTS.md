# Package Directory (`pkg/`)

Most reusable packages have been migrated to `lib-uncommons` (`github.com/LerianStudio/lib-uncommons/uncommons/`).

## Migrated Packages (now in lib-uncommons)

| Former Location      | New Location                 | Import Path                                                            |
| -------------------- | ---------------------------- | ---------------------------------------------------------------------- |
| `pkg/assert`         | `uncommons/assert`             | `github.com/LerianStudio/lib-uncommons/uncommons/assert`             |
| `pkg/runtime`        | `uncommons/runtime`            | `github.com/LerianStudio/lib-uncommons/uncommons/runtime`            |
| `pkg/safe`           | `uncommons/safe`               | `github.com/LerianStudio/lib-uncommons/uncommons/safe`               |
| `pkg/jwt`            | `uncommons/jwt`                | `github.com/LerianStudio/lib-uncommons/uncommons/jwt`                |
| `pkg/backoff`        | `uncommons/backoff`            | `github.com/LerianStudio/lib-uncommons/uncommons/backoff`            |
| `pkg/cron`           | `uncommons/cron`               | `github.com/LerianStudio/lib-uncommons/uncommons/cron`               |
| `pkg/errgroup`       | `uncommons/errgroup`           | `github.com/LerianStudio/lib-uncommons/uncommons/errgroup`           |
| `pkg/logging`        | `uncommons/logging`            | `github.com/LerianStudio/lib-uncommons/uncommons/logging`            |
| `pkg/http`           | `uncommons/net/http`           | `github.com/LerianStudio/lib-uncommons/uncommons/net/http`           |
| `pkg/http/ratelimit` | `uncommons/net/http/ratelimit` | `github.com/LerianStudio/lib-uncommons/uncommons/net/http/ratelimit` |

## Remaining Packages

### StorageOpt (`pkg/storageopt`)

**Goal:** Functional options for S3-compatible object storage uploads.

- **Purpose:** Exists to break an import cycle between `ports` and `mocks` in Matcher.
- **Exports:** `UploadOption`, `UploadOptions`, `WithStorageClass`, `WithServerSideEncryption`.
- **Not migrated** because it solves a Matcher-specific import cycle.
