# Package Directory (`pkg/`)

Most reusable packages have been migrated to `lib-commons` (`github.com/LerianStudio/lib-commons/commons/`).

## Migrated Packages (now in lib-commons)

| Former Location      | New Location                 | Import Path                                                            |
| -------------------- | ---------------------------- | ---------------------------------------------------------------------- |
| `pkg/assert`         | `commons/assert`             | `github.com/LerianStudio/lib-commons/commons/assert`             |
| `pkg/runtime`        | `commons/runtime`            | `github.com/LerianStudio/lib-commons/commons/runtime`            |
| `pkg/safe`           | `commons/safe`               | `github.com/LerianStudio/lib-commons/commons/safe`               |
| `pkg/jwt`            | `commons/jwt`                | `github.com/LerianStudio/lib-commons/commons/jwt`                |
| `pkg/backoff`        | `commons/backoff`            | `github.com/LerianStudio/lib-commons/commons/backoff`            |
| `pkg/cron`           | `commons/cron`               | `github.com/LerianStudio/lib-commons/commons/cron`               |
| `pkg/errgroup`       | `commons/errgroup`           | `github.com/LerianStudio/lib-commons/commons/errgroup`           |
| `pkg/logging`        | `commons/logging`            | `github.com/LerianStudio/lib-commons/commons/logging`            |
| `pkg/http`           | `commons/net/http`           | `github.com/LerianStudio/lib-commons/commons/net/http`           |
| `pkg/http/ratelimit` | `commons/net/http/ratelimit` | `github.com/LerianStudio/lib-commons/commons/net/http/ratelimit` |

## Remaining Packages

### StorageOpt (`pkg/storageopt`)

**Goal:** Functional options for S3-compatible object storage uploads.

- **Purpose:** Exists to break an import cycle between `ports` and `mocks` in Matcher.
- **Exports:** `UploadOption`, `UploadOptions`, `WithStorageClass`, `WithServerSideEncryption`.
- **Not migrated** because it solves a Matcher-specific import cycle.
