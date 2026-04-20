# config

Configuration files for the Matcher service.

## Files

| File | Description |
|------|-------------|
| `.config-map.example` | Bootstrap-only settings that require a restart. Copy to a Kubernetes ConfigMap/Secret for production. Not needed for local development. |
| `seaweedfs-s3.json` | S3-compatible object storage configuration for SeaweedFS (used for export/archival features). |

## How Configuration Works

Matcher uses a **zero-config** approach:

1. **Defaults are in the binary.** All configuration has sensible defaults baked into `defaultConfig()`. Running `make up && make dev` works with no config files.

2. **Env vars override defaults at startup.** Any `env:` tagged field in the `Config` struct can be overridden via environment variables.

3. **Systemplane handles runtime changes.** After startup, configuration is managed through the canonical lib-commons v5 systemplane admin API (management-plane surface, intentionally excluded from the public OpenAPI spec) — no restart required for most settings:

```
GET  /system/matcher             — list all keys (inline schema metadata)
GET  /system/matcher/:key        — read a single key
PUT  /system/matcher/:key        — write a single key
```

See `github.com/LerianStudio/lib-commons/v5/commons/systemplane/admin` for the full HTTP surface. The previous v4 `/v1/system/configs[...]` paths and the `/schema`, `/history`, `/reload` sub-endpoints were removed in the v5 migration.

Only bootstrap-only keys (listed in `.config-map.example`) require a restart.
