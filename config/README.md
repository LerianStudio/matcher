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

3. **Systemplane handles runtime changes.** After startup, configuration is managed through the systemplane API — no restart required for most settings:

```
GET  /v1/system/configs          — view current runtime config
PATCH /v1/system/configs         — change any runtime-managed key
GET  /v1/system/configs/schema   — see all keys, types, and mutability
GET  /v1/system/configs/history  — audit trail of changes
```

Only bootstrap-only keys (listed in `.config-map.example`) require a restart.
