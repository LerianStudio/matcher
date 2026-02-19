# config

Configuration files and environment variable templates for the Matcher service.

## Files

| File | Description |
|------|-------------|
| `.env.example` | Template with all environment variables and default values. Copy to `.env` for local development. |
| `seaweedfs-s3.json` | S3-compatible object storage configuration for SeaweedFS (used for export/archival features). |

## Setup

```bash
make set-env    # Copies .env.example to config/.env
make clear-envs # Removes config/.env
```

See the root [README.md](../README.md) for the full list of environment variable categories.
