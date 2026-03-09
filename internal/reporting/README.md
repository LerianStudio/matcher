# Reporting Context

The `internal/reporting` bounded context is responsible for read models, analytics, and data exports. It provides optimized queries for generating reconciliation reports, variance analysis, dashboard metrics, and async export jobs.

## Overview

While the `matching` context handles the write-side complexity of reconciliation, `reporting` focuses on:
1. **Dashboard Metrics**: Volume stats, match rate, SLA metrics, source breakdown, and cash impact summary.
2. **Read-Optimized Queries**: Fetching large datasets of matched/unmatched items efficiently.
3. **Variance Analysis**: Aggregating fee variances by source, currency, and fee type.
4. **Export Jobs**: Async export pipeline with CSV and PDF generation, S3 storage, and presigned download URLs.
5. **Streaming Exports**: Streamed report generation for large datasets.
6. **Caching**: Redis-backed dashboard aggregate caching.

## Architecture

### Hexagonal Layers

```
internal/reporting/
├── adapters/
│   ├── http/            # Dashboard, report export, and export job handlers
│   ├── postgres/        # Repositories
│   │   ├── dashboard/   # Dashboard aggregate queries
│   │   ├── export_job/  # Export job persistence
│   │   └── report/      # Report queries with streaming support
│   ├── redis/           # Dashboard cache service
│   └── storage/         # S3-compatible object storage client
├── domain/
│   ├── entities/        # Dashboard, DashboardMetrics, ExportJob, Report read models
│   └── repositories/    # Repository interfaces (dashboard, export_job, report)
├── ports/               # Cache and ObjectStorage interfaces
└── services/
    ├── command/         # Export job creation and management
    ├── query/           # Dashboard, report, and export job queries
    │   └── exports/     # CSV, PDF generation and streaming
    └── worker/          # Export worker (background) and cleanup worker
```

## Domain Model

### Read Models

- **Dashboard**: Aggregated reconciliation overview with volume, match rate, SLA, source breakdown, and cash impact.
- **DashboardMetrics**: Detailed matcher dashboard metrics for UI visualization.
- **ExportJob**: Tracks async export lifecycle (PENDING -> PROCESSING -> COMPLETED / FAILED / CANCELLED).
- **Report**: Base read model for matched, unmatched, summary, and variance report rows.

## Features

### Dashboard Metrics
Pre-aggregated dashboard views including volume statistics, match rate breakdowns, SLA compliance metrics, per-source breakdowns, and cash impact summaries.

### Export Jobs
Async export pipeline — create export jobs that run in the background, generating CSV or PDF files stored in S3-compatible object storage. Jobs can be listed, inspected, cancelled, and downloaded via presigned URLs.

### Streaming Exports
Synchronous streaming report generation for large datasets. The repository supports efficient cursor-based pagination and streaming to avoid loading entire result sets into memory.

### Redis Cache
Dashboard aggregate queries are cached in Redis to reduce database load for frequently accessed metrics.

### Cleanup Worker
Background worker that periodically removes old export files from object storage and their associated job records.

### Rate Limiting
Export endpoints are rate-limited to prevent abuse and protect infrastructure from expensive report generation requests.

### Cursor Pagination
Uses the standard `lib-commons` cursor pagination for efficient traversal of large result sets.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/reports/contexts/:contextId/dashboard` | Dashboard aggregates |
| GET | `/v1/reports/contexts/:contextId/dashboard/metrics` | Matcher dashboard metrics |
| GET | `/v1/reports/contexts/:contextId/dashboard/volume` | Volume statistics |
| GET | `/v1/reports/contexts/:contextId/dashboard/match-rate` | Match rate statistics |
| GET | `/v1/reports/contexts/:contextId/dashboard/sla` | SLA statistics |
| GET | `/v1/reports/contexts/:contextId/dashboard/source-breakdown` | Source breakdown |
| GET | `/v1/reports/contexts/:contextId/dashboard/cash-impact` | Cash impact summary |
| GET | `/v1/reports/contexts/:contextId/matches/count` | Count matched transactions |
| GET | `/v1/reports/contexts/:contextId/transactions/count` | Count transactions |
| GET | `/v1/reports/contexts/:contextId/exceptions/count` | Count exceptions |
| GET | `/v1/reports/contexts/:contextId/matched/export` | Export matched report (rate-limited) |
| GET | `/v1/reports/contexts/:contextId/unmatched/export` | Export unmatched report (rate-limited) |
| GET | `/v1/reports/contexts/:contextId/summary/export` | Export summary report (rate-limited) |
| GET | `/v1/reports/contexts/:contextId/variance/export` | Export variance report (rate-limited) |
| POST | `/v1/contexts/:contextId/export-jobs` | Create async export job (rate-limited) |
| GET | `/v1/export-jobs` | List export jobs |
| GET | `/v1/export-jobs/:jobId` | Get export job details |
| POST | `/v1/export-jobs/:jobId/cancel` | Cancel export job |
| GET | `/v1/export-jobs/:jobId/download` | Download export file |
