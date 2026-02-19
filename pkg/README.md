# pkg

Reusable, domain-agnostic library packages. These packages have no dependency on internal bounded contexts and can be imported by any layer.

## Packages

| Package | Description |
|---------|-------------|
| [assert](assert/) | Always-on runtime assertions with observability (logs, metrics, traces) |
| [backoff](backoff/) | Exponential backoff with jitter for retry mechanisms |
| [cron](cron/) | Minimal cron expression parser and scheduler |
| [errgroup](errgroup/) | Goroutine group with coordinated cancellation and panic recovery |
| [http](http/) | Standard HTTP response helpers, error mapping, and pagination |
| [jwt](jwt/) | Minimal HMAC-based JWT signing and verification (HS256/384/512) |
| [logging](logging/) | Production-safe logging utilities with PII sanitization |
| [runtime](runtime/) | Panic recovery with policy-based behavior and observability |
| [safe](safe/) | Safe wrappers for panic-prone operations (division, slice access, regex) |
| [storageopt](storageopt/) | Functional options for object storage operations |
