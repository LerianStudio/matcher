# Fetcher -> Matcher Integration

**Status:** Implementation complete (T-001..T-006 shipped 2026-04-16).
**Last updated:** 2026-04-17
**Owner:** Fred Amaral (fred@lerian.studio)

---

## This document has been superseded

The planning and design content that used to live here has moved to the pre-dev
artifacts. Go there for anything authoritative:

- **Product requirements:** [`docs/pre-dev/fetcher-bridge/prd.md`](docs/pre-dev/fetcher-bridge/prd.md)
- **Technical architecture:** [`docs/pre-dev/fetcher-bridge/trd.md`](docs/pre-dev/fetcher-bridge/trd.md)
- **Research basis:** [`docs/pre-dev/fetcher-bridge/research.md`](docs/pre-dev/fetcher-bridge/research.md)
- **UX criteria:** [`docs/pre-dev/fetcher-bridge/ux-criteria.md`](docs/pre-dev/fetcher-bridge/ux-criteria.md)
- **Task breakdown:** [`docs/pre-dev/fetcher-bridge/tasks.md`](docs/pre-dev/fetcher-bridge/tasks.md)
- **Delivery roadmap:** [`docs/pre-dev/fetcher-bridge/delivery-roadmap.md`](docs/pre-dev/fetcher-bridge/delivery-roadmap.md)
- **Gate 4 code review:** [`REVIEW.md`](REVIEW.md)

Locked design decisions (D2 retention, D4 key distribution, D9 custody format)
live in the session-local project memory file
`project_fetcher_bridge_decisions.md`. T-002 Gate 8 deferred findings — P1
through P6 — live in `project_fetcher_bridge_t002_preconditions.md`. T-001 Gate
8 preconditions applicable to T-003 live in
`project_fetcher_bridge_t003_preconditions.md`.

As of 2026-04-17, T-001 through T-006 have shipped on `develop` across 9
commits (`7a74051` through `a1fa898`). `ExtractionRequest.IngestionJobID` is
now populated at
[`internal/discovery/services/command/bridge_extraction_commands.go:308`](internal/discovery/services/command/bridge_extraction_commands.go)
— the "declared, never populated" gap that motivated this feature is closed.

---

## Warning: real-Fetcher integration is blocked on P6

> **Real-Fetcher integration is BLOCKED on P6 HMAC contract resolution.**
> **Do not set `FETCHER_ENABLED=true` in production until resolved.**

Per the 2026-04-17 code review, the HMAC contract divergence between Matcher
and Fetcher is **confirmed real and deferred** by explicit architect decision:

- Matcher signs the **ciphertext** (as implemented in T-002).
- Fetcher's published verification contract signs `<unix-timestamp>.<plaintext-body>`.
- If Matcher talks to a real Fetcher using Fetcher's real contract, every HMAC
  verification fails terminally and the bridge is dead on arrival.

This must be resolved cross-team before enabling real-Fetcher in any
production-adjacent environment. Tracking details and the three resolution
options (A/B/C) are in `project_fetcher_bridge_t002_preconditions.md` under
P6. The test suite today exercises the pipeline against Matcher-produced
artifacts, which is why the divergence has not blocked implementation.

---

*End of document.*
