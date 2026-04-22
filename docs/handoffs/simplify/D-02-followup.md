# D-02 follow-up: DTO snake_case → camelCase rename (asymmetric remainder)

**Date:** 2026-04-22
**Parent:** [D-02.md](./D-02.md) (commit `a3f84ed6`)
**Scope:** 3 HTTP DTOs that were deliberately deferred in D-02 plus the error-text drift their new tags trigger. Atomic with the E2E client mirror and Swagger regeneration.
**Cross-repo coordination:** [lerianstudio/product-console#317](https://github.com/lerianstudio/product-console/issues/317) — **scope extended**, frontend must add these 3 DTOs to the TypeScript mirror rewrite.

## Why this is a follow-up, not a new task

D-02 landed 4 DTOs. Three more existed in the same files but were excluded to keep the blast radius auditable. The most visible asymmetry: `UpsertActorMappingRequest` serialized as `display_name` while its response (`ActorMappingResponse`) returned `displayName` — frontend had to serialize snake_case and deserialize camelCase for the same field. Correcting that symmetry is the trigger; the other two (`BulkAssignRequest`, `BulkDispatchRequest`) follow the same DTO convention and ride the same commit.

## DTOs renamed (3)

### 1. `UpsertActorMappingRequest` — `internal/governance/adapters/http/dto/actor_mapping.go`

| Field | Old tag | New tag |
|---|---|---|
| `DisplayName` | `display_name` | `displayName` |

`Email` unchanged (single word).

Doc comment on the struct updated: `At least one of display_name or email must be provided` → `At least one of displayName or email must be provided`.

### 2. `BulkAssignRequest` — `internal/exception/adapters/http/dto/bulk_requests.go`

| Field | Old tag | New tag |
|---|---|---|
| `ExceptionIDs` | `exception_ids` | `exceptionIds` |

`Assignee` unchanged (single word).

### 3. `BulkDispatchRequest` — `internal/exception/adapters/http/dto/bulk_requests.go`

| Field | Old tag | New tag |
|---|---|---|
| `ExceptionIDs` | `exception_ids` | `exceptionIds` |
| `TargetSystem` | `target_system` | `targetSystem` |

`Queue` unchanged (single word). **No `reason_code` / `ReasonCode` field exists on this DTO** — verified by reading the full struct. (A `reason_code` key does exist in `exception/adapters/audit/outbox_publisher.go` as an audit-event payload map key; that is not a DTO JSON tag and was intentionally left alone.)

## Error-text drift — `internal/governance/adapters/http/handlers_actor_mapping.go`

The sentinel `ErrAtLeastOneFieldRequired` previously read:

```
"at least one of display_name or email must be provided"
```

This is a user-facing 400 response body. After flipping the JSON tag to `displayName`, the snake_case in the error text misaligns with the actual wire key. Updated to:

```
"at least one of displayName or email must be provided"
```

The corresponding assertion in `handlers_actor_mapping_test.go:438` was updated to match.

## Test-fixture updates

| File | Change |
|---|---|
| `internal/exception/adapters/http/dto/bulk_requests_test.go` | `TestBulkAssignRequest_JSONTags` and `TestBulkDispatchRequest_JSONTags` now assert `"exceptionIds"` / `"targetSystem"` on the marshaled output (previously asserted snake_case). |
| `internal/exception/adapters/http/handlers_coverage_test.go` | 4 raw JSON request bodies flipped: lines 955, 981, 1081, 1107 — all covering `BulkAssign` / `BulkDispatch` error paths. |
| `internal/governance/adapters/http/handlers_actor_mapping_test.go` | Line 438 — updated the sentinel-text assertion. No raw JSON bodies: the helper uses `json.Marshal(body)`, so it tracks the struct tag automatically. |

## E2E client mirror — `tests/e2e/client/types.go`

| Type | Fields flipped |
|---|---|
| `UpsertActorMappingRequest` | `DisplayName` → `displayName` |
| `BulkAssignRequest` | `ExceptionIDs` → `exceptionIds` |
| `BulkDispatchRequest` | `ExceptionIDs` → `exceptionIds`, `TargetSystem` → `targetSystem` |

## Swagger regeneration

`make generate-docs` ran clean. Verification:

```
$ grep 'exception_ids\|display_name\|target_system' docs/swagger/swagger.json
(no matches)
```

`docs/swagger/docs.go`, `docs/swagger/swagger.json`, and `docs/swagger/swagger.yaml` all updated (14 insertions, 14 deletions each — the three DTOs' wire contracts).

## Verification

1. `go build ./...` — clean.
2. `go vet -tags unit ./internal/governance/... ./internal/exception/...` — clean.
3. `go test -tags=unit -run 'TestBulk|TestActorMapping|TestUpsertActorMapping' ./internal/exception/adapters/http/... ./internal/governance/adapters/http/...`:

```
ok  	github.com/LerianStudio/matcher/internal/exception/adapters/http	0.874s
ok  	github.com/LerianStudio/matcher/internal/exception/adapters/http/connectors	1.532s [no tests to run]
ok  	github.com/LerianStudio/matcher/internal/exception/adapters/http/dto	0.469s
ok  	github.com/LerianStudio/matcher/internal/governance/adapters/http	1.871s
ok  	github.com/LerianStudio/matcher/internal/governance/adapters/http/dto	1.206s
```

4. JSON-tag grep across `internal/` and `tests/`:

```
$ grep -rn '"exception_ids"\|"display_name"\|"target_system"\|"reason_code"' internal/ tests/
internal/exception/adapters/audit/outbox_publisher.go:226:    changes["reason_code"] = *event.ReasonCode
internal/exception/adapters/audit/outbox_publisher_test.go:310:    assert.Equal(t, reasonCode, changes["reason_code"])
internal/exception/adapters/audit/outbox_publisher_test.go:333:    assert.Nil(t, changes["reason_code"])
internal/governance/adapters/postgres/actor_mapping/actor_mapping.postgresql.go:70:    Columns("actor_id", "display_name", "email", "created_at", "updated_at").
internal/governance/adapters/postgres/actor_mapping/actor_mapping.postgresql.go:118:   Select("actor_id", "display_name", "email", "created_at", "updated_at").
internal/governance/adapters/postgres/actor_mapping/actor_mapping.postgresql.go:170:   Set("display_name", "[REDACTED]").
internal/governance/adapters/postgres/actor_mapping/actor_mapping.postgresql_test.go:27: "actor_id", "display_name", "email", "created_at", "updated_at",
```

All remaining hits are **out of scope** and correctly untouched:
- `outbox_publisher*` — `reason_code` is an audit-event payload map key (wire format for audit messages, not an HTTP DTO).
- `actor_mapping.postgresql*` — `display_name` is a Postgres column name (DB schema, not JSON).

Zero hits inside target DTO JSON tags. Zero hits in Swagger.

## LOC delta

```
docs/swagger/docs.go                                       | 14 +++++++-------
docs/swagger/swagger.json                                  | 14 +++++++-------
docs/swagger/swagger.yaml                                  | 14 +++++++-------
internal/exception/adapters/http/dto/bulk_requests.go      |  6 +++---
internal/exception/adapters/http/dto/bulk_requests_test.go |  6 +++---
internal/exception/adapters/http/handlers_coverage_test.go |  8 ++++----
internal/governance/adapters/http/dto/actor_mapping.go     |  4 ++--
internal/governance/adapters/http/handlers_actor_mapping.go |  2 +-
internal/governance/adapters/http/handlers_actor_mapping_test.go |  2 +-
tests/e2e/client/types.go                                  |  8 ++++----
10 files changed, 39 insertions(+), 39 deletions(-)
```

Pure rename: +39 / −39 / net 0 LOC.

## Cross-repo note

[lerianstudio/product-console#317](https://github.com/lerianstudio/product-console/issues/317) — the TypeScript mirror rewrite tracked in that issue must now cover these three additional DTOs:

- `UpsertActorMappingRequest`: `display_name` → `displayName`
- `BulkAssignRequest`: `exception_ids` → `exceptionIds`
- `BulkDispatchRequest`: `exception_ids` → `exceptionIds`, `target_system` → `targetSystem`

Plus one user-facing error-message string change (the 400 body for `PUT /v1/governance/actor-mappings/:actorId` when both fields are omitted): `display_name or email` → `displayName or email`. If the console asserts on that literal in any toast/error mapper, it needs to flip too.

With this follow-up, all four `a3f84ed6` DTOs plus the three deferred ones bring the Matcher governance + exception surface fully onto camelCase. No further asymmetry remains in these contexts.
