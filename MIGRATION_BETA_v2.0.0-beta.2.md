# Migration Prompt — Matcher `v2.0.0-beta.1` → `v2.0.0-beta.2`

> **How to use this file:** Open the `product-console` repo, paste everything below (from `## Prompt` to end-of-file) into your AI agent's context window, and let it execute. The prompt is self-contained — no other matcher knowledge required.
>
> **Source of truth:** Generated from matcher branch `feat/streaming-and-others` (HEAD `1f3d667a`) against `develop` at tag `v2.0.0-beta.1`. The next matcher beta cut from develop after this branch lands will be **`v2.0.0-beta.2`** (counter increments — the underlying `2.0.0` major bump from `1.4.0` is still in effect, and the new commits since `v2.0.0-beta.1` are at most minor-level under conventional-commits parsing).
>
> **Audience:** Whoever owns the matcher API integration in `product-console`.
>
> **Severity:** 🟡 Low — five URL paths require updates, one response body grows additively. No DTO shape changes, no auth changes, no status-code changes.

---

## Prompt

You are upgrading the `product-console` codebase from Matcher **`v2.0.0-beta.1`** to Matcher **`v2.0.0-beta.2`**. The new beta contains five required URL updates plus a couple of additive observability changes.

Your job: find every reference to the old paths in this repo, update them, regenerate the typed SDK if one exists, and prove correctness with tests.

### Context

Matcher is Lerian's reconciliation engine. `product-console` integrates with it over HTTP. The required URL updates in this beta come from one commit: `fix(api): remove /config prefix from configuration endpoint routes` (matcher commit `ea16bfdc`). The Matcher team dropped a redundant `/config` URL segment from the fee-rule endpoints to align them with the rest of the configuration surface (contexts, sources, rules, field-maps were already on `/v1/...` — fee-rules were the inconsistent ones).

Two additional, **fully backward-compatible** changes accompany the beta and you should still know about them:

1. `/readyz` now reports up to two extra dependency keys (`fetcher`, `streaming`) inside the `checks` map.
2. `/readyz` and `/health` responses are now cached for 250 ms server-side to dampen Kubernetes probe amplification.

Neither of those should affect a sane consumer. Document them anyway so anyone polling those endpoints isn't surprised.

### Required URL updates (5 endpoints)

Every one of these must be updated. Calling the old paths will return **404** on the new beta.

| Method   | Old (current) path                                    | New path                                       |
| -------- | ----------------------------------------------------- | ---------------------------------------------- |
| `POST`   | `/v1/config/contexts/{contextId}/fee-rules`           | `/v1/contexts/{contextId}/fee-rules`           |
| `GET`    | `/v1/config/contexts/{contextId}/fee-rules`           | `/v1/contexts/{contextId}/fee-rules`           |
| `GET`    | `/v1/config/fee-rules/{feeRuleId}`                    | `/v1/fee-rules/{feeRuleId}`                    |
| `PATCH`  | `/v1/config/fee-rules/{feeRuleId}`                    | `/v1/fee-rules/{feeRuleId}`                    |
| `DELETE` | `/v1/config/fee-rules/{feeRuleId}`                    | `/v1/fee-rules/{feeRuleId}`                    |

Request bodies, response bodies, status codes, headers, and auth scopes are **unchanged**. Only the URL changes.

### Documentation-only corrections — stale swagger paths (7 endpoints)

If `product-console` uses an OpenAPI-generated TypeScript SDK built from Matcher's `develop`-channel `swagger.json`, your SDK was already broken for these endpoints on `v2.0.0-beta.1`. Matcher's swagger documented them under `/v1/config/...` but the actual Fiber routes had already moved to `/v1/...`. Regenerating the SDK against the new beta will silently start working.

| Path family                                                                  | Methods affected           |
| ---------------------------------------------------------------------------- | -------------------------- |
| `/v1/config/contexts`                                                        | `POST`, `GET`              |
| `/v1/config/contexts/{contextId}`                                            | `GET`, `PATCH`, `DELETE`   |
| `/v1/config/contexts/{contextId}/clone`                                      | `POST`                     |
| `/v1/config/contexts/{contextId}/rules` (and `/{ruleId}`, `/reorder`)        | `POST`, `GET`, `PATCH`, `DELETE` |
| `/v1/config/contexts/{contextId}/sources` (and `/{sourceId}`)                | `POST`, `GET`, `PATCH`, `DELETE` |
| `/v1/config/contexts/{contextId}/sources/{sourceId}/field-maps`              | `POST`, `GET`              |
| `/v1/config/field-maps/{fieldMapId}`                                         | `PATCH`, `DELETE`          |

All of these now correctly resolve to `/v1/...` (no `/config/`). If you were calling them via direct `fetch`/`axios` strings, you were probably already on the right URL and there's nothing to do. If you were using a generated SDK, regenerate it.

### Additive — `/readyz` body extension

If `product-console` parses `/readyz` (e.g. for an admin status indicator), be aware that the response `checks` map can now contain two new keys conditional on Matcher's deployment configuration:

```jsonc
{
  "status": "up",
  "checks": {
    "postgres":          { "status": "up" },
    "postgres_replica":  { "status": "up" },
    "redis":             { "status": "up" },
    "rabbitmq":          { "status": "up" },
    "object_storage":    { "status": "up" },
    "fetcher":           { "status": "up" },     // NEW — present when fetcher is enabled
    "streaming":         { "status": "up" }      // NEW — present when streaming is enabled
  }
}
```

If you do strict shape matching (e.g., a snapshot test or a `Record<KnownKey, ...>` type), widen it. Lenient parsing (object spread, key iteration) needs nothing.

Also: server-side response is cached for **250 ms**. Do not poll `/readyz` faster than 4 Hz expecting fresh data — you'll get the cached envelope until TTL expires. Drain (SIGTERM) bypasses the cache, so shutdown signals propagate immediately.

### Execution plan

1. **Inventory the impact.** Search the entire `product-console` repo for any of these literal substrings — they are the affected patterns:

   ```text
   /v1/config/contexts
   /v1/config/fee-rules
   /v1/config/field-maps
   ```

   Use `rg` (ripgrep) — do not rely on `grep` alone. Cover `.ts`, `.tsx`, `.js`, `.jsx`, `.mjs`, `.json`, `.yaml`, `.yml`, `.env*`, `.md`, and any `*.openapi.*` / `*.swagger.*` files. Also scan tests, mocks, fixtures, MSW handlers, Storybook stories, Cypress/Playwright specs, and CI configs.

2. **Classify each hit.**
   - URL constants / route maps → rename in place.
   - SDK source files (auto-generated from OpenAPI) → do not hand-edit; regenerate (step 3).
   - Test fixtures / recorded HTTP responses → update to the new URLs.
   - Documentation / README / ADRs → update the prose.
   - Comments referencing old URLs → update for accuracy.

3. **Regenerate the OpenAPI SDK** (if `product-console` consumes Matcher via a generated client). Pull `docs/swagger/swagger.json` from Matcher tag **`v2.0.0-beta.2`** (or whatever later beta you are integrating with) and regenerate. Common toolchains:
   - `openapi-typescript` → `npx openapi-typescript <swagger-url> -o src/lib/matcher-types.ts`
   - `orval` / `kubb` → run their codegen command
   - `openapi-generator-cli` → `npx @openapitools/openapi-generator-cli generate -i <swagger-url> -g typescript-fetch -o src/lib/matcher-sdk`
   - Hand-rolled types → diff schema definitions against the new spec, but expect zero shape changes (Matcher's swagger has 136 ↔ 136 definitions on both sides).

4. **Update tests.**
   - Find any `expect(...).toHaveBeenCalledWith` / `nock(...)` / `msw http.*(...)` referencing the old paths and migrate.
   - Run the full suite. If a snapshot test on `/readyz` fails because of the two new keys, widen the expectation rather than freezing the old shape.

5. **Smoke test against `v2.0.0-beta.2`.**
   - Spin up Matcher locally (`docker compose up` from the matcher repo on `develop` once the beta is cut, or pull the published `v2.0.0-beta.2` image from DockerHub / GHCR).
   - In `product-console`, exercise: list contexts, create a context, create a source, create a field map, create a fee schedule, **create / list / get / patch / delete a fee rule** (the high-risk path), and any context-clone flow.
   - Confirm `/health` and `/readyz` (if surfaced in the UI) still parse cleanly.

6. **Document.** Add a single line to `product-console`'s `CHANGELOG.md` (or equivalent) noting the Matcher integration was upgraded, with a reference to this file's source tag.

### Verification checklist

Before opening the PR, confirm all of the following:

- [ ] `rg -n '/v1/config/' --hidden --no-ignore` returns **zero hits** outside of (a) this migration prompt itself if you copy it in, and (b) historical changelog entries.
- [ ] Type checking passes (`tsc --noEmit` or whatever the repo uses).
- [ ] Unit tests pass.
- [ ] Integration / e2e tests pass against the new Matcher beta.
- [ ] If applicable, the regenerated SDK shows expected URL changes in its diff and no unexpected schema drift.
- [ ] No new `console.warn` or runtime warnings about unrecognized `/readyz` keys in the browser console.
- [ ] PR description links back to Matcher commit `ea16bfdc` and references this file.

### Rollback

If something goes wrong, the rollback is trivial: revert the URL renames and pin Matcher back to image tag `v2.0.0-beta.1`. Matcher's data plane and DTOs are unchanged between `v2.0.0-beta.1` and `v2.0.0-beta.2`, so there is no on-disk state migration to worry about either way.

### What you should NOT touch

To keep this PR atomic, **do not** in the same change:

- Refactor unrelated Matcher integration code.
- Migrate to a different HTTP client or query library.
- Regenerate code from any other upstream OpenAPI source.
- Add new features that consume Matcher endpoints not covered above.

Anything beyond URL rename + SDK regen + test updates goes in a separate PR.

### Done criteria

A reviewer should be able to look at the PR diff and immediately see: a tightly-scoped set of URL string changes, an SDK regeneration commit (if applicable), and matching test updates. Nothing else. Total churn should be on the order of dozens of lines, not hundreds.

---

**Source artifact:** This prompt was generated from matcher branch `feat/streaming-and-others` (HEAD `1f3d667a`) against `develop` at tag `v2.0.0-beta.1`. The contract diff was verified by comparing route registrations, swagger path keys, schema definitions (136 ↔ 136 unchanged), and `@Router` annotations. Fee-rule routes are the only true URL change; the other 7 swagger renames are stale-annotation corrections that bring the spec in line with routes that already existed.

**Version computation:** The next beta is `v2.0.0-beta.2` (not `v2.1.0-beta.1`) because `origin/develop` HEAD is still `chore(release): 2.0.0-beta.1` with no `v2.0.0` backmerge yet, and the new commits since beta.1 are at most minor-level under conventional-commits parsing — so semantic-release increments the prerelease counter instead of bumping the underlying `2.0.0` version. Matcher stays in the `v2.x` line.
