# Auth Package

The `internal/auth` package provides authentication, authorization, and multi-tenancy support for the Matcher service. It integrates with `lib-auth` for RBAC and manages tenant-aware context for downstream handlers.

## Overview

This package handles:
1. **JWT extraction and validation**: Parses HMAC-signed JWTs (HS256/384/512) via the internal `pkg/jwt` package, enforces expiration, and extracts claims.
2. **Tenant context**: Extracts `tenant_id`/`tenantId`, optional `tenant_slug`/`tenantSlug`, and `sub` into request context.
3. **Authorization**: Enforces RBAC permissions via `lib-auth` middleware.
4. **Schema isolation**: Applies tenant-specific PostgreSQL search paths using `SET LOCAL search_path`.

## Architecture

```
internal/auth/
├── middleware.go       # TenantExtractor, Authorize, context getters (GetTenantID, GetTenantSlug, GetUserID, GetDefaultTenantID)
├── resources.go        # Resource constants (currently empty)
├── routes.go           # ProtectedGroup, ProtectedGroupWithMiddleware
└── tenant_schema.go    # ApplyTenantSchema
```

## Components

### TenantExtractor

The `TenantExtractor` middleware populates the request context with tenant information.

- **Auth enabled**: Requires a configured token secret, extracts the bearer token, validates signature/expiration, and reads claims (`tenant_id`/`tenantId`, `tenant_slug`/`tenantSlug`, `sub`).
- **Auth disabled**: Sets the configured defaults; if none provided, uses:
  - `DefaultTenantID = "11111111-1111-1111-1111-111111111111"`
  - `DefaultTenantSlug = "default"`

### Middleware

- **`ExtractTenant()`**: Fiber middleware that injects `TenantIDKey`, `TenantSlugKey`, and `UserIDKey` (only when `sub` is present).
- **`Authorize(authClient, resource, action)`**: Wraps `lib-auth` `AuthClient.Authorize` using `constants.ApplicationName`.
- **`ProtectedGroup(...)`**: Creates a Fiber route group with authorization and tenant extraction middleware. If the extractor is nil, the group returns a 500 error.
- **`ProtectedGroupWithMiddleware(...)`**: Same as `ProtectedGroup` but accepts additional middleware to run after authorization.
- **`SetDefaultTenantID(tenantID)`** / **`SetDefaultTenantSlug(tenantSlug)`**: Override default tenant values at runtime (validates UUID format for tenant ID).
- **`GetDefaultTenantID()`**: Returns the current default tenant ID.

### Database Multi-tenancy

`ApplyTenantSchema` sets the PostgreSQL search path for the current tenant, but only inside a transaction.

```go
// Applies SET LOCAL search_path for tenant UUID.
// Requires *sql.Tx to avoid connection pool pollution.
if err := auth.ApplyTenantSchema(ctx, tx); err != nil {
    return err
}
```

- If the tenant is the default tenant, it is a no-op.
- Passing `*sql.Conn` or any non-transaction executor returns an error.

## Usage

### Route Protection

```go
import (
    "github.com/LerianStudio/lib-auth/v2/auth/middleware"
    "github.com/LerianStudio/matcher/internal/auth"
    "github.com/gofiber/fiber/v2"
)

func RegisterRoutes(app *fiber.App, authClient *middleware.AuthClient, te *auth.TenantExtractor) {
    app.Get("/health", healthHandler)

    // Requires "read" permission on "transactions" resource
    api := auth.ProtectedGroup(app, authClient, te, "transactions", "read")
    api.Get("/", listTransactionsHandler)
}
```

### Accessing Context

```go
func listTransactionsHandler(c *fiber.Ctx) error {
    ctx := c.UserContext()

    tenantID := auth.GetTenantID(ctx)
    tenantSlug := auth.GetTenantSlug(ctx)
    userID := auth.GetUserID(ctx)

    // Use tenantID/tenantSlug/userID for business logic...
    return nil
}
```

## Configuration

`TenantExtractor` is created via `NewTenantExtractor(authEnabled, defaultTenantID, defaultTenantSlug, tokenSecret, envName)` and returns `(*TenantExtractor, error)`.
The `envName` parameter controls security features: the `X-User-ID` header is only accepted in non-production environments.

- **Auth enabled**: `tokenSecret` is required.
- **Defaults**: if empty, `DefaultTenantID` and `DefaultTenantSlug` are used.
