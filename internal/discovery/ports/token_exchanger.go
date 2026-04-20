package ports

import "context"

// TokenExchanger abstracts the OAuth2 client_credentials token exchange
// used by the Fetcher HTTP client for Bearer authentication.
// Implementations handle token caching, refresh, and per-tenant invalidation.
type TokenExchanger interface {
	// GetToken returns a valid Bearer token for the given client credentials.
	// Implementations should cache tokens and only contact the auth service on miss or expiry.
	GetToken(ctx context.Context, clientID, clientSecret string) (string, error)

	// InvalidateToken removes the cached token for the given client_id,
	// forcing a fresh exchange on the next GetToken call.
	InvalidateToken(clientID string)

	// InvalidateTokenByTenant removes the cached Bearer token associated with a tenant.
	// Uses a reverse tenant->clientID mapping to avoid calling GetCredentials during 401 recovery.
	InvalidateTokenByTenant(tenantOrgID string)

	// RegisterTenantClient records the mapping from tenantOrgID to clientID so that
	// InvalidateTokenByTenant can efficiently invalidate the correct token.
	RegisterTenantClient(tenantOrgID, clientID string)
}
