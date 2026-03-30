package ports

import "context"

// M2MCredentials holds machine-to-machine authentication credentials
// retrieved from a secret store (e.g., AWS Secrets Manager).
// SECURITY: These fields MUST NOT be logged or serialized to JSON responses.
type M2MCredentials struct {
	ClientID     string `json:"-"`
	ClientSecret string `json:"-"`
}

// M2MProvider abstracts per-tenant M2M credential retrieval with caching.
// In multi-tenant mode, each tenant has its own clientId+clientSecret stored
// in a secret vault. In single-tenant mode, the provider is nil and callers
// skip credential injection entirely.
//
//go:generate mockgen -source=m2m.go -destination=mocks/m2m_mock.go -package=mocks
type M2MProvider interface {
	// GetCredentials returns the M2M credentials for the given tenant.
	// Implementations SHOULD use multi-level caching (L1 in-memory, L2 Redis)
	// to minimize calls to the underlying secret store.
	GetCredentials(ctx context.Context, tenantOrgID string) (*M2MCredentials, error)

	// InvalidateCredentials removes cached credentials for a tenant from all
	// cache levels. Call this when a 401 is received during token exchange
	// to force re-fetch from the secret store on the next request.
	// Returns an error if the Redis L2 eviction fails (L1 is always cleared first).
	InvalidateCredentials(ctx context.Context, tenantOrgID string) error
}
