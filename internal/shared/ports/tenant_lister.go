package ports

import "context"

// TenantLister enumerates tenant IDs for background workers that must fan out
// work across tenant-scoped schemas.
type TenantLister interface {
	ListTenants(ctx context.Context) ([]string, error)
}
