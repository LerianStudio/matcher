// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package ports

import "context"

// TenantLister enumerates tenant IDs for background workers that must fan out
// work across tenant-scoped schemas.
type TenantLister interface {
	ListTenants(ctx context.Context) ([]string, error)
}
