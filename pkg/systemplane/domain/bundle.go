// Copyright 2025 Lerian Studio.

package domain

import "context"

// RuntimeBundle is an application-defined container for runtime dependencies
// that are rebuilt when configuration changes (e.g., database connection pools,
// rate limiters, cache clients). Implementations must release held resources
// in Close.
type RuntimeBundle interface {
	Close(ctx context.Context) error
}
