package ports

import (
	"context"
	"time"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// SchemaCache defines an optional cache layer for discovered schema data.
// Implementations should handle cache misses gracefully.
type SchemaCache interface {
	// GetSchema retrieves cached schema for a connection.
	// Returns (nil, ErrCacheMiss) on cache miss.
	GetSchema(ctx context.Context, connectionID string) (*sharedPorts.FetcherSchema, error)

	// SetSchema stores a schema in the cache with a TTL.
	SetSchema(ctx context.Context, connectionID string, schema *sharedPorts.FetcherSchema, ttl time.Duration) error

	// InvalidateSchema removes cached schema for a connection.
	InvalidateSchema(ctx context.Context, connectionID string) error
}
