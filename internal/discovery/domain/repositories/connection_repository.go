// Package repositories defines repository interfaces for the Discovery bounded context.
// These interfaces specify persistence contracts for domain aggregates, following the
// Hexagonal Architecture pattern where the domain defines its own storage requirements.
//
// The package exports the following repository interfaces:
//
//   - [ConnectionRepository]: Manages [entities.FetcherConnection] aggregates, representing
//     database connections discovered from the Fetcher service. Supports upsert semantics
//     for idempotent sync, lookup by internal or Fetcher-assigned ID, and stale cleanup.
//
//   - [SchemaRepository]: Manages [entities.DiscoveredSchema] entities that represent
//     table schemas discovered from Fetcher connections. Supports batch upsert for
//     efficient schema sync and lookup by parent connection.
//
//   - [ExtractionRepository]: Manages [entities.ExtractionRequest] entities that track
//     data extraction jobs submitted to the Fetcher service. Supports lifecycle tracking
//     from creation through submission, extraction, and completion or failure.
//
// Implementations of these interfaces will be provided in the adapters/postgres package,
// which handles tenant isolation, transaction management, and cursor-based pagination.
// Use cases in the services/command and services/query packages depend on these
// interfaces for database-agnostic persistence.
package repositories

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
)

// Domain-level sentinel errors for connection repository operations.
// These are defined at the domain layer to allow service-layer code to use errors.Is()
// without importing adapter packages (depguard compliance).
var (
	ErrConnectionNotFound = errors.New("fetcher connection not found")
	ErrProviderRequired   = errors.New("infrastructure provider is required")
	ErrRepoNotInitialized = errors.New("connection repository not initialized")
	ErrEntityRequired     = errors.New("fetcher connection entity is required")
	ErrModelRequired      = errors.New("fetcher connection model is required")
)

// ConnectionRepository defines persistence operations for FetcherConnection entities.
type ConnectionRepository interface {
	// Upsert creates or updates a FetcherConnection based on FetcherConnID.
	Upsert(ctx context.Context, conn *entities.FetcherConnection) error
	// UpsertWithTx creates or updates a FetcherConnection within an existing transaction.
	UpsertWithTx(ctx context.Context, tx *sql.Tx, conn *entities.FetcherConnection) error
	// FindAll returns all known FetcherConnections.
	FindAll(ctx context.Context) ([]*entities.FetcherConnection, error)
	// FindByID retrieves a FetcherConnection by its internal ID.
	FindByID(ctx context.Context, id uuid.UUID) (*entities.FetcherConnection, error)
	// FindByFetcherID retrieves a FetcherConnection by its Fetcher-assigned external ID.
	FindByFetcherID(ctx context.Context, fetcherConnID string) (*entities.FetcherConnection, error)
	// DeleteStale removes connections not seen since the given duration.
	DeleteStale(ctx context.Context, notSeenSince time.Duration) (int64, error)
	// DeleteStaleWithTx removes stale connections within an existing transaction.
	DeleteStaleWithTx(ctx context.Context, tx *sql.Tx, notSeenSince time.Duration) (int64, error)
}
