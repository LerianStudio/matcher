package repositories

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

//go:generate mockgen -source=transaction_repository.go -destination=mocks/transaction_repository_mock.go -package=mocks

// ExternalIDKey represents a unique key for external ID lookup.
type ExternalIDKey struct {
	SourceID   uuid.UUID
	ExternalID string
}

// TransactionSearchParams defines filter criteria for transaction search.
type TransactionSearchParams struct {
	Query     string           // Free text search across reference, description
	AmountMin *decimal.Decimal // Minimum amount filter
	AmountMax *decimal.Decimal // Maximum amount filter
	DateFrom  *time.Time       // Start date filter
	DateTo    *time.Time       // End date filter
	Reference string           // Exact reference match
	Currency  string           // Currency code filter
	SourceID  *uuid.UUID       // Filter by source
	Status    string           // matched/unmatched filter
	Limit     int              // Max results (default 20, max 50)
	Offset    int              // Pagination offset
}

// TransactionRepository defines the interface for transaction persistence.
type TransactionRepository interface {
	Create(ctx context.Context, tx *shared.Transaction) (*shared.Transaction, error)
	CreateBatch(ctx context.Context, txs []*shared.Transaction) ([]*shared.Transaction, error)
	FindByID(ctx context.Context, id uuid.UUID) (*shared.Transaction, error)
	FindByJobID(
		ctx context.Context,
		jobID uuid.UUID,
		filter CursorFilter,
	) ([]*shared.Transaction, libHTTP.CursorPagination, error)
	FindByJobAndContextID(
		ctx context.Context,
		jobID, contextID uuid.UUID,
		filter CursorFilter,
	) ([]*shared.Transaction, libHTTP.CursorPagination, error)
	FindBySourceAndExternalID(
		ctx context.Context,
		sourceID uuid.UUID,
		externalID string,
	) (*shared.Transaction, error)
	ExistsBySourceAndExternalID(
		ctx context.Context,
		sourceID uuid.UUID,
		externalID string,
	) (bool, error)
	ExistsBulkBySourceAndExternalID(
		ctx context.Context,
		keys []ExternalIDKey,
	) (map[ExternalIDKey]bool, error)
	UpdateStatus(
		ctx context.Context,
		id, contextID uuid.UUID,
		status shared.TransactionStatus,
	) (*shared.Transaction, error)
	SearchTransactions(
		ctx context.Context,
		contextID uuid.UUID,
		params TransactionSearchParams,
	) ([]*shared.Transaction, int64, error)
}
