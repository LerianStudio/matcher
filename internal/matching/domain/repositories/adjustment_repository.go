// Package repositories provides matching persistence abstractions.
package repositories

import (
	"context"
	"database/sql"

	"github.com/google/uuid"

	libHTTP "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"

	"github.com/LerianStudio/matcher/internal/matching/domain/entities"
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
)

//go:generate mockgen -destination=mocks/adjustment_repository_mock.go -package=mocks . AdjustmentRepository

// AdjustmentRepository defines persistence operations for adjustments.
type AdjustmentRepository interface {
	Create(ctx context.Context, adjustment *entities.Adjustment) (*entities.Adjustment, error)
	// CreateWithTx creates an adjustment within the provided transaction.
	// This enables atomic operations where adjustment creation and audit logging
	// must succeed or fail together (SOX compliance).
	CreateWithTx(
		ctx context.Context,
		tx *sql.Tx,
		adjustment *entities.Adjustment,
	) (*entities.Adjustment, error)
	// CreateWithAuditLog atomically persists an adjustment and its corresponding audit log
	// in a single transaction. This ensures SOX compliance: both records are committed
	// together or both are rolled back on failure.
	CreateWithAuditLog(
		ctx context.Context,
		adjustment *entities.Adjustment,
		auditLog *sharedDomain.AuditLog,
	) (*entities.Adjustment, error)
	// CreateWithAuditLogWithTx atomically persists an adjustment and its corresponding audit log
	// using a caller-owned transaction. The caller is responsible for commit/rollback.
	// This enables composing the adjustment+audit operation inside a larger transaction.
	CreateWithAuditLogWithTx(
		ctx context.Context,
		tx *sql.Tx,
		adjustment *entities.Adjustment,
		auditLog *sharedDomain.AuditLog,
	) (*entities.Adjustment, error)
	FindByID(ctx context.Context, contextID, id uuid.UUID) (*entities.Adjustment, error)
	ListByContextID(
		ctx context.Context,
		contextID uuid.UUID,
		filter CursorFilter,
	) ([]*entities.Adjustment, libHTTP.CursorPagination, error)
	ListByMatchGroupID(
		ctx context.Context,
		contextID, matchGroupID uuid.UUID,
	) ([]*entities.Adjustment, error)
}
