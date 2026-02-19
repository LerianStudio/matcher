// Package repositories provides governance persistence contracts.
package repositories

//go:generate mockgen -source=audit_repository.go -destination=mocks/audit_repository_mock.go -package=mocks

import (
	"context"
	"database/sql"

	"github.com/google/uuid"

	sharedhttp "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"

	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
)

// Tx is the transaction alias used for audit log repository operations.
type Tx = *sql.Tx

// AuditLogRepository defines persistence operations for audit logs.
// This is an append-only repository: no Update or Delete methods.
type AuditLogRepository interface {
	// Create inserts a new audit log entry.
	Create(ctx context.Context, auditLog *entities.AuditLog) (*entities.AuditLog, error)

	// CreateWithTx inserts a new audit log entry using the provided transaction.
	// This enables atomic audit logging within the same transaction as the audited operation.
	CreateWithTx(
		ctx context.Context,
		tx *sql.Tx,
		auditLog *entities.AuditLog,
	) (*entities.AuditLog, error)

	// GetByID retrieves a single audit log by its ID.
	// Tenant is extracted from context via auth.GetTenantID(ctx).
	GetByID(ctx context.Context, id uuid.UUID) (*entities.AuditLog, error)

	// ListByEntity retrieves audit logs for a specific entity using cursor-based pagination.
	// Uses timestamp+ID keyset pagination for correct ordering with (created_at DESC, id DESC).
	// Tenant is extracted from context via auth.GetTenantID(ctx).
	// Returns the logs, next cursor (empty if no more pages), and any error.
	ListByEntity(
		ctx context.Context,
		entityType string,
		entityID uuid.UUID,
		cursor *sharedhttp.TimestampCursor,
		limit int,
	) ([]*entities.AuditLog, string, error)

	// List retrieves audit logs with optional filters using cursor-based pagination.
	// Uses timestamp+ID keyset pagination for correct ordering with (created_at DESC, id DESC).
	// Tenant is extracted from context via auth.GetTenantID(ctx).
	// Returns the logs, next cursor (empty if no more pages), and any error.
	List(
		ctx context.Context,
		filter entities.AuditLogFilter,
		cursor *sharedhttp.TimestampCursor,
		limit int,
	) ([]*entities.AuditLog, string, error)
}
