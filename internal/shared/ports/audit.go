// Package ports defines shared interfaces (ports) for infrastructure abstraction.
package ports

import (
	"context"
	"database/sql"

	"github.com/google/uuid"

	sharedhttp "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
)

// AuditLogRepository defines persistence operations for audit logs.
// This is the shared kernel interface used by all bounded contexts that record
// or query audit logs, avoiding direct imports into governance/domain/repositories.
//
// This is an append-only repository: no Update or Delete methods are defined.
type AuditLogRepository interface {
	// Create inserts a new audit log entry.
	Create(ctx context.Context, auditLog *sharedDomain.AuditLog) (*sharedDomain.AuditLog, error)

	// CreateWithTx inserts a new audit log entry using the provided transaction.
	// This enables atomic audit logging within the same transaction as the audited operation.
	CreateWithTx(
		ctx context.Context,
		tx *sql.Tx,
		auditLog *sharedDomain.AuditLog,
	) (*sharedDomain.AuditLog, error)

	// GetByID retrieves a single audit log by its ID.
	// Tenant is extracted from context via auth.GetTenantID(ctx).
	GetByID(ctx context.Context, id uuid.UUID) (*sharedDomain.AuditLog, error)

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
	) ([]*sharedDomain.AuditLog, string, error)

	// List retrieves audit logs with optional filters using cursor-based pagination.
	// Uses timestamp+ID keyset pagination for correct ordering with (created_at DESC, id DESC).
	// Tenant is extracted from context via auth.GetTenantID(ctx).
	// Returns the logs, next cursor (empty if no more pages), and any error.
	List(
		ctx context.Context,
		filter sharedDomain.AuditLogFilter,
		cursor *sharedhttp.TimestampCursor,
		limit int,
	) ([]*sharedDomain.AuditLog, string, error)
}
