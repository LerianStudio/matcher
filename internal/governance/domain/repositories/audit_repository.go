// Package repositories provides governance persistence contracts.
//
// AuditLogRepository is the canonical persistence contract for audit logs.
// It lives here (governance-local) rather than in internal/shared/ports
// because audit logging is governance's core responsibility — matching and
// exception are permitted consumers via explicit depguard carve-outs
// (see cross-context-matching / cross-context-exception rules in .golangci.yml).
package repositories

//go:generate mockgen -destination=mocks/audit_repository_mock.go -package=mocks github.com/LerianStudio/matcher/internal/governance/domain/repositories AuditLogRepository

import (
	"context"
	"database/sql"

	"github.com/google/uuid"

	sharedhttp "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
)

// Tx is the transaction type used by audit log write operations.
// Aliased here so callers that only import governance/domain/repositories
// do not need to also import database/sql for the transaction parameter.
type Tx = *sql.Tx

// AuditLogRepository defines persistence operations for audit logs.
//
// This is an append-only repository: no Update or Delete methods are defined.
// The append-only invariant is enforced by the database schema (trigger
// prevents UPDATE/DELETE on audit_logs) and by the absence of mutating
// methods on this interface.
//
// Hash chain integrity: Create and CreateWithTx append to a per-tenant
// monotonic sequence (tenant_seq) and compute prev_hash/record_hash as
// part of the insert transaction. See
// internal/governance/adapters/postgres/audit_log.postgresql.go for the
// implementation and concurrency model.
type AuditLogRepository interface {
	// Create inserts a new audit log entry.
	// Tenant is extracted from context via auth.GetTenantID(ctx).
	Create(ctx context.Context, auditLog *sharedDomain.AuditLog) (*sharedDomain.AuditLog, error)

	// CreateWithTx inserts a new audit log entry using the provided transaction.
	// This enables atomic audit logging within the same transaction as the
	// audited operation.
	CreateWithTx(
		ctx context.Context,
		tx Tx,
		auditLog *sharedDomain.AuditLog,
	) (*sharedDomain.AuditLog, error)

	// GetByID retrieves a single audit log by its ID.
	// Tenant is extracted from context via auth.GetTenantID(ctx).
	GetByID(ctx context.Context, id uuid.UUID) (*sharedDomain.AuditLog, error)

	// ListByEntity retrieves audit logs for a specific entity using cursor-based pagination.
	// Uses timestamp+ID keyset pagination for correct ordering with
	// (created_at DESC, id DESC).
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
	// Uses timestamp+ID keyset pagination for correct ordering with
	// (created_at DESC, id DESC).
	// Tenant is extracted from context via auth.GetTenantID(ctx).
	// Returns the logs, next cursor (empty if no more pages), and any error.
	List(
		ctx context.Context,
		filter sharedDomain.AuditLogFilter,
		cursor *sharedhttp.TimestampCursor,
		limit int,
	) ([]*sharedDomain.AuditLog, string, error)
}
