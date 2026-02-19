package repositories

import (
	"context"
	"time"

	"github.com/google/uuid"

	libHTTP "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"

	"github.com/LerianStudio/matcher/internal/exception/domain/dispute"
)

// DisputeFilter defines optional filters for listing disputes.
type DisputeFilter struct {
	State    *dispute.DisputeState
	Category *dispute.DisputeCategory
	DateFrom *time.Time
	DateTo   *time.Time
}

//go:generate mockgen -destination=mocks/dispute_repository_mock.go -package=mocks . DisputeRepository

// DisputeRepository defines persistence operations for disputes.
//
// Delete is intentionally omitted: disputes are immutable audit records whose
// lifecycle is governed by a state machine (Draft → Open → Won/Lost, with
// reopen from Lost).  Instead of deletion, disputes reach terminal states
// (Won or Lost) that preserve the full evidence trail for compliance and
// SOX-auditability.  See dispute.AllowedDisputeTransitions for the
// complete state graph.
type DisputeRepository interface {
	Create(ctx context.Context, d *dispute.Dispute) (*dispute.Dispute, error)
	// CreateWithTx creates a dispute using the provided transaction.
	// This enables atomic operations across multiple repositories.
	CreateWithTx(ctx context.Context, tx Tx, d *dispute.Dispute) (*dispute.Dispute, error)
	FindByID(ctx context.Context, id uuid.UUID) (*dispute.Dispute, error)
	FindByExceptionID(ctx context.Context, exceptionID uuid.UUID) (*dispute.Dispute, error)
	// List retrieves disputes with optional filters and cursor pagination.
	List(
		ctx context.Context,
		filter DisputeFilter,
		cursor CursorFilter,
	) ([]*dispute.Dispute, libHTTP.CursorPagination, error)
	Update(ctx context.Context, d *dispute.Dispute) (*dispute.Dispute, error)
	// UpdateWithTx updates a dispute using the provided transaction.
	// This enables atomic operations across multiple repositories.
	UpdateWithTx(ctx context.Context, tx Tx, d *dispute.Dispute) (*dispute.Dispute, error)
}
