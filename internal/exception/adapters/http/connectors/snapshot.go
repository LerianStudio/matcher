package connectors

import (
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/LerianStudio/matcher/internal/exception/domain/services"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
)

// ExceptionSnapshot is a projection of exception data for external dispatch.
type ExceptionSnapshot struct {
	ID            uuid.UUID
	TransactionID uuid.UUID
	Severity      value_objects.ExceptionSeverity
	Status        value_objects.ExceptionStatus
	Amount        decimal.Decimal
	Currency      string
	Reason        string
	SourceType    string
	CreatedAt     time.Time
	DueAt         *time.Time
}

// DispatchContext contains all data needed for external dispatch.
type DispatchContext struct {
	Snapshot  ExceptionSnapshot
	Decision  services.RoutingDecision
	Timestamp time.Time
	TraceID   string
}
