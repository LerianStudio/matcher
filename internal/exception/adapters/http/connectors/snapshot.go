package connectors

import (
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/LerianStudio/matcher/internal/exception/domain/services"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	sharedexception "github.com/LerianStudio/matcher/internal/shared/domain/exception"
)

// ExceptionSnapshot is a projection of exception data for external dispatch.
type ExceptionSnapshot struct {
	ID            uuid.UUID
	TransactionID uuid.UUID
	Severity      sharedexception.ExceptionSeverity
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
