package entities

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-uncommons/v2/uncommons/assert"

	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	"github.com/LerianStudio/matcher/internal/shared/utils"
)

// Sentinel errors for Exception operations.
var (
	ErrExceptionNotFound                      = errors.New("exception not found")
	ErrExceptionNil                           = errors.New("exception is nil")
	ErrTransactionIDRequired                  = errors.New("transaction id is required")
	ErrInvalidExceptionSeverity               = errors.New("invalid exception severity")
	ErrExceptionMustBeOpenToAssign            = errors.New("exception must be open to assign")
	ErrExceptionMustBeOpenOrAssignedToResolve = errors.New(
		"exception must be open or assigned to resolve",
	)
	ErrResolutionNotesRequired           = errors.New("resolution notes are required")
	ErrAssigneeRequired                  = errors.New("assignee is required")
	ErrAssignedExceptionRequiresAssignee = errors.New(
		"assigned exception must have an assignee",
	)
	ErrExceptionMustBeAssignedToUnassign = errors.New("exception must be assigned to unassign")
	ErrExceptionPendingResolution        = errors.New(
		"exception is already pending resolution",
	)
	ErrExceptionMustBePendingToAbort = errors.New(
		"exception must be pending resolution to abort",
	)
	ErrInvalidAbortTargetStatus = errors.New(
		"abort resolution target must be OPEN or ASSIGNED",
	)
)

// Exception represents the exception aggregate.
type Exception struct {
	ID               uuid.UUID
	TransactionID    uuid.UUID
	Severity         value_objects.ExceptionSeverity
	Status           value_objects.ExceptionStatus
	ExternalSystem   *string
	ExternalIssueID  *string
	AssignedTo       *string
	DueAt            *time.Time
	ResolutionNotes  *string
	ResolutionType   *string
	ResolutionReason *string
	Reason           *string
	Version          int64
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

type resolveOptions struct {
	resolutionType   string
	resolutionReason string
}

// ResolveOption is a functional option for the Resolve method.
type ResolveOption func(*resolveOptions)

// WithResolutionType sets the resolution type.
func WithResolutionType(t string) ResolveOption {
	return func(o *resolveOptions) {
		o.resolutionType = t
	}
}

// WithResolutionReason sets the resolution reason.
func WithResolutionReason(r string) ResolveOption {
	return func(o *resolveOptions) {
		o.resolutionReason = r
	}
}

// NewException creates a new Exception aggregate in OPEN status.
func NewException(
	ctx context.Context,
	transactionID uuid.UUID,
	severity value_objects.ExceptionSeverity,
	reason *string,
) (*Exception, error) {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "exception.exception.new")

	if err := asserter.That(ctx, transactionID != uuid.Nil, "transaction id is required"); err != nil {
		return nil, ErrTransactionIDRequired
	}

	if err := asserter.That(ctx, severity.IsValid(), "invalid exception severity", "severity", severity.String()); err != nil {
		return nil, ErrInvalidExceptionSeverity
	}

	now := time.Now().UTC()

	return &Exception{
		ID:            uuid.New(),
		TransactionID: transactionID,
		Severity:      severity,
		Status:        value_objects.ExceptionStatusOpen,
		Reason:        utils.NormalizeOptionalText(reason),
		CreatedAt:     now,
		UpdatedAt:     now,
	}, nil
}

// Assign transitions the exception from OPEN to ASSIGNED.
func (exception *Exception) Assign(ctx context.Context, assignee string, dueAt *time.Time) error {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "exception.exception.assign")

	if err := asserter.NotNil(ctx, exception, "exception is required"); err != nil {
		return ErrExceptionNil
	}

	if exception.Status != value_objects.ExceptionStatusOpen {
		return ErrExceptionMustBeOpenToAssign
	}

	trimmedAssignee := strings.TrimSpace(assignee)
	if err := asserter.NotEmpty(ctx, trimmedAssignee, "assignee is required"); err != nil {
		return ErrAssigneeRequired
	}

	exception.Status = value_objects.ExceptionStatusAssigned
	exception.AssignedTo = &trimmedAssignee
	exception.DueAt = dueAt
	exception.UpdatedAt = time.Now().UTC()

	return nil
}

// Unassign transitions the exception from ASSIGNED back to OPEN.
func (exception *Exception) Unassign(ctx context.Context) error {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "exception.exception.unassign")

	if err := asserter.NotNil(ctx, exception, "exception is required"); err != nil {
		return ErrExceptionNil
	}

	if exception.Status != value_objects.ExceptionStatusAssigned {
		return ErrExceptionMustBeAssignedToUnassign
	}

	exception.Status = value_objects.ExceptionStatusOpen
	exception.AssignedTo = nil
	exception.DueAt = nil
	exception.UpdatedAt = time.Now().UTC()

	return nil
}

// StartResolution transitions the exception to PENDING_RESOLUTION.
// Valid source statuses: OPEN or ASSIGNED.
// This is an intermediate guard status that prevents re-processing
// while an external gateway call is in progress.
func (exception *Exception) StartResolution(ctx context.Context) error {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "exception.exception.start_resolution")

	if err := asserter.NotNil(ctx, exception, "exception is required"); err != nil {
		return ErrExceptionNil
	}

	if exception.Status == value_objects.ExceptionStatusPendingResolution {
		return ErrExceptionPendingResolution
	}

	if exception.Status != value_objects.ExceptionStatusOpen &&
		exception.Status != value_objects.ExceptionStatusAssigned {
		return ErrExceptionMustBeOpenOrAssignedToResolve
	}

	exception.Status = value_objects.ExceptionStatusPendingResolution
	exception.UpdatedAt = time.Now().UTC()

	return nil
}

// AbortResolution reverts the exception from PENDING_RESOLUTION to the given
// previous status. Used when a gateway call fails and the exception must be
// returned to its original state.
func (exception *Exception) AbortResolution(
	ctx context.Context,
	previousStatus value_objects.ExceptionStatus,
) error {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "exception.exception.abort_resolution")

	if err := asserter.NotNil(ctx, exception, "exception is required"); err != nil {
		return ErrExceptionNil
	}

	if exception.Status != value_objects.ExceptionStatusPendingResolution {
		return ErrExceptionMustBePendingToAbort
	}

	if previousStatus != value_objects.ExceptionStatusOpen &&
		previousStatus != value_objects.ExceptionStatusAssigned {
		return ErrInvalidAbortTargetStatus
	}

	exception.Status = previousStatus
	exception.UpdatedAt = time.Now().UTC()

	return nil
}

// Resolve transitions the exception to RESOLVED with resolution notes.
func (exception *Exception) Resolve(
	ctx context.Context,
	notes string,
	opts ...ResolveOption,
) error {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "exception.exception.resolve")

	if err := asserter.NotNil(ctx, exception, "exception is required"); err != nil {
		return ErrExceptionNil
	}

	if exception.Status != value_objects.ExceptionStatusOpen &&
		exception.Status != value_objects.ExceptionStatusAssigned &&
		exception.Status != value_objects.ExceptionStatusPendingResolution {
		return ErrExceptionMustBeOpenOrAssignedToResolve
	}

	trimmedNotes := strings.TrimSpace(notes)
	if err := asserter.NotEmpty(ctx, trimmedNotes, "resolution notes are required"); err != nil {
		return ErrResolutionNotesRequired
	}

	if exception.Status == value_objects.ExceptionStatusAssigned {
		if exception.AssignedTo == nil || strings.TrimSpace(*exception.AssignedTo) == "" {
			return ErrAssignedExceptionRequiresAssignee
		}
	}

	var options resolveOptions
	for _, opt := range opts {
		opt(&options)
	}

	exception.Status = value_objects.ExceptionStatusResolved
	exception.ResolutionNotes = &trimmedNotes

	if options.resolutionType != "" {
		exception.ResolutionType = &options.resolutionType
	}

	if options.resolutionReason != "" {
		exception.ResolutionReason = &options.resolutionReason
	}

	exception.UpdatedAt = time.Now().UTC()

	return nil
}
