package entities

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v4/commons/assert"

	"github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/shared/constants"
)

const (
	minMatchConfidence    = 60
	autoConfirmConfidence = 90
	minMatchItems         = 2
	maxMatchItems         = 500
)

// Sentinel errors for MatchGroup operations.
var (
	ErrMatchGroupMustBeProposedToConfirm  = errors.New("match group must be proposed to confirm")
	ErrMatchGroupMustBeProposedToReject   = errors.New("match group must be proposed to reject")
	ErrMatchGroupMustBeConfirmedToRevoke  = errors.New("match group must be confirmed to revoke")
	ErrMatchGroupRevocationReasonRequired = errors.New("revocation reason is required")
)

// MatchGroup represents an aggregate of matched transactions.
type MatchGroup struct {
	ID        uuid.UUID
	ContextID uuid.UUID
	RunID     uuid.UUID
	RuleID    uuid.UUID
	// Confidence score on a 0–100 integer scale representing match certainty. Scores at or above 60 qualify for matching; scores at or above 90 trigger auto-confirmation. The value is always a whole number with no fractional component.
	Confidence     value_objects.ConfidenceScore `swaggertype:"integer" minimum:"0" maximum:"100" example:"85"`
	Status         value_objects.MatchGroupStatus
	Items          []*MatchItem
	CreatedAt      time.Time
	UpdatedAt      time.Time
	RejectedReason *string
	ConfirmedAt    *time.Time
}

// NewMatchGroup creates a new MatchGroup aggregate with validated items.
func NewMatchGroup(
	ctx context.Context,
	contextID, runID, ruleID uuid.UUID,
	confidence value_objects.ConfidenceScore,
	items []*MatchItem,
) (*MatchGroup, error) {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "matching.match_group.new")

	if err := asserter.That(ctx, contextID != uuid.Nil, "context id is required"); err != nil {
		return nil, fmt.Errorf("match group context id: %w", err)
	}

	if err := asserter.That(ctx, runID != uuid.Nil, "run id is required"); err != nil {
		return nil, fmt.Errorf("match group run id: %w", err)
	}

	// ruleID may be uuid.Nil for manual matches (no automated rule applied).

	if err := asserter.That(ctx, confidence.Value() >= 0 && confidence.Value() <= 100, "confidence score out of bounds", "confidence", confidence.Value()); err != nil {
		return nil, fmt.Errorf("match group confidence bounds: %w", err)
	}

	if err := asserter.That(ctx, confidence.Value() >= minMatchConfidence, "confidence below minimum match threshold", "confidence", confidence.Value()); err != nil {
		return nil, fmt.Errorf("match group confidence threshold: %w", err)
	}

	if err := asserter.That(ctx, len(items) >= minMatchItems, "match group must include at least two items", "item_count", len(items)); err != nil {
		return nil, fmt.Errorf("match group item count: %w", err)
	}

	if err := asserter.That(ctx, len(items) <= maxMatchItems, "match group must not exceed maximum items", "item_count", len(items)); err != nil {
		return nil, fmt.Errorf("match group item count: %w", err)
	}

	groupID := uuid.New()

	for index, item := range items {
		if err := asserter.NotNil(ctx, item, "match group item is required", "item_index", index); err != nil {
			return nil, fmt.Errorf("match group item %d: %w", index, err)
		}

		item.MatchGroupID = groupID
	}

	now := time.Now().UTC()

	return &MatchGroup{
		ID:         groupID,
		ContextID:  contextID,
		RunID:      runID,
		RuleID:     ruleID,
		Confidence: confidence,
		Status:     value_objects.MatchGroupStatusProposed,
		Items:      items,
		CreatedAt:  now,
		UpdatedAt:  now,
	}, nil
}

// Confirm transitions the match group to confirmed status.
func (group *MatchGroup) Confirm(ctx context.Context) error {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "matching.match_group.confirm")

	if err := asserter.NotNil(ctx, group, "match group is required"); err != nil {
		return fmt.Errorf("match group required: %w", err)
	}

	if group.Status != value_objects.MatchGroupStatusProposed {
		return ErrMatchGroupMustBeProposedToConfirm
	}

	now := time.Now().UTC()
	group.Status = value_objects.MatchGroupStatusConfirmed
	group.ConfirmedAt = &now
	group.UpdatedAt = now

	return nil
}

// CanAutoConfirm returns true if the confidence score is high enough for auto-confirmation.
func (group *MatchGroup) CanAutoConfirm() bool {
	if group == nil {
		return false
	}

	return group.Confidence.Value() >= autoConfirmConfidence
}

// Reject transitions the match group to rejected status with a reason.
func (group *MatchGroup) Reject(ctx context.Context, reason string) error {
	reason = strings.TrimSpace(reason)

	asserter := assert.New(ctx, nil, constants.ApplicationName, "matching.match_group.reject")

	if err := asserter.NotNil(ctx, group, "match group is required"); err != nil {
		return fmt.Errorf("match group required: %w", err)
	}

	if group.Status != value_objects.MatchGroupStatusProposed {
		return ErrMatchGroupMustBeProposedToReject
	}

	if err := asserter.NotEmpty(ctx, reason, "rejection reason is required"); err != nil {
		return fmt.Errorf("match group rejection reason: %w", err)
	}

	group.Status = value_objects.MatchGroupStatusRejected
	group.RejectedReason = &reason
	group.UpdatedAt = time.Now().UTC()

	return nil
}

// Revoke transitions a confirmed match group to revoked status with a reason.
// Unlike Reject(), this method is specifically for undoing a confirmation and
// should trigger compensating events to notify downstream systems.
// ConfirmedAt is preserved to maintain the audit trail of when the group was
// originally confirmed before revocation.
func (group *MatchGroup) Revoke(ctx context.Context, reason string) error {
	reason = strings.TrimSpace(reason)

	asserter := assert.New(ctx, nil, constants.ApplicationName, "matching.match_group.revoke")

	if err := asserter.NotNil(ctx, group, "match group is required"); err != nil {
		return fmt.Errorf("match group required: %w", err)
	}

	if group.Status != value_objects.MatchGroupStatusConfirmed {
		return ErrMatchGroupMustBeConfirmedToRevoke
	}

	if err := asserter.NotEmpty(ctx, reason, "revocation reason is required"); err != nil {
		return fmt.Errorf("match group revocation reason: %w", errors.Join(err, ErrMatchGroupRevocationReasonRequired))
	}

	group.Status = value_objects.MatchGroupStatusRevoked
	group.RejectedReason = &reason
	group.UpdatedAt = time.Now().UTC()

	return nil
}
