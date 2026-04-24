// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package dispute

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v5/commons/assert"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// Sentinel errors for Dispute operations.
var (
	ErrDisputeNil                      = errors.New("dispute is nil")
	ErrDisputeExceptionIDRequired      = errors.New("dispute exception id is required")
	ErrDisputeDescriptionRequired      = errors.New("dispute description is required")
	ErrDisputeOpenedByRequired         = errors.New("dispute opened by is required")
	ErrDisputeResolutionRequired       = errors.New("dispute resolution is required")
	ErrDisputeReopenReasonRequired     = errors.New("dispute reopen reason is required")
	ErrCannotAddEvidenceInCurrentState = errors.New("cannot add evidence in current state")
)

// Dispute represents a dispute aggregate linked to an exception.
type Dispute struct {
	ID           uuid.UUID
	ExceptionID  uuid.UUID
	Category     DisputeCategory
	State        DisputeState
	Description  string
	OpenedBy     string
	Resolution   *string
	ReopenReason *string
	Evidence     []Evidence
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// NewDispute creates a new Dispute in DRAFT state.
func NewDispute(
	ctx context.Context,
	exceptionID uuid.UUID,
	category DisputeCategory,
	description, openedBy string,
) (*Dispute, error) {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "dispute.dispute.new")

	if err := asserter.That(ctx, exceptionID != uuid.Nil, "exception id is required"); err != nil {
		return nil, ErrDisputeExceptionIDRequired
	}

	if err := asserter.That(ctx, category.IsValid(), "invalid dispute category", "category", category.String()); err != nil {
		return nil, ErrInvalidDisputeCategory
	}

	trimmedDescription := strings.TrimSpace(description)
	if err := asserter.NotEmpty(ctx, trimmedDescription, "description is required"); err != nil {
		return nil, ErrDisputeDescriptionRequired
	}

	trimmedOpenedBy := strings.TrimSpace(openedBy)
	if err := asserter.NotEmpty(ctx, trimmedOpenedBy, "opened by is required"); err != nil {
		return nil, ErrDisputeOpenedByRequired
	}

	now := time.Now().UTC()

	return &Dispute{
		ID:          uuid.New(),
		ExceptionID: exceptionID,
		Category:    category,
		State:       DisputeStateDraft,
		Description: trimmedDescription,
		OpenedBy:    trimmedOpenedBy,
		Evidence:    []Evidence{},
		CreatedAt:   now,
		UpdatedAt:   now,
	}, nil
}

// Open transitions the dispute from Draft to Open.
func (dispute *Dispute) Open(ctx context.Context) error {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "dispute.dispute.open")

	if err := asserter.NotNil(ctx, dispute, "dispute is required"); err != nil {
		return ErrDisputeNil
	}

	if err := ValidateDisputeTransition(dispute.State, DisputeStateOpen); err != nil {
		return err
	}

	dispute.State = DisputeStateOpen
	dispute.UpdatedAt = time.Now().UTC()

	return nil
}

// RequestEvidence transitions the dispute from Open to PendingEvidence.
func (dispute *Dispute) RequestEvidence(ctx context.Context) error {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "dispute.dispute.request_evidence")

	if err := asserter.NotNil(ctx, dispute, "dispute is required"); err != nil {
		return ErrDisputeNil
	}

	if err := ValidateDisputeTransition(dispute.State, DisputeStatePendingEvidence); err != nil {
		return err
	}

	dispute.State = DisputeStatePendingEvidence
	dispute.UpdatedAt = time.Now().UTC()

	return nil
}

// AddEvidence adds evidence to the dispute (allowed in Open or PendingEvidence states).
// If in PendingEvidence state, auto-transitions back to Open.
func (dispute *Dispute) AddEvidence(
	ctx context.Context,
	comment, submittedBy string,
	fileURL *string,
) error {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "dispute.dispute.add_evidence")

	if err := asserter.NotNil(ctx, dispute, "dispute is required"); err != nil {
		return ErrDisputeNil
	}

	if dispute.State != DisputeStateOpen && dispute.State != DisputeStatePendingEvidence {
		return ErrCannotAddEvidenceInCurrentState
	}

	evidence, err := NewEvidence(ctx, dispute.ID, comment, submittedBy, fileURL)
	if err != nil {
		return err
	}

	dispute.Evidence = append(dispute.Evidence, *evidence)

	if dispute.State == DisputeStatePendingEvidence {
		dispute.State = DisputeStateOpen
	}

	dispute.UpdatedAt = time.Now().UTC()

	return nil
}

// Win transitions the dispute to Won state with a resolution.
func (dispute *Dispute) Win(ctx context.Context, resolution string) error {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "dispute.dispute.win")

	if err := asserter.NotNil(ctx, dispute, "dispute is required"); err != nil {
		return ErrDisputeNil
	}

	trimmedResolution := strings.TrimSpace(resolution)
	if err := asserter.NotEmpty(ctx, trimmedResolution, "resolution is required"); err != nil {
		return ErrDisputeResolutionRequired
	}

	if err := ValidateDisputeTransition(dispute.State, DisputeStateWon); err != nil {
		return err
	}

	dispute.State = DisputeStateWon
	dispute.Resolution = &trimmedResolution
	dispute.UpdatedAt = time.Now().UTC()

	return nil
}

// Lose transitions the dispute to Lost state with a resolution.
func (dispute *Dispute) Lose(ctx context.Context, resolution string) error {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "dispute.dispute.lose")

	if err := asserter.NotNil(ctx, dispute, "dispute is required"); err != nil {
		return ErrDisputeNil
	}

	trimmedResolution := strings.TrimSpace(resolution)
	if err := asserter.NotEmpty(ctx, trimmedResolution, "resolution is required"); err != nil {
		return ErrDisputeResolutionRequired
	}

	if err := ValidateDisputeTransition(dispute.State, DisputeStateLost); err != nil {
		return err
	}

	dispute.State = DisputeStateLost
	dispute.Resolution = &trimmedResolution
	dispute.UpdatedAt = time.Now().UTC()

	return nil
}

// Reopen transitions the dispute from Lost back to Open.
func (dispute *Dispute) Reopen(ctx context.Context, reason string) error {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "dispute.dispute.reopen")

	if err := asserter.NotNil(ctx, dispute, "dispute is required"); err != nil {
		return ErrDisputeNil
	}

	trimmedReason := strings.TrimSpace(reason)
	if err := asserter.That(ctx, trimmedReason != "", "reopen reason is required"); err != nil {
		return ErrDisputeReopenReasonRequired
	}

	if err := ValidateDisputeTransition(dispute.State, DisputeStateOpen); err != nil {
		return err
	}

	dispute.State = DisputeStateOpen
	dispute.Resolution = nil
	dispute.ReopenReason = &trimmedReason
	dispute.UpdatedAt = time.Now().UTC()

	return nil
}
