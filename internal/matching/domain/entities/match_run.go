package entities

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v4/commons/assert"
	"github.com/LerianStudio/lib-commons/v4/commons/pointers"

	"github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// Sentinel errors for MatchRun operations.
var (
	ErrMatchRunMustBeProcessingToComplete = errors.New("match run must be processing to complete")
	ErrMatchRunMustBeProcessingToFail     = errors.New("match run must be processing to fail")
	ErrFailureReasonRequired              = errors.New("failure reason is required")
)

// MatchRun represents an execution of the matching engine.
type MatchRun struct {
	ID            uuid.UUID
	ContextID     uuid.UUID
	Mode          value_objects.MatchRunMode
	Status        value_objects.MatchRunStatus
	StartedAt     time.Time
	CompletedAt   *time.Time
	Stats         map[string]int
	FailureReason *string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// NewMatchRun creates a new MatchRun in processing status.
func NewMatchRun(
	ctx context.Context,
	contextID uuid.UUID,
	mode value_objects.MatchRunMode,
) (*MatchRun, error) {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "matching.match_run.new")

	if err := asserter.That(ctx, contextID != uuid.Nil, "context id is required"); err != nil {
		return nil, fmt.Errorf("match run context id: %w", err)
	}

	if err := asserter.That(ctx, mode.IsValid(), "invalid match run mode", "mode", mode.String()); err != nil {
		return nil, fmt.Errorf("match run mode: %w", err)
	}

	now := time.Now().UTC()

	return &MatchRun{
		ID:        uuid.New(),
		ContextID: contextID,
		Mode:      mode,
		Status:    value_objects.MatchRunStatusProcessing,
		StartedAt: now,
		Stats:     make(map[string]int),
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

// Complete transitions the match run to completed status with stats.
func (run *MatchRun) Complete(ctx context.Context, stats map[string]int) error {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "matching.match_run.complete")

	if err := asserter.NotNil(ctx, run, "match run is required"); err != nil {
		return fmt.Errorf("match run required: %w", err)
	}

	if run.Status != value_objects.MatchRunStatusProcessing {
		return ErrMatchRunMustBeProcessingToComplete
	}

	run.Status = value_objects.MatchRunStatusCompleted
	run.FailureReason = nil

	if stats == nil {
		stats = make(map[string]int)
	}

	statsCopy := make(map[string]int, len(stats))
	maps.Copy(statsCopy, stats)

	run.Stats = statsCopy
	now := time.Now().UTC()
	run.CompletedAt = pointers.Time(now)
	run.UpdatedAt = now

	return nil
}

// Fail transitions the match run to failed status with a reason.
func (run *MatchRun) Fail(ctx context.Context, reason string) error {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "matching.match_run.fail")

	if err := asserter.NotNil(ctx, run, "match run is required"); err != nil {
		return fmt.Errorf("match run required: %w", err)
	}

	if run.Status != value_objects.MatchRunStatusProcessing {
		return ErrMatchRunMustBeProcessingToFail
	}

	if reason == "" {
		return ErrFailureReasonRequired
	}

	run.Status = value_objects.MatchRunStatusFailed
	run.FailureReason = pointers.String(reason)
	now := time.Now().UTC()
	run.CompletedAt = pointers.Time(now)
	run.UpdatedAt = now

	return nil
}
