//go:build unit

package command

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

// --- validateBulkIDs ---

func TestValidateBulkIDs_EmptySlice(t *testing.T) {
	t.Parallel()

	_, err := validateBulkIDs(nil)

	require.ErrorIs(t, err, ErrBulkEmptyIDs)
}

func TestValidateBulkIDs_TooManyIDs(t *testing.T) {
	t.Parallel()

	ids := testutil.DeterministicUUIDs("bulk-id", maxBulkIDs+1)

	_, err := validateBulkIDs(ids)

	require.ErrorIs(t, err, ErrBulkTooManyIDs)
}

func TestValidateBulkIDs_ValidIDs(t *testing.T) {
	t.Parallel()

	ids := []uuid.UUID{testutil.DeterministicUUID("valid-id")}

	result, err := validateBulkIDs(ids)

	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, ids[0], result[0])
}

func TestValidateBulkIDs_MaxIDs(t *testing.T) {
	t.Parallel()

	ids := testutil.DeterministicUUIDs("max-id", maxBulkIDs)

	result, err := validateBulkIDs(ids)

	require.NoError(t, err)
	require.Len(t, result, maxBulkIDs)
}

func TestValidateBulkIDs_DuplicateIDs(t *testing.T) {
	t.Parallel()

	id1 := testutil.DeterministicUUID("dup-id-1")
	id2 := testutil.DeterministicUUID("dup-id-2")
	ids := []uuid.UUID{id1, id2, id1, id2, id1}

	result, err := validateBulkIDs(ids)

	require.NoError(t, err)
	require.Len(t, result, 2, "duplicates should be removed")
	assert.Equal(t, id1, result[0], "order should be preserved")
	assert.Equal(t, id2, result[1], "order should be preserved")
}

func TestValidateBulkIDs_AllSameID(t *testing.T) {
	t.Parallel()

	id := testutil.DeterministicUUID("same-id")
	ids := []uuid.UUID{id, id, id}

	result, err := validateBulkIDs(ids)

	require.NoError(t, err)
	require.Len(t, result, 1, "all duplicates should collapse to one")
	assert.Equal(t, id, result[0])
}

// --- BulkAssign ---

func TestBulkAssign_EmptyIDs(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&stubExceptionRepo{},
		&stubResolutionExecutor{},
		&stubAuditPublisher{},
		actorExtractor("analyst"),
		&stubInfraProvider{},
	)
	require.NoError(t, err)

	result, assignErr := uc.BulkAssign(context.Background(), BulkAssignInput{
		ExceptionIDs: nil,
		Assignee:     "someone",
	})

	require.ErrorIs(t, assignErr, ErrBulkEmptyIDs)
	assert.Nil(t, result)
}

func TestBulkAssign_EmptyAssignee(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&stubExceptionRepo{},
		&stubResolutionExecutor{},
		&stubAuditPublisher{},
		actorExtractor("analyst"),
		&stubInfraProvider{},
	)
	require.NoError(t, err)

	result, assignErr := uc.BulkAssign(context.Background(), BulkAssignInput{
		ExceptionIDs: []uuid.UUID{uuid.New()},
		Assignee:     "",
	})

	require.ErrorIs(t, assignErr, ErrBulkAssigneeEmpty)
	assert.Nil(t, result)
}

func TestBulkAssign_WhitespaceAssignee(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&stubExceptionRepo{},
		&stubResolutionExecutor{},
		&stubAuditPublisher{},
		actorExtractor("analyst"),
		&stubInfraProvider{},
	)
	require.NoError(t, err)

	result, assignErr := uc.BulkAssign(context.Background(), BulkAssignInput{
		ExceptionIDs: []uuid.UUID{uuid.New()},
		Assignee:     "   ",
	})

	require.ErrorIs(t, assignErr, ErrBulkAssigneeEmpty)
	assert.Nil(t, result)
}

// --- BulkResolve ---

func TestBulkResolve_EmptyIDs(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&stubExceptionRepo{},
		&stubResolutionExecutor{},
		&stubAuditPublisher{},
		actorExtractor("analyst"),
		&stubInfraProvider{},
	)
	require.NoError(t, err)

	result, resolveErr := uc.BulkResolve(context.Background(), BulkResolveInput{
		ExceptionIDs: nil,
		Resolution:   "ACCEPTED",
	})

	require.ErrorIs(t, resolveErr, ErrBulkEmptyIDs)
	assert.Nil(t, result)
}

func TestBulkResolve_EmptyResolution(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&stubExceptionRepo{},
		&stubResolutionExecutor{},
		&stubAuditPublisher{},
		actorExtractor("analyst"),
		&stubInfraProvider{},
	)
	require.NoError(t, err)

	result, resolveErr := uc.BulkResolve(context.Background(), BulkResolveInput{
		ExceptionIDs: []uuid.UUID{uuid.New()},
		Resolution:   "",
	})

	require.ErrorIs(t, resolveErr, ErrBulkResolutionEmpty)
	assert.Nil(t, result)
}

func TestBulkResolve_WhitespaceResolution(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&stubExceptionRepo{},
		&stubResolutionExecutor{},
		&stubAuditPublisher{},
		actorExtractor("analyst"),
		&stubInfraProvider{},
	)
	require.NoError(t, err)

	result, resolveErr := uc.BulkResolve(context.Background(), BulkResolveInput{
		ExceptionIDs: []uuid.UUID{uuid.New()},
		Resolution:   "   \t  ",
	})

	require.ErrorIs(t, resolveErr, ErrBulkResolutionEmpty)
	assert.Nil(t, result)
}

// --- BulkResolve PENDING_RESOLUTION guard ---

func TestBulkResolve_SkipsPendingResolutionException(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Create an exception in PENDING_RESOLUTION status.
	exception, err := entities.NewException(
		ctx,
		uuid.New(),
		value_objects.ExceptionSeverityMedium,
		nil,
	)
	require.NoError(t, err)
	// Direct field mutation: simulating an already-PENDING_RESOLUTION exception
	// that can't be reached via domain methods in a unit test context (would require
	// a concurrent gateway call to be in-flight). This tests the guard's rejection path.
	exception.Status = value_objects.ExceptionStatusPendingResolution

	repo := &stubExceptionRepo{exception: exception}
	exec := &stubResolutionExecutor{}
	audit := &stubAuditPublisher{}

	uc, err := NewUseCase(repo, exec, audit, actorExtractor("analyst"), &stubInfraProvider{})
	require.NoError(t, err)

	result, resolveErr := uc.BulkResolve(ctx, BulkResolveInput{
		ExceptionIDs: []uuid.UUID{exception.ID},
		Resolution:   "resolve attempt",
	})

	require.NoError(t, resolveErr)
	require.NotNil(t, result)
	assert.Empty(t, result.Succeeded, "PENDING_RESOLUTION exception should not succeed")
	require.Len(t, result.Failed, 1, "PENDING_RESOLUTION exception should be in failed list")
	assert.Contains(t, result.Failed[0].Error, "already pending resolution")
}

// --- Sentinel Errors ---

func TestBulkErrors_AreDistinct(t *testing.T) {
	t.Parallel()

	errs := []error{
		ErrBulkEmptyIDs,
		ErrBulkTooManyIDs,
		ErrBulkAssigneeEmpty,
		ErrBulkResolutionEmpty,
		ErrBulkTargetSystemEmpty,
	}

	seen := make(map[string]int)

	for i, err := range errs {
		msg := err.Error()
		if existingIdx, ok := seen[msg]; ok {
			t.Errorf("duplicate error message %q at index %d and %d", msg, existingIdx, i)
		}

		seen[msg] = i
	}
}
