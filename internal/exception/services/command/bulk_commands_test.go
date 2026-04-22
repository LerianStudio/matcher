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
	sharedexception "github.com/LerianStudio/matcher/internal/shared/domain/exception"
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

	uc, err := NewExceptionUseCase(&stubExceptionRepo{}, actorExtractor("analyst"), &stubAuditPublisher{}, &stubInfraProvider{}, WithResolutionExecutor(&stubResolutionExecutor{}))
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

	uc, err := NewExceptionUseCase(&stubExceptionRepo{}, actorExtractor("analyst"), &stubAuditPublisher{}, &stubInfraProvider{}, WithResolutionExecutor(&stubResolutionExecutor{}))
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

	uc, err := NewExceptionUseCase(&stubExceptionRepo{}, actorExtractor("analyst"), &stubAuditPublisher{}, &stubInfraProvider{}, WithResolutionExecutor(&stubResolutionExecutor{}))
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

	uc, err := NewExceptionUseCase(&stubExceptionRepo{}, actorExtractor("analyst"), &stubAuditPublisher{}, &stubInfraProvider{}, WithResolutionExecutor(&stubResolutionExecutor{}))
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

	uc, err := NewExceptionUseCase(&stubExceptionRepo{}, actorExtractor("analyst"), &stubAuditPublisher{}, &stubInfraProvider{}, WithResolutionExecutor(&stubResolutionExecutor{}))
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

	uc, err := NewExceptionUseCase(&stubExceptionRepo{}, actorExtractor("analyst"), &stubAuditPublisher{}, &stubInfraProvider{}, WithResolutionExecutor(&stubResolutionExecutor{}))
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
		sharedexception.ExceptionSeverityMedium,
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

	uc, err := NewExceptionUseCase(repo, actorExtractor("analyst"), audit, &stubInfraProvider{}, WithResolutionExecutor(exec))
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

// --- N+1 Regression ---

// newOpenException returns an OPEN-status exception suitable for the
// bulk-resolve path (ValidateResolutionTransition(OPEN -> RESOLVED) succeeds).
func newOpenException(t *testing.T) *entities.Exception {
	t.Helper()

	ex, err := entities.NewException(
		context.Background(),
		uuid.New(),
		sharedexception.ExceptionSeverityMedium,
		nil,
	)
	require.NoError(t, err)

	return ex
}

// TestBulkResolve_NPlusOne_Regression asserts the refactor that replaced
// N FindByID round-trips with a single FindByIDs preload. For a 10-item
// batch the repository must see exactly one FindByIDs call and zero
// FindByID calls; the per-item transaction boundary is preserved, so
// BeginTx and the audit publisher each get hit N times (one UPDATE +
// one outbox insert inside each small tx).
func TestBulkResolve_NPlusOne_Regression(t *testing.T) {
	t.Parallel()

	const batchSize = 10

	ctx := context.Background()

	exceptions := make([]*entities.Exception, 0, batchSize)
	ids := make([]uuid.UUID, 0, batchSize)

	for range batchSize {
		ex := newOpenException(t)
		exceptions = append(exceptions, ex)
		ids = append(ids, ex.ID)
	}

	repo := &stubExceptionRepo{findByIDs: exceptions}
	exec := &stubResolutionExecutor{}
	audit := &stubAuditPublisher{}
	provider := &stubInfraProvider{}

	uc, err := NewExceptionUseCase(repo, actorExtractor("analyst"), audit, provider, WithResolutionExecutor(exec))
	require.NoError(t, err)

	result, resolveErr := uc.BulkResolve(ctx, BulkResolveInput{
		ExceptionIDs: ids,
		Resolution:   "RESOLVED",
	})

	require.NoError(t, resolveErr)
	require.NotNil(t, result)
	assert.Len(t, result.Succeeded, batchSize, "all items should succeed")
	assert.Empty(t, result.Failed)

	assert.Equal(t, 1, repo.findIDsCall,
		"BulkResolve must issue exactly one FindByIDs preload (was N before refactor)")
	assert.Equal(t, int64(batchSize), provider.beginTxCall.Load(),
		"per-item transactions are preserved: one BeginTx per exception")
	assert.Equal(t, batchSize, audit.getCallCount(),
		"one outbox insert per item (inside each per-item tx)")
}

// TestBulkAssign_NPlusOne_Regression mirrors the resolve-side guarantee
// for BulkAssign: one FindByIDs + N per-item transactions.
func TestBulkAssign_NPlusOne_Regression(t *testing.T) {
	t.Parallel()

	const batchSize = 10

	ctx := context.Background()

	exceptions := make([]*entities.Exception, 0, batchSize)
	ids := make([]uuid.UUID, 0, batchSize)

	for range batchSize {
		ex := newOpenException(t)
		exceptions = append(exceptions, ex)
		ids = append(ids, ex.ID)
	}

	repo := &stubExceptionRepo{findByIDs: exceptions}
	exec := &stubResolutionExecutor{}
	audit := &stubAuditPublisher{}
	provider := &stubInfraProvider{}

	uc, err := NewExceptionUseCase(repo, actorExtractor("analyst"), audit, provider, WithResolutionExecutor(exec))
	require.NoError(t, err)

	result, assignErr := uc.BulkAssign(ctx, BulkAssignInput{
		ExceptionIDs: ids,
		Assignee:     "operator-1",
	})

	require.NoError(t, assignErr)
	require.NotNil(t, result)
	assert.Len(t, result.Succeeded, batchSize)
	assert.Empty(t, result.Failed)

	assert.Equal(t, 1, repo.findIDsCall,
		"BulkAssign must issue exactly one FindByIDs preload")
	assert.Equal(t, int64(batchSize), provider.beginTxCall.Load(),
		"per-item transactions preserved")
	assert.Equal(t, batchSize, audit.getCallCount(),
		"one outbox insert per item")
}

// TestBulkResolve_MissingIDReportedAsNotFound asserts that when FindByIDs
// returns a subset of the requested ids (the rest were not found), each
// missing id is reported in Failed with entities.ErrExceptionNotFound
// rather than silently succeeding. This keeps the not-found flow
// indistinguishable from the pre-refactor FindByID-per-item path.
func TestBulkResolve_MissingIDReportedAsNotFound(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	found := newOpenException(t)
	missingID := uuid.New()

	repo := &stubExceptionRepo{findByIDs: []*entities.Exception{found}}
	exec := &stubResolutionExecutor{}
	audit := &stubAuditPublisher{}
	provider := &stubInfraProvider{}

	uc, err := NewExceptionUseCase(repo, actorExtractor("analyst"), audit, provider, WithResolutionExecutor(exec))
	require.NoError(t, err)

	result, resolveErr := uc.BulkResolve(ctx, BulkResolveInput{
		ExceptionIDs: []uuid.UUID{found.ID, missingID},
		Resolution:   "RESOLVED",
	})

	require.NoError(t, resolveErr)
	require.NotNil(t, result)
	assert.Equal(t, []uuid.UUID{found.ID}, result.Succeeded)
	require.Len(t, result.Failed, 1)
	assert.Equal(t, missingID, result.Failed[0].ExceptionID)
	assert.Contains(t, result.Failed[0].Error, "exception not found")

	assert.Equal(t, 1, repo.findIDsCall, "still only one preload")
	assert.Equal(t, int64(1), provider.beginTxCall.Load(), "only the found item opens a tx")
	assert.Equal(t, 1, audit.getCallCount(), "only the found item emits audit")
}
