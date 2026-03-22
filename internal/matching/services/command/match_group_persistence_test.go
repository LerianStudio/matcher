//go:build unit

package command

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/matching/ports"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

func TestCommitMatchResults_RefreshFailed_Returns_Error(t *testing.T) {
	t.Parallel()

	matchRunRepo := &stubMatchRunRepo{}
	uc := &UseCase{matchRunRepo: matchRunRepo}

	run, err := matchingEntities.NewMatchRun(context.Background(), uuid.New(), matchingVO.MatchRunModeCommit)
	require.NoError(t, err)

	refreshFailed := &atomic.Bool{}
	refreshFailed.Store(true)

	_, err = uc.commitMatchResults(
		context.Background(), nil, run,
		nil, nil, nil, nil, nil, nil,
		refreshFailed,
		map[string]int{},
		nil,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrLockRefreshFailed)
}

func TestCompleteDryRun_RefreshFailed_Returns_LockError(t *testing.T) {
	t.Parallel()

	matchRunRepo := &stubMatchRunRepo{}
	uc := &UseCase{matchRunRepo: matchRunRepo}

	run, err := matchingEntities.NewMatchRun(context.Background(), uuid.New(), matchingVO.MatchRunModeDryRun)
	require.NoError(t, err)

	refreshFailed := &atomic.Bool{}
	refreshFailed.Store(true)

	_, _, err = uc.completeDryRun(
		context.Background(), nil, run,
		map[string]int{"matches": 0}, nil, refreshFailed,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrLockRefreshFailed)
}

func TestCompleteDryRun_UpdateError_Returns_Wrapped(t *testing.T) {
	t.Parallel()

	matchRunRepo := &stubMatchRunRepo{updateErr: errors.New("update failed")}
	uc := &UseCase{matchRunRepo: matchRunRepo}

	run, err := matchingEntities.NewMatchRun(context.Background(), uuid.New(), matchingVO.MatchRunModeDryRun)
	require.NoError(t, err)

	_, _, err = uc.completeDryRun(
		context.Background(), nil, run,
		map[string]int{"matches": 0}, nil, &atomic.Bool{},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to update match run")
}

func TestCompleteDryRun_Success_ReturnsGroups(t *testing.T) {
	t.Parallel()

	matchRunRepo := &stubMatchRunRepo{}
	uc := &UseCase{matchRunRepo: matchRunRepo}

	run, err := matchingEntities.NewMatchRun(context.Background(), uuid.New(), matchingVO.MatchRunModeDryRun)
	require.NoError(t, err)

	confidence, _ := matchingVO.ParseConfidenceScore(90)
	groups := []*matchingEntities.MatchGroup{
		{ID: uuid.New(), Confidence: confidence},
	}

	updatedRun, resultGroups, err := uc.completeDryRun(
		context.Background(), nil, run,
		map[string]int{"matches": 1}, groups, &atomic.Bool{},
	)
	require.NoError(t, err)
	require.NotNil(t, updatedRun)
	assert.Len(t, resultGroups, 1)
}

func TestPerformFeeVerification_NilFeeInput_Returns_NoError(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}
	err := uc.performFeeVerification(context.Background(), nil, nil, nil, nil)
	require.NoError(t, err)
}

func TestPerformFeeVerification_NilCtxInfo_Returns_NoError(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}
	err := uc.performFeeVerification(context.Background(), nil, nil, nil, &feeVerificationInput{
		ctxInfo: nil,
	})
	require.NoError(t, err)
}

func TestPerformFeeVerification_NilRateID_Returns_NoError(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}
	err := uc.performFeeVerification(context.Background(), nil, nil, nil, &feeVerificationInput{
		ctxInfo: &ports.ReconciliationContextInfo{RateID: nil},
	})
	require.NoError(t, err)
}

func TestPersistFeeFindings_EmptyFindings_NoOp(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		feeVarianceRepo:  &stubFeeVarianceRepo{},
		exceptionCreator: &stubExceptionCreator{},
	}
	err := uc.persistFeeFindings(context.Background(), nil, nil, nil, &feeFindings{})
	require.NoError(t, err)
}

func TestPersistFeeFindings_VariancePersistError(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		feeVarianceRepo: &stubFeeVarianceRepo{err: errors.New("persist failed")},
	}
	run := &matchingEntities.MatchRun{ContextID: uuid.New()}
	findings := &feeFindings{
		variances: []*matchingEntities.FeeVariance{{}},
	}

	err := uc.persistFeeFindings(context.Background(), nil, nil, run, findings)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "persist fee variances")
}

func TestPersistFeeFindings_ExceptionPersistError(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		feeVarianceRepo:  &stubFeeVarianceRepo{},
		exceptionCreator: &stubExceptionCreator{err: errors.New("exception failed")},
	}
	run := &matchingEntities.MatchRun{ID: uuid.New(), ContextID: uuid.New()}
	findings := &feeFindings{
		exceptionInputs: []ports.ExceptionTransactionInput{{TransactionID: uuid.New()}},
	}

	err := uc.persistFeeFindings(context.Background(), nil, nil, run, findings)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create fee exceptions")
}

func TestProcessFeeForItem_NilItem_Returns_Nil(t *testing.T) {
	t.Parallel()

	result := processFeeForItem(
		context.Background(), nil, nil, nil, nil,
		&feeVerificationInput{}, &fee.Rate{}, fee.Tolerance{},
	)
	assert.Nil(t, result)
}

func TestCollectFeeFindings_SkipsNilAndNonConfirmed(t *testing.T) {
	t.Parallel()

	confidence, _ := matchingVO.ParseConfidenceScore(50)
	groups := []*matchingEntities.MatchGroup{
		nil,
		{
			ID:         uuid.New(),
			Status:     matchingVO.MatchGroupStatusProposed,
			Confidence: confidence,
		},
	}
	findings := collectFeeFindings(
		context.Background(), nil, groups, nil,
		&feeVerificationInput{}, &fee.Rate{}, fee.Tolerance{},
	)
	assert.Empty(t, findings.variances)
	assert.Empty(t, findings.exceptionInputs)
}

func TestParseAmount_AllSupportedTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   any
		wantErr bool
	}{
		{"string valid", "123.45", false},
		{"string invalid", "xyz", true},
		{"float64", float64(42.5), false},
		{"int", int(100), false},
		{"int64", int64(9999), false},
		{"bool unsupported", true, true},
		{"nil unsupported", nil, true},
		{"slice unsupported", []int{1}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, feeErr := parseAmount(tt.input)
			if tt.wantErr {
				require.NotNil(t, feeErr)
			} else {
				require.Nil(t, feeErr)
			}
		})
	}
}
