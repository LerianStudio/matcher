//go:build unit

package command

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

func ptrString(s string) *string { return &s }

// ===========================================================================
// buildClonedContextEntity
// ===========================================================================

func TestBuildClonedContextEntity(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()

	sourceContext := &entities.ReconciliationContext{
		ID:                uuid.New(),
		TenantID:          tenantID,
		Name:              "Original",
		Type:              shared.ContextTypeOneToOne,
		Interval:          "0 0 * * *",
		Status:            value_objects.ContextStatusActive,
		FeeToleranceAbs:   decimal.NewFromFloat(0.01),
		FeeTolerancePct:   decimal.NewFromFloat(0.5),
		FeeNormalization:  ptrString("NET"),
		AutoMatchOnUpload: true,
		CreatedAt:         time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt:         time.Date(2020, 6, 1, 0, 0, 0, 0, time.UTC),
	}

	tests := []struct {
		name              string
		newName           string
		autoMatchOnUpload bool
		expectedName      string
		expectAutoMatch   bool
		expectedErr       error
	}{
		{
			name:              "copies all fields with new name",
			newName:           "Cloned Context",
			autoMatchOnUpload: true,
			expectedName:      "Cloned Context",
			expectAutoMatch:   true,
		},
		{
			name:              "trims whitespace from name",
			newName:           "  Padded Name  ",
			autoMatchOnUpload: false,
			expectedName:      "Padded Name",
			expectAutoMatch:   false,
		},
		{
			name:              "overrides autoMatch",
			newName:           "Override AutoMatch",
			autoMatchOnUpload: false,
			expectedName:      "Override AutoMatch",
			expectAutoMatch:   false,
		},
		{
			name:        "rejects names that violate context invariants",
			newName:     strings.Repeat("x", 101),
			expectedErr: entities.ErrContextNameTooLong,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			uc := &UseCase{}

			input := CloneContextInput{
				SourceContextID: sourceContext.ID,
				NewName:         tt.newName,
			}

			result, err := uc.buildClonedContextEntity(context.Background(), input, sourceContext, tt.autoMatchOnUpload)
			if tt.expectedErr != nil {
				require.ErrorIs(t, err, tt.expectedErr)
				assert.Nil(t, result)
				return
			}

			require.NoError(t, err)

			assert.NotEqual(t, sourceContext.ID, result.ID)
			assert.NotEqual(t, uuid.Nil, result.ID)
			assert.Equal(t, tenantID, result.TenantID)
			assert.Equal(t, tt.expectedName, result.Name)
			assert.Equal(t, sourceContext.Type, result.Type)
			assert.Equal(t, sourceContext.Interval, result.Interval)
			assert.Equal(t, value_objects.ContextStatusActive, result.Status)
			assert.True(t, sourceContext.FeeToleranceAbs.Equal(result.FeeToleranceAbs))
			assert.True(t, sourceContext.FeeTolerancePct.Equal(result.FeeTolerancePct))
			assert.Equal(t, sourceContext.FeeNormalization, result.FeeNormalization)
			assert.Equal(t, tt.expectAutoMatch, result.AutoMatchOnUpload)
			assert.False(t, result.CreatedAt.IsZero())
			assert.False(t, result.UpdatedAt.IsZero())
			assert.False(t, result.CreatedAt.Before(sourceContext.CreatedAt))
		})
	}
}

// ===========================================================================
// createClonedContext (non-transactional)
// ===========================================================================

func TestCreateClonedContext_Success(t *testing.T) {
	t.Parallel()

	sourceContext := &entities.ReconciliationContext{
		ID:       uuid.New(),
		TenantID: uuid.New(),
		Name:     "Original",
		Type:     shared.ContextTypeOneToOne,
		Interval: "daily",
	}

	createCalled := false
	repo := &contextRepoStub{
		createFn: func(_ context.Context, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			createCalled = true
			assert.Equal(t, "Clone Name", entity.Name)
			return entity, nil
		},
	}

	uc := &UseCase{contextRepo: repo}

	result, err := uc.createClonedContext(
		context.Background(),
		CloneContextInput{NewName: "Clone Name"},
		sourceContext,
		false,
	)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, createCalled)
	assert.Equal(t, "Clone Name", result.Name)
}

func TestCreateClonedContext_RepoError(t *testing.T) {
	t.Parallel()

	errDB := errors.New("DB write failed")

	repo := &contextRepoStub{
		createFn: func(_ context.Context, _ *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return nil, errDB
		},
	}

	uc := &UseCase{contextRepo: repo}

	_, err := uc.createClonedContext(
		context.Background(),
		CloneContextInput{NewName: "Clone"},
		&entities.ReconciliationContext{ID: uuid.New(), TenantID: uuid.New(), Type: shared.ContextTypeOneToOne, Interval: "daily"},
		false,
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, errDB)
	assert.Contains(t, err.Error(), "creating cloned context")
}

// ===========================================================================
// createClonedContextWithTx (transactional)
// ===========================================================================

func TestCreateClonedContextWithTx_Success(t *testing.T) {
	t.Parallel()

	sourceContext := &entities.ReconciliationContext{
		ID:       uuid.New(),
		TenantID: uuid.New(),
		Name:     "Source",
		Type:     shared.ContextTypeOneToOne,
		Interval: "hourly",
	}

	txCreateCalled := false
	txRepo := &contextRepoTxStub{
		contextRepoStub: &contextRepoStub{},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			txCreateCalled = true
			assert.Equal(t, "Tx Clone", entity.Name)
			return entity, nil
		},
	}

	uc := &UseCase{contextRepo: txRepo}
	fakeTx := &sql.Tx{}

	result, err := uc.createClonedContextWithTx(
		context.Background(),
		fakeTx,
		CloneContextInput{NewName: "Tx Clone"},
		sourceContext,
		true,
	)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, txCreateCalled)
}

func TestCreateClonedContextWithTx_RepoDoesNotSupportTx(t *testing.T) {
	t.Parallel()

	// contextRepoStub does NOT implement contextTxCreator.
	repo := &contextRepoStub{}

	uc := &UseCase{contextRepo: repo}
	fakeTx := &sql.Tx{}

	_, err := uc.createClonedContextWithTx(
		context.Background(),
		fakeTx,
		CloneContextInput{NewName: "No Tx Support"},
		&entities.ReconciliationContext{ID: uuid.New(), TenantID: uuid.New(), Type: shared.ContextTypeOneToOne, Interval: "daily"},
		false,
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrCloneProviderRequired)
}

func TestCreateClonedContextWithTx_CreateError(t *testing.T) {
	t.Parallel()

	errTxCreate := errors.New("tx create failed")

	txRepo := &contextRepoTxStub{
		contextRepoStub: &contextRepoStub{},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, _ *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return nil, errTxCreate
		},
	}

	uc := &UseCase{contextRepo: txRepo}
	fakeTx := &sql.Tx{}

	_, err := uc.createClonedContextWithTx(
		context.Background(),
		fakeTx,
		CloneContextInput{NewName: "Fail"},
		&entities.ReconciliationContext{ID: uuid.New(), TenantID: uuid.New(), Type: shared.ContextTypeOneToOne, Interval: "daily"},
		false,
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, errTxCreate)
	assert.Contains(t, err.Error(), "creating cloned context")
}

// ===========================================================================
// cloneContextNonTransactional
// ===========================================================================

func TestCloneContextNonTransactional_ContextOnly(t *testing.T) {
	t.Parallel()

	sourceCtxID := uuid.New()
	sourceContext := &entities.ReconciliationContext{
		ID:       sourceCtxID,
		TenantID: uuid.New(),
		Name:     "Source",
		Type:     shared.ContextTypeOneToOne,
		Interval: "daily",
	}

	repo := &contextRepoStub{
		createFn: func(_ context.Context, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}

	uc := &UseCase{
		contextRepo:   repo,
		sourceRepo:    &sourceRepoStub{},
		fieldMapRepo:  &fieldMapRepoStub{},
		matchRuleRepo: &matchRuleRepoStub{},
	}

	result, err := uc.cloneContextNonTransactional(
		context.Background(),
		CloneContextInput{
			SourceContextID: sourceCtxID,
			NewName:         "Clone",
			IncludeSources:  false,
			IncludeRules:    false,
		},
		sourceContext,
		false,
	)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "Clone", result.Context.Name)
	assert.Equal(t, 0, result.SourcesCloned)
	assert.Equal(t, 0, result.RulesCloned)
	assert.Equal(t, 0, result.FeeRulesCloned)
	assert.Equal(t, 0, result.FieldMapsCloned)
}

func TestCloneContextNonTransactional_WithSources(t *testing.T) {
	t.Parallel()

	sourceCtxID := uuid.New()
	sourceID := uuid.New()

	sourceContext := &entities.ReconciliationContext{
		ID:       sourceCtxID,
		TenantID: uuid.New(),
		Name:     "Source",
		Type:     shared.ContextTypeOneToOne,
		Interval: "daily",
	}

	ctxRepo := &contextRepoStub{
		createFn: func(_ context.Context, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}

	srcRepo := &sourceRepoStub{
		findByContextIDFn: func(_ context.Context, ctxID uuid.UUID) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
			if ctxID == sourceCtxID {
				return []*entities.ReconciliationSource{
					{ID: sourceID, ContextID: sourceCtxID, Name: "Bank A"},
				}, libHTTP.CursorPagination{}, nil
			}

			return nil, libHTTP.CursorPagination{}, nil
		},
		createFn: func(_ context.Context, entity *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
			return entity, nil
		},
	}

	fmRepo := &fieldMapRepoStub{
		findBySourceIDFn: func(_ context.Context, _ uuid.UUID) (*entities.FieldMap, error) {
			return nil, sql.ErrNoRows
		},
	}

	uc := &UseCase{
		contextRepo:   ctxRepo,
		sourceRepo:    srcRepo,
		fieldMapRepo:  fmRepo,
		matchRuleRepo: &matchRuleRepoStub{},
	}

	result, err := uc.cloneContextNonTransactional(
		context.Background(),
		CloneContextInput{
			SourceContextID: sourceCtxID,
			NewName:         "Clone With Sources",
			IncludeSources:  true,
			IncludeRules:    false,
		},
		sourceContext,
		false,
	)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 1, result.SourcesCloned)
}

func TestCloneContextNonTransactional_WithRules(t *testing.T) {
	t.Parallel()

	sourceCtxID := uuid.New()

	sourceContext := &entities.ReconciliationContext{
		ID:       sourceCtxID,
		TenantID: uuid.New(),
		Name:     "Source",
		Type:     shared.ContextTypeOneToOne,
		Interval: "daily",
	}

	ctxRepo := &contextRepoStub{
		createFn: func(_ context.Context, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}

	ruleRepo := &matchRuleRepoStub{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID, _ string, _ int) (entities.MatchRules, libHTTP.CursorPagination, error) {
			return entities.MatchRules{
				{ID: uuid.New(), ContextID: sourceCtxID, Priority: 1, Type: shared.RuleTypeExact, Config: map[string]any{}},
			}, libHTTP.CursorPagination{}, nil
		},
		createFn: func(_ context.Context, rule *entities.MatchRule) (*entities.MatchRule, error) {
			return rule, nil
		},
	}

	feeRepo := newFeeRuleMockRepo()

	uc := &UseCase{
		contextRepo:   ctxRepo,
		sourceRepo:    &sourceRepoStub{},
		fieldMapRepo:  &fieldMapRepoStub{},
		matchRuleRepo: ruleRepo,
		feeRuleRepo:   feeRepo,
	}

	result, err := uc.cloneContextNonTransactional(
		context.Background(),
		CloneContextInput{
			SourceContextID: sourceCtxID,
			NewName:         "Clone With Rules",
			IncludeSources:  false,
			IncludeRules:    true,
		},
		sourceContext,
		false,
	)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 1, result.RulesCloned)
	assert.Equal(t, 0, result.FeeRulesCloned)
}

func TestCloneContextNonTransactional_SourceCloneError(t *testing.T) {
	t.Parallel()

	sourceCtxID := uuid.New()
	errSrcFetch := errors.New("source fetch failed")

	sourceContext := &entities.ReconciliationContext{
		ID:       sourceCtxID,
		TenantID: uuid.New(),
		Name:     "Source",
		Type:     shared.ContextTypeOneToOne,
		Interval: "daily",
	}

	ctxRepo := &contextRepoStub{
		createFn: func(_ context.Context, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}

	srcRepo := &sourceRepoStub{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
			return nil, libHTTP.CursorPagination{}, errSrcFetch
		},
	}

	uc := &UseCase{
		contextRepo:   ctxRepo,
		sourceRepo:    srcRepo,
		fieldMapRepo:  &fieldMapRepoStub{},
		matchRuleRepo: &matchRuleRepoStub{},
	}

	_, err := uc.cloneContextNonTransactional(
		context.Background(),
		CloneContextInput{
			SourceContextID: sourceCtxID,
			NewName:         "Clone Fail Sources",
			IncludeSources:  true,
		},
		sourceContext,
		false,
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "cloning sources")
}

func TestCloneContextNonTransactional_RuleCloneError(t *testing.T) {
	t.Parallel()

	sourceCtxID := uuid.New()
	errRuleFetch := errors.New("rules fetch failed")

	sourceContext := &entities.ReconciliationContext{
		ID:       sourceCtxID,
		TenantID: uuid.New(),
		Name:     "Source",
		Type:     shared.ContextTypeOneToOne,
		Interval: "daily",
	}

	ctxRepo := &contextRepoStub{
		createFn: func(_ context.Context, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}

	ruleRepo := &matchRuleRepoStub{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID, _ string, _ int) (entities.MatchRules, libHTTP.CursorPagination, error) {
			return nil, libHTTP.CursorPagination{}, errRuleFetch
		},
	}

	uc := &UseCase{
		contextRepo:   ctxRepo,
		sourceRepo:    &sourceRepoStub{},
		fieldMapRepo:  &fieldMapRepoStub{},
		matchRuleRepo: ruleRepo,
		feeRuleRepo:   newFeeRuleMockRepo(),
	}

	_, err := uc.cloneContextNonTransactional(
		context.Background(),
		CloneContextInput{
			SourceContextID: sourceCtxID,
			NewName:         "Clone Fail Rules",
			IncludeRules:    true,
		},
		sourceContext,
		false,
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "cloning rules")
}

func TestCloneContextNonTransactional_FeeRuleCloneError(t *testing.T) {
	t.Parallel()

	sourceCtxID := uuid.New()

	sourceContext := &entities.ReconciliationContext{
		ID:       sourceCtxID,
		TenantID: uuid.New(),
		Name:     "Source",
		Type:     shared.ContextTypeOneToOne,
		Interval: "daily",
	}

	ctxRepo := &contextRepoStub{
		createFn: func(_ context.Context, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}

	ruleRepo := &matchRuleRepoStub{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID, _ string, _ int) (entities.MatchRules, libHTTP.CursorPagination, error) {
			return nil, libHTTP.CursorPagination{}, nil
		},
	}

	feeRepo := newFeeRuleMockRepo()
	feeRepo.findErr = errors.New("fee rules fetch exploded")

	uc := &UseCase{
		contextRepo:   ctxRepo,
		sourceRepo:    &sourceRepoStub{},
		fieldMapRepo:  &fieldMapRepoStub{},
		matchRuleRepo: ruleRepo,
		feeRuleRepo:   feeRepo,
	}

	_, err := uc.cloneContextNonTransactional(
		context.Background(),
		CloneContextInput{
			SourceContextID: sourceCtxID,
			NewName:         "Clone Fail Fee Rules",
			IncludeRules:    true,
		},
		sourceContext,
		false,
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "cloning fee rules")
}

func TestCloneContextNonTransactional_CreateContextError(t *testing.T) {
	t.Parallel()

	errCreate := errors.New("create context failed")

	repo := &contextRepoStub{
		createFn: func(_ context.Context, _ *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return nil, errCreate
		},
	}

	uc := &UseCase{contextRepo: repo}

	_, err := uc.cloneContextNonTransactional(
		context.Background(),
		CloneContextInput{NewName: "Fail"},
		&entities.ReconciliationContext{ID: uuid.New(), TenantID: uuid.New(), Type: shared.ContextTypeOneToOne, Interval: "daily"},
		false,
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, errCreate)
}

// ===========================================================================
// cloneContextTransactional
// ===========================================================================

func TestCloneContextTransactional_Success(t *testing.T) {
	t.Parallel()

	provider, mock, db := setupInfraProviderWithSQLMock(t)
	defer db.Close()

	sourceCtxID := uuid.New()
	sourceContext := &entities.ReconciliationContext{
		ID:       sourceCtxID,
		TenantID: uuid.New(),
		Name:     "Original",
		Type:     shared.ContextTypeOneToOne,
		Interval: "daily",
	}

	mock.ExpectBegin()
	// FOR SHARE lock
	mock.ExpectExec("SELECT 1 FROM reconciliation_contexts WHERE id = \\$1 FOR SHARE").
		WithArgs(sourceCtxID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	ctxRepo := &contextRepoTxStub{
		contextRepoStub: &contextRepoStub{},
		findByIDWithTxFn: func(_ context.Context, _ *sql.Tx, _ uuid.UUID) (*entities.ReconciliationContext, error) {
			return &entities.ReconciliationContext{
				ID:       sourceCtxID,
				TenantID: sourceContext.TenantID,
				Name:     "Locked Source",
				Type:     sourceContext.Type,
				Interval: "hourly",
				Status:   value_objects.ContextStatusPaused,
			}, nil
		},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			assert.Equal(t, "hourly", entity.Interval)
			assert.Equal(t, value_objects.ContextStatusActive, entity.Status)
			return entity, nil
		},
	}

	uc := &UseCase{
		contextRepo:   ctxRepo,
		sourceRepo:    &sourceRepoStub{},
		fieldMapRepo:  &fieldMapRepoStub{},
		matchRuleRepo: &matchRuleRepoStub{},
		infraProvider: provider,
	}

	result, err := uc.cloneContextTransactional(
		context.Background(),
		CloneContextInput{
			SourceContextID: sourceCtxID,
			NewName:         "Tx Clone",
			IncludeSources:  false,
			IncludeRules:    false,
		},
		sourceContext,
	)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "Tx Clone", result.Context.Name)
	assert.Equal(t, "hourly", result.Context.Interval)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCloneContextTransactional_BeginTxError(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		contextRepo:   &contextRepoStub{},
		infraProvider: nil, // nil provider → beginTenantTx fails
	}

	_, err := uc.cloneContextTransactional(
		context.Background(),
		CloneContextInput{NewName: "Fail"},
		&entities.ReconciliationContext{ID: uuid.New(), TenantID: uuid.New()},
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "begin clone transaction")
}

func TestCloneContextTransactional_LockError(t *testing.T) {
	t.Parallel()

	provider, mock, db := setupInfraProviderWithSQLMock(t)
	defer db.Close()

	sourceCtxID := uuid.New()
	errLock := errors.New("lock timeout")

	mock.ExpectBegin()
	mock.ExpectExec("SELECT 1 FROM reconciliation_contexts WHERE id = \\$1 FOR SHARE").
		WithArgs(sourceCtxID).
		WillReturnError(errLock)
	mock.ExpectRollback()

	uc := &UseCase{
		contextRepo:   &contextRepoStub{},
		infraProvider: provider,
	}

	_, err := uc.cloneContextTransactional(
		context.Background(),
		CloneContextInput{
			SourceContextID: sourceCtxID,
			NewName:         "Lock Fail",
		},
		&entities.ReconciliationContext{ID: sourceCtxID, TenantID: uuid.New()},
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "lock source context for clone")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCloneContextTransactional_CommitError(t *testing.T) {
	t.Parallel()

	provider, mock, db := setupInfraProviderWithSQLMock(t)
	defer db.Close()

	sourceCtxID := uuid.New()
	errCommit := errors.New("commit failed")

	mock.ExpectBegin()
	mock.ExpectExec("SELECT 1 FROM reconciliation_contexts WHERE id = \\$1 FOR SHARE").
		WithArgs(sourceCtxID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit().WillReturnError(errCommit)

	ctxRepo := &contextRepoTxStub{
		contextRepoStub: &contextRepoStub{},
		findByIDWithTxFn: func(_ context.Context, _ *sql.Tx, _ uuid.UUID) (*entities.ReconciliationContext, error) {
			return &entities.ReconciliationContext{ID: sourceCtxID, TenantID: uuid.New(), Type: shared.ContextTypeOneToOne, Interval: "daily"}, nil
		},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}

	uc := &UseCase{
		contextRepo:   ctxRepo,
		sourceRepo:    &sourceRepoStub{},
		fieldMapRepo:  &fieldMapRepoStub{},
		matchRuleRepo: &matchRuleRepoStub{},
		infraProvider: provider,
	}

	_, err := uc.cloneContextTransactional(
		context.Background(),
		CloneContextInput{
			SourceContextID: sourceCtxID,
			NewName:         "Commit Fail",
		},
		&entities.ReconciliationContext{ID: sourceCtxID, TenantID: uuid.New()},
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "commit clone transaction")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCloneContextTransactional_SourceCloneError(t *testing.T) {
	t.Parallel()

	provider, mock, db := setupInfraProviderWithSQLMock(t)
	defer db.Close()

	sourceCtxID := uuid.New()
	errSrcFetch := errors.New("source fetch failed tx")

	mock.ExpectBegin()
	mock.ExpectExec("SELECT 1 FROM reconciliation_contexts WHERE id = \\$1 FOR SHARE").
		WithArgs(sourceCtxID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectRollback()

	ctxRepo := &contextRepoTxStub{
		contextRepoStub: &contextRepoStub{},
		findByIDWithTxFn: func(_ context.Context, _ *sql.Tx, _ uuid.UUID) (*entities.ReconciliationContext, error) {
			return &entities.ReconciliationContext{ID: sourceCtxID, TenantID: uuid.New(), Type: shared.ContextTypeOneToOne, Interval: "daily"}, nil
		},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}

	// sourceRepoStub will fail on FindByContextID (non-tx fallback since it doesn't implement sourceTxFinder).
	srcRepo := &sourceRepoStub{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
			return nil, libHTTP.CursorPagination{}, errSrcFetch
		},
	}

	uc := &UseCase{
		contextRepo:   ctxRepo,
		sourceRepo:    srcRepo,
		fieldMapRepo:  &fieldMapRepoStub{},
		matchRuleRepo: &matchRuleRepoStub{},
		infraProvider: provider,
	}

	_, err := uc.cloneContextTransactional(
		context.Background(),
		CloneContextInput{
			SourceContextID: sourceCtxID,
			NewName:         "Src Fail Tx",
			IncludeSources:  true,
		},
		&entities.ReconciliationContext{ID: sourceCtxID, TenantID: uuid.New()},
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "cloning sources")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCloneContextTransactional_WithAllIncludes(t *testing.T) {
	t.Parallel()

	provider, mock, db := setupInfraProviderWithSQLMock(t)
	defer db.Close()

	sourceCtxID := uuid.New()
	sourceID := uuid.New()
	tenantID := uuid.New()

	sourceContext := &entities.ReconciliationContext{
		ID:       sourceCtxID,
		TenantID: tenantID,
		Name:     "Full Clone Source",
		Type:     shared.ContextTypeOneToOne,
		Interval: "daily",
	}

	mock.ExpectBegin()
	mock.ExpectExec("SELECT 1 FROM reconciliation_contexts WHERE id = \\$1 FOR SHARE").
		WithArgs(sourceCtxID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	ctxRepo := &contextRepoTxStub{
		contextRepoStub: &contextRepoStub{},
		findByIDWithTxFn: func(_ context.Context, _ *sql.Tx, _ uuid.UUID) (*entities.ReconciliationContext, error) {
			return sourceContext, nil
		},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}

	srcRepo := &sourceRepoTxStub{
		sourceRepoStub: &sourceRepoStub{
			findByContextIDFn: func(_ context.Context, ctxID uuid.UUID) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
				return []*entities.ReconciliationSource{
					{ID: sourceID, ContextID: sourceCtxID, Name: "Bank", Config: map[string]any{"key": "val"}},
				}, libHTTP.CursorPagination{}, nil
			},
		},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
			return entity, nil
		},
	}

	fmRepo := &fieldMapRepoTxStub{
		fieldMapRepoStub: &fieldMapRepoStub{},
	}

	ruleRepo := &matchRuleRepoTxStub{
		matchRuleRepoStub: &matchRuleRepoStub{
			findByContextIDFn: func(_ context.Context, _ uuid.UUID, _ string, _ int) (entities.MatchRules, libHTTP.CursorPagination, error) {
				return entities.MatchRules{
					{ID: uuid.New(), ContextID: sourceCtxID, Priority: 1, Type: shared.RuleTypeExact, Config: map[string]any{}},
				}, libHTTP.CursorPagination{}, nil
			},
		},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, rule *entities.MatchRule) (*entities.MatchRule, error) {
			return rule, nil
		},
	}

	feeRepo := newFeeRuleMockRepo()

	uc := &UseCase{
		contextRepo:   ctxRepo,
		sourceRepo:    srcRepo,
		fieldMapRepo:  fmRepo,
		matchRuleRepo: ruleRepo,
		feeRuleRepo:   feeRepo,
		infraProvider: provider,
	}

	result, err := uc.cloneContextTransactional(
		context.Background(),
		CloneContextInput{
			SourceContextID: sourceCtxID,
			NewName:         "Full Clone",
			IncludeSources:  true,
			IncludeRules:    true,
		},
		sourceContext,
	)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "Full Clone", result.Context.Name)
	assert.Equal(t, 1, result.SourcesCloned)
	assert.Equal(t, 1, result.RulesCloned)
	assert.Equal(t, 0, result.FeeRulesCloned)
	require.NoError(t, mock.ExpectationsWereMet())
}

// Verify that the provider helper function is available and works.
func TestSetupInfraProviderWithSQLMock_Utility(t *testing.T) {
	t.Parallel()

	provider, mock, db := setupInfraProviderWithSQLMock(t)
	defer db.Close()

	require.NotNil(t, provider)
	require.NotNil(t, mock)
	require.NotNil(t, db)

	// Verify we can use mock expectations
	mock.ExpectPing()
	require.NoError(t, db.Ping())
	require.NoError(t, mock.ExpectationsWereMet())
}

// Verify the MockInfrastructureProvider type is accessible and produces
// a predictable error when no database is configured.
func TestMockInfrastructureProvider_BeginTxReturnsError(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}

	txLease, err := provider.BeginTx(context.Background())

	// Empty MockInfrastructureProvider has no DB → returns an error.
	assert.Nil(t, txLease)
	assert.Error(t, err)
}
