// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"database/sql"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedfee "github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

func TestCreateContext_TransactionalInlineCreate_Success(t *testing.T) {
	t.Parallel()

	provider, mock, db := setupInfraProviderWithSQLMock(t)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	contextCreateWithTxCalls := 0
	sourceCreateWithTxCalls := 0
	fieldMapCreateWithTxCalls := 0
	ruleCreateWithTxCalls := 0

	ctxRepo := &contextRepoTxStub{
		contextRepoStub: &contextRepoStub{
			findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
				return nil, nil
			},
		},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			contextCreateWithTxCalls++
			return entity, nil
		},
	}

	srcRepo := &sourceRepoTxStub{
		sourceRepoStub: &sourceRepoStub{},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
			sourceCreateWithTxCalls++
			return entity, nil
		},
	}

	fmRepo := &fieldMapRepoTxStub{
		fieldMapRepoStub: &fieldMapRepoStub{},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *shared.FieldMap) (*shared.FieldMap, error) {
			fieldMapCreateWithTxCalls++
			return entity, nil
		},
	}

	ruleRepo := &matchRuleRepoTxStub{
		matchRuleRepoStub: &matchRuleRepoStub{},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.MatchRule) (*entities.MatchRule, error) {
			ruleCreateWithTxCalls++
			return entity, nil
		},
	}

	useCase, err := NewUseCase(
		ctxRepo,
		srcRepo,
		fmRepo,
		ruleRepo,
		WithInfrastructureProvider(provider),
	)
	require.NoError(t, err)

	created, err := useCase.CreateContext(context.Background(), uuid.New(), entities.CreateReconciliationContextInput{
		Name:     "Transactional Context",
		Type:     shared.ContextTypeOneToOne,
		Interval: "0 0 * * *",
		Sources: []entities.CreateContextSourceInput{
			{
				Name:    "Bank",
				Type:    value_objects.SourceTypeBank,
				Side:    sharedfee.MatchingSideLeft,
				Mapping: map[string]any{"amount": "col_amount"},
			},
		},
		Rules: []entities.CreateMatchRuleInput{
			{
				Priority: 1,
				Type:     shared.RuleTypeExact,
				Config:   map[string]any{"matchAmount": true},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created)
	assert.Equal(t, "Transactional Context", created.Name)
	assert.Equal(t, shared.ContextTypeOneToOne, created.Type)
	assert.Equal(t, value_objects.ContextStatusDraft, created.Status)
	assert.Equal(t, 1, contextCreateWithTxCalls)
	assert.Equal(t, 1, sourceCreateWithTxCalls)
	assert.Equal(t, 1, fieldMapCreateWithTxCalls)
	assert.Equal(t, 1, ruleCreateWithTxCalls)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateContext_TransactionalInlineCreate_EmptyMappingSkipsFieldMap(t *testing.T) {
	t.Parallel()

	provider, mock, db := setupInfraProviderWithSQLMock(t)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	fieldMapCreateWithTxCalls := 0

	ctxRepo := &contextRepoTxStub{
		contextRepoStub: &contextRepoStub{
			findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
				return nil, nil
			},
		},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}

	srcRepo := &sourceRepoTxStub{
		sourceRepoStub: &sourceRepoStub{},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
			return entity, nil
		},
	}

	fmRepo := &fieldMapRepoTxStub{
		fieldMapRepoStub: &fieldMapRepoStub{},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *shared.FieldMap) (*shared.FieldMap, error) {
			fieldMapCreateWithTxCalls++
			return entity, nil
		},
	}

	ruleRepo := &matchRuleRepoTxStub{
		matchRuleRepoStub: &matchRuleRepoStub{},
	}

	useCase, err := NewUseCase(
		ctxRepo,
		srcRepo,
		fmRepo,
		ruleRepo,
		WithInfrastructureProvider(provider),
	)
	require.NoError(t, err)

	_, err = useCase.CreateContext(context.Background(), uuid.New(), entities.CreateReconciliationContextInput{
		Name:     "Empty Mapping Context",
		Type:     shared.ContextTypeOneToOne,
		Interval: "0 0 * * *",
		Sources: []entities.CreateContextSourceInput{
			{
				Name:    "Bank",
				Type:    value_objects.SourceTypeBank,
				Side:    sharedfee.MatchingSideLeft,
				Mapping: map[string]any{},
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 0, fieldMapCreateWithTxCalls)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateContext_TransactionalInlineCreate_DuplicateRulePriority(t *testing.T) {
	t.Parallel()

	provider, _, db := setupInfraProviderWithSQLMock(t)
	defer db.Close()

	contextCreateWithTxCalls := 0

	ctxRepo := &contextRepoTxStub{
		contextRepoStub: &contextRepoStub{
			findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
				return nil, nil
			},
		},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			contextCreateWithTxCalls++
			return entity, nil
		},
	}

	useCase, err := NewUseCase(
		ctxRepo,
		&sourceRepoTxStub{sourceRepoStub: &sourceRepoStub{}},
		&fieldMapRepoTxStub{fieldMapRepoStub: &fieldMapRepoStub{}},
		&matchRuleRepoTxStub{matchRuleRepoStub: &matchRuleRepoStub{}},
		WithInfrastructureProvider(provider),
	)
	require.NoError(t, err)

	_, err = useCase.CreateContext(context.Background(), uuid.New(), entities.CreateReconciliationContextInput{
		Name:     "Duplicate Priorities",
		Type:     shared.ContextTypeOneToOne,
		Interval: "0 0 * * *",
		Rules: []entities.CreateMatchRuleInput{
			{Priority: 1, Type: shared.RuleTypeExact, Config: map[string]any{"matchAmount": true}},
			{Priority: 1, Type: shared.RuleTypeTolerance, Config: map[string]any{"absTolerance": 0.01}},
		},
	})
	require.ErrorIs(t, err, entities.ErrRulePriorityConflict)
	assert.Equal(t, 0, contextCreateWithTxCalls)
}

func TestCreateContext_TransactionalInlineCreate_SourceCreateError(t *testing.T) {
	t.Parallel()

	provider, mock, db := setupInfraProviderWithSQLMock(t)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectRollback()

	ctxRepo := &contextRepoTxStub{
		contextRepoStub: &contextRepoStub{
			findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
				return nil, nil
			},
		},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}

	srcRepo := &sourceRepoTxStub{
		sourceRepoStub: &sourceRepoStub{},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, _ *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
			return nil, errCreateFailed
		},
	}

	useCase, err := NewUseCase(
		ctxRepo,
		srcRepo,
		&fieldMapRepoTxStub{fieldMapRepoStub: &fieldMapRepoStub{}},
		&matchRuleRepoTxStub{matchRuleRepoStub: &matchRuleRepoStub{}},
		WithInfrastructureProvider(provider),
	)
	require.NoError(t, err)

	_, err = useCase.CreateContext(context.Background(), uuid.New(), entities.CreateReconciliationContextInput{
		Name:     "Source Create Failure",
		Type:     shared.ContextTypeOneToOne,
		Interval: "0 0 * * *",
		Sources: []entities.CreateContextSourceInput{{
			Name: "Bank",
			Type: value_objects.SourceTypeBank,
			Side: sharedfee.MatchingSideLeft,
		}},
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "creating source")
	require.ErrorIs(t, err, errCreateFailed)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateContext_TransactionalInlineCreate_FieldMapCreateError(t *testing.T) {
	t.Parallel()

	provider, mock, db := setupInfraProviderWithSQLMock(t)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectRollback()

	ctxRepo := &contextRepoTxStub{
		contextRepoStub: &contextRepoStub{
			findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
				return nil, nil
			},
		},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}

	srcRepo := &sourceRepoTxStub{
		sourceRepoStub: &sourceRepoStub{},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
			return entity, nil
		},
	}

	fmRepo := &fieldMapRepoTxStub{
		fieldMapRepoStub: &fieldMapRepoStub{},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, _ *shared.FieldMap) (*shared.FieldMap, error) {
			return nil, errCreateFailed
		},
	}

	useCase, err := NewUseCase(
		ctxRepo,
		srcRepo,
		fmRepo,
		&matchRuleRepoTxStub{matchRuleRepoStub: &matchRuleRepoStub{}},
		WithInfrastructureProvider(provider),
	)
	require.NoError(t, err)

	_, err = useCase.CreateContext(context.Background(), uuid.New(), entities.CreateReconciliationContextInput{
		Name:     "Field Map Create Failure",
		Type:     shared.ContextTypeOneToOne,
		Interval: "0 0 * * *",
		Sources: []entities.CreateContextSourceInput{{
			Name:    "Bank",
			Type:    value_objects.SourceTypeBank,
			Side:    sharedfee.MatchingSideLeft,
			Mapping: map[string]any{"amount": "col_amount"},
		}},
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "creating field map")
	require.ErrorIs(t, err, errCreateFailed)
	require.NoError(t, mock.ExpectationsWereMet())
}
