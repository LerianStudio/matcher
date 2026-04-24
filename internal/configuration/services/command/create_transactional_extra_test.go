// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedfee "github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

func TestCreateContext_TransactionalInlineCreate_AllSourcesWithMappings(t *testing.T) {
	t.Parallel()

	provider, mock, db := setupInfraProviderWithSQLMock(t)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	sourceCreateWithTxCalls := 0
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

	useCase, err := NewUseCase(
		ctxRepo,
		srcRepo,
		fmRepo,
		&matchRuleRepoTxStub{matchRuleRepoStub: &matchRuleRepoStub{}},
		WithInfrastructureProvider(provider),
	)
	require.NoError(t, err)

	_, err = useCase.CreateContext(context.Background(), uuid.New(), entities.CreateReconciliationContextInput{
		Name:     "All Mapped Context",
		Type:     shared.ContextTypeOneToOne,
		Interval: "0 0 * * *",
		Sources: []entities.CreateContextSourceInput{
			{Name: "Bank A", Type: value_objects.SourceTypeBank, Side: sharedfee.MatchingSideLeft, Mapping: map[string]any{"amount": "col_amount"}},
			{Name: "Bank B", Type: value_objects.SourceTypeBank, Side: sharedfee.MatchingSideRight, Mapping: map[string]any{"date": "col_date"}},
			{Name: "Bank C", Type: value_objects.SourceTypeBank, Side: sharedfee.MatchingSideRight, Mapping: map[string]any{"ref": "col_ref"}},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 3, sourceCreateWithTxCalls)
	assert.Equal(t, 3, fieldMapCreateWithTxCalls)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateContext_TransactionalInlineCreate_BeginTxError(t *testing.T) {
	t.Parallel()

	provider, mock, db := setupInfraProviderWithSQLMock(t)
	defer db.Close()

	beginErr := errors.New("database unavailable")
	mock.ExpectBegin().WillReturnError(beginErr)

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

	useCase, err := NewUseCase(
		ctxRepo,
		&sourceRepoTxStub{sourceRepoStub: &sourceRepoStub{}},
		&fieldMapRepoTxStub{fieldMapRepoStub: &fieldMapRepoStub{}},
		&matchRuleRepoTxStub{matchRuleRepoStub: &matchRuleRepoStub{}},
		WithInfrastructureProvider(provider),
	)
	require.NoError(t, err)

	_, err = useCase.CreateContext(context.Background(), uuid.New(), entities.CreateReconciliationContextInput{
		Name:     "Begin Tx Error Context",
		Type:     shared.ContextTypeOneToOne,
		Interval: "0 0 * * *",
		Sources: []entities.CreateContextSourceInput{{
			Name: "Bank",
			Type: value_objects.SourceTypeBank,
			Side: sharedfee.MatchingSideLeft,
		}},
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "begin create transaction")
	require.ErrorIs(t, err, beginErr)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateContext_TransactionalInlineCreate_NilMappingSkipsFieldMap(t *testing.T) {
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
		Name:     "Nil Mapping Context",
		Type:     shared.ContextTypeOneToOne,
		Interval: "0 0 * * *",
		Sources: []entities.CreateContextSourceInput{
			{
				Name:    "Bank",
				Type:    value_objects.SourceTypeBank,
				Side:    sharedfee.MatchingSideLeft,
				Mapping: nil, // explicitly nil — distinct from map[string]any{}
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 0, fieldMapCreateWithTxCalls, "field map creator must NOT be called when Mapping is nil")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateContext_TransactionalInlineCreate_SecondSourceCreateError(t *testing.T) {
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

	sourceCallCount := 0
	srcRepo := &sourceRepoTxStub{
		sourceRepoStub: &sourceRepoStub{},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
			sourceCallCount++
			if sourceCallCount == 2 {
				return nil, errCreateFailed
			}

			return entity, nil
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
		Name:     "Two Sources Second Fails",
		Type:     shared.ContextTypeOneToOne,
		Interval: "0 0 * * *",
		Sources: []entities.CreateContextSourceInput{
			{Name: "Bank A", Type: value_objects.SourceTypeBank, Side: sharedfee.MatchingSideLeft},
			{Name: "Bank B", Type: value_objects.SourceTypeBank, Side: sharedfee.MatchingSideRight},
		},
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "creating source")
	require.ErrorIs(t, err, errCreateFailed)
	assert.Equal(t, 2, sourceCallCount, "source creator must be called twice: once for success, once for failure")
	require.NoError(t, mock.ExpectationsWereMet())
}
