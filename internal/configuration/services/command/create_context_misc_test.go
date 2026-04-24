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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	repoMocks "github.com/LerianStudio/matcher/internal/configuration/domain/repositories/mocks"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	configPorts "github.com/LerianStudio/matcher/internal/configuration/ports"
	portMocks "github.com/LerianStudio/matcher/internal/configuration/ports/mocks"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedfee "github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

func TestCreateContext_DatabaseUniqueViolationMapsToContextNameAlreadyExists(t *testing.T) {
	t.Parallel()

	repo := &contextRepoStub{
		findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
			return nil, nil
		},
		createFn: func(_ context.Context, _ *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return nil, &pgconn.PgError{Code: postgresUniqueViolationCode, ConstraintName: "uq_context_tenant_name"}
		},
	}

	useCase, err := NewUseCase(repo, &sourceRepoStub{}, &fieldMapRepoStub{}, &matchRuleRepoStub{})
	require.NoError(t, err)

	_, err = useCase.CreateContext(context.Background(), uuid.New(), entities.CreateReconciliationContextInput{
		Name:     "Race Context",
		Type:     shared.ContextTypeOneToOne,
		Interval: "0 0 * * *",
	})
	require.ErrorIs(t, err, ErrContextNameAlreadyExists)
}

func TestCreateContext_WithAuditPublisher_IncludesInlineCounts(t *testing.T) {
	t.Parallel()

	provider, mock, db := setupInfraProviderWithSQLMock(t)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	ctrl := gomock.NewController(t)
	mockAuditPub := portMocks.NewMockAuditPublisher(ctrl)

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

	fmRepo := &fieldMapRepoTxStub{fieldMapRepoStub: &fieldMapRepoStub{}}
	ruleRepo := &matchRuleRepoTxStub{
		matchRuleRepoStub: &matchRuleRepoStub{},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.MatchRule) (*entities.MatchRule, error) {
			return entity, nil
		},
	}

	mockAuditPub.EXPECT().
		Publish(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, event configPorts.AuditEvent) error {
			require.Equal(t, "context", event.EntityType)
			require.Equal(t, "create", event.Action)
			assert.Equal(t, 1, event.Changes["sources_count"])
			assert.Equal(t, 1, event.Changes["rules_count"])
			return nil
		})

	useCase, err := NewUseCase(
		ctxRepo,
		srcRepo,
		fmRepo,
		ruleRepo,
		WithInfrastructureProvider(provider),
		WithAuditPublisher(mockAuditPub),
	)
	require.NoError(t, err)

	_, err = useCase.CreateContext(context.Background(), uuid.New(), entities.CreateReconciliationContextInput{
		Name:     "Audited Context",
		Type:     shared.ContextTypeOneToOne,
		Interval: "0 0 * * *",
		Sources:  []entities.CreateContextSourceInput{{Name: "Bank", Type: value_objects.SourceTypeBank, Side: sharedfee.MatchingSideLeft}},
		Rules: []entities.CreateMatchRuleInput{{
			Priority: 1,
			Type:     shared.RuleTypeExact,
			Config:   map[string]any{"matchAmount": true},
		}},
	})
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateContext_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	_, err := uc.CreateContext(
		context.Background(),
		uuid.New(),
		entities.CreateReconciliationContextInput{},
	)
	require.ErrorIs(t, err, ErrNilContextRepository)
}

func TestCreateContext_NilContextRepo(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		contextRepo: nil,
	}

	_, err := uc.CreateContext(
		context.Background(),
		uuid.New(),
		entities.CreateReconciliationContextInput{},
	)
	require.ErrorIs(t, err, ErrNilContextRepository)
}

func TestCreateContext_WithAuditPublisher_SimpleCreate(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)
	mockAuditPub := portMocks.NewMockAuditPublisher(ctrl)

	tenantID := uuid.New()
	input := entities.CreateReconciliationContextInput{
		Name:     "Test Context",
		Type:     shared.ContextTypeOneToOne,
		Interval: "0 0 * * *",
	}

	mockCtxRepo.EXPECT().
		FindByName(gomock.Any(), input.Name).
		Return(nil, sql.ErrNoRows)

	mockCtxRepo.EXPECT().
		Create(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		})

	mockAuditPub.EXPECT().
		Publish(gomock.Any(), gomock.Any()).
		Return(nil)

	uc, err := NewUseCase(
		mockCtxRepo,
		mockSrcRepo,
		mockFmRepo,
		mockMrRepo,
		WithAuditPublisher(mockAuditPub),
	)
	require.NoError(t, err)

	result, err := uc.CreateContext(context.Background(), tenantID, input)
	require.NoError(t, err)
	assert.Equal(t, input.Name, result.Name)
}
