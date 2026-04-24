// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/repositories"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

func TestUseCase_CurrentDedupeTTL_PrefersResolver(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		dedupeTTL:         time.Minute,
		dedupeTTLResolver: func(context.Context) time.Duration { return 2 * time.Minute },
	}

	require.Equal(t, 2*time.Minute, uc.currentDedupeTTL(context.Background()))
}

func TestUseCase_FilterAndInsertChunk_UsesResolverTTLForDedupeMarking(t *testing.T) {
	t.Parallel()

	dedupe := &recordingDedupe{}
	uc := &UseCase{
		transactionRepo:   fakeTxRepo{},
		dedupe:            dedupe,
		dedupeTTL:         time.Minute,
		dedupeTTLResolver: func(context.Context) time.Duration { return 2 * time.Minute },
	}

	job := &entities.IngestionJob{ContextID: uuid.New()}
	transactions := []*shared.Transaction{{SourceID: uuid.New(), ExternalID: "ext-1"}}

	inserted, markedHashes, err := uc.filterAndInsertChunk(context.Background(), job, transactions)
	require.NoError(t, err)
	require.Equal(t, 1, inserted)
	require.Len(t, markedHashes, 1)
	require.Equal(t, 2*time.Minute, dedupe.lastTTL)
}

func TestNewUseCaseRequiresDependencies(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()

	testDeps := deps
	testDeps.JobRepo = nil
	_, err := NewUseCase(testDeps)
	require.ErrorIs(t, err, ErrNilJobRepository)

	testDeps = deps
	testDeps.TransactionRepo = nil
	_, err = NewUseCase(testDeps)
	require.ErrorIs(t, err, ErrNilTransactionRepository)

	testDeps = deps
	testDeps.Dedupe = nil
	_, err = NewUseCase(testDeps)
	require.ErrorIs(t, err, ErrNilDedupeService)

	testDeps = deps
	testDeps.Publisher = nil
	_, err = NewUseCase(testDeps)
	require.ErrorIs(t, err, ErrNilEventPublisher)

	testDeps = deps
	testDeps.OutboxRepo = nil
	_, err = NewUseCase(testDeps)
	require.ErrorIs(t, err, ErrNilOutboxRepository)

	testDeps = deps
	testDeps.Parsers = nil
	_, err = NewUseCase(testDeps)
	require.ErrorIs(t, err, ErrNilParserRegistry)

	testDeps = deps
	testDeps.FieldMapRepo = nil
	_, err = NewUseCase(testDeps)
	require.ErrorIs(t, err, ErrNilFieldMapRepository)

	testDeps = deps
	testDeps.SourceRepo = nil
	_, err = NewUseCase(testDeps)
	require.ErrorIs(t, err, ErrNilSourceRepository)
}

func TestNewUseCase_NormalizesTypedNilOptionalDeps(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()

	var typedNilTrigger *fakeMatchTrigger
	var typedNilProvider *fakeContextProvider

	deps.MatchTrigger = sharedPorts.MatchTrigger(typedNilTrigger)
	deps.ContextProvider = sharedPorts.ContextProvider(typedNilProvider)

	uc, err := NewUseCase(deps)
	require.NoError(t, err)
	require.Nil(t, uc.matchTrigger)
	require.Nil(t, uc.contextProvider)
}

func TestTriggerAutoMatchIfEnabled_IgnoresTypedNilMatchTrigger(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, uuid.NewString())

	var typedNilTrigger *fakeMatchTrigger
	uc := &UseCase{
		contextProvider: fakeContextProvider{enabled: true},
		matchTrigger:    sharedPorts.MatchTrigger(typedNilTrigger),
	}

	require.NotPanics(t, func() {
		uc.triggerAutoMatchIfEnabled(ctx, contextID)
	})
}

func TestTriggerAutoMatchIfEnabled_IgnoresTypedNilContextProvider(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, uuid.NewString())
	trigger := &fakeMatchTrigger{}

	var typedNilProvider *fakeContextProvider
	uc := &UseCase{
		contextProvider: sharedPorts.ContextProvider(typedNilProvider),
		matchTrigger:    trigger,
	}

	require.NotPanics(t, func() {
		uc.triggerAutoMatchIfEnabled(ctx, contextID)
	})
	require.False(t, trigger.called)
}

type noTxJobRepo struct{}

func (noTxJobRepo) Create(
	_ context.Context,
	_ *entities.IngestionJob,
) (*entities.IngestionJob, error) {
	return nil, nil
}

func (noTxJobRepo) FindByID(_ context.Context, _ uuid.UUID) (*entities.IngestionJob, error) {
	return nil, nil
}

func (noTxJobRepo) FindByContextID(
	_ context.Context,
	_ uuid.UUID,
	_ repositories.CursorFilter,
) ([]*entities.IngestionJob, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (noTxJobRepo) Update(
	_ context.Context,
	_ *entities.IngestionJob,
) (*entities.IngestionJob, error) {
	return nil, nil
}

func (noTxJobRepo) FindLatestByExtractionID(
	_ context.Context,
	_ uuid.UUID,
) (*entities.IngestionJob, error) {
	return nil, nil
}

func TestNewUseCaseRequiresTxInterfaces(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	deps.JobRepo = repositories.JobRepository(noTxJobRepo{})

	_, err := NewUseCase(deps)
	require.ErrorIs(t, err, ErrJobRepoNotTxRunner)
}

// noTxUpdaterJobRepo implements WithTx but not UpdateWithTx.
type noTxUpdaterJobRepo struct {
	noTxJobRepo
}

func (noTxUpdaterJobRepo) WithTx(_ context.Context, fn func(*sql.Tx) error) error {
	return fn(&sql.Tx{})
}

func TestNewUseCaseRequiresJobTxUpdater(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	deps.JobRepo = noTxUpdaterJobRepo{}

	_, err := NewUseCase(deps)
	require.ErrorIs(t, err, ErrJobRepoNotTxUpdater)
}

func TestNewUseCaseRequiresOutboxTxCreator(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()

	_, ok := deps.OutboxRepo.(outboxTxCreator)
	require.True(t, ok, "fakeOutboxRepo must implement outboxTxCreator")

	uc, err := NewUseCase(deps)
	require.NoError(t, err)
	require.NotNil(t, uc)
	require.NotNil(t, uc.outboxRepoTx)
}

func TestNewUseCaseDedupeTTLValidation(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	deps.DedupeTTL = time.Second
	_, err := NewUseCase(deps)
	require.ErrorIs(t, err, ErrDedupeTTLTooShort)

	deps = newTestDeps()
	uc, err := NewUseCase(deps)
	require.NoError(t, err)
	require.Equal(t, defaultDedupeTTL, uc.dedupeTTL)

	deps = newTestDeps()
	deps.DedupeTTL = 2 * time.Minute
	uc, err = NewUseCase(deps)
	require.NoError(t, err)
	require.Equal(t, 2*time.Minute, uc.dedupeTTL)
}
