// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	tmcore "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/core"
	streaming "github.com/LerianStudio/lib-streaming"
	"github.com/LerianStudio/lib-streaming/streamingtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/mock/gomock"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"

	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	"github.com/LerianStudio/matcher/internal/governance/domain/repositories/mocks"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
	"github.com/LerianStudio/matcher/internal/streaming/emission"
)

var errTestRepoFailure = errors.New("repository failure")

func testContext() context.Context {
	return libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
}

func TestNewActorMappingUseCase(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockActorMappingRepository(ctrl)

		uc, err := NewActorMappingUseCase(repo)
		require.NoError(t, err)
		require.NotNil(t, uc)
	})

	t.Run("nil repository", func(t *testing.T) {
		t.Parallel()

		uc, err := NewActorMappingUseCase(nil)
		require.ErrorIs(t, err, ErrNilActorMappingRepository)
		require.Nil(t, uc)
	})
}

func TestUpsertActorMapping(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockActorMappingRepository(ctrl)
		repo.EXPECT().Upsert(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, m *entities.ActorMapping) (*entities.ActorMapping, error) {
				require.Equal(t, "actor-123", m.ActorID)
				require.NotNil(t, m.DisplayName)
				require.Equal(t, "John Doe", *m.DisplayName)
				require.NotNil(t, m.Email)
				require.Equal(t, "john@example.com", *m.Email)
				return &entities.ActorMapping{ActorID: m.ActorID}, nil
			},
		)

		uc, err := NewActorMappingUseCase(repo)
		require.NoError(t, err)

		displayName := "John Doe"
		email := "john@example.com"
		result, err := uc.UpsertActorMapping(testContext(), "actor-123", &displayName, &email)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, "actor-123", result.ActorID)
	})

	t.Run("success with nil optional fields", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockActorMappingRepository(ctrl)
		repo.EXPECT().Upsert(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, m *entities.ActorMapping) (*entities.ActorMapping, error) {
				require.Equal(t, "actor-123", m.ActorID)
				require.Nil(t, m.DisplayName)
				require.Nil(t, m.Email)
				return &entities.ActorMapping{ActorID: m.ActorID}, nil
			},
		)

		uc, err := NewActorMappingUseCase(repo)
		require.NoError(t, err)

		result, err := uc.UpsertActorMapping(testContext(), "actor-123", nil, nil)
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("empty actor id returns entity validation error", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockActorMappingRepository(ctrl)

		uc, err := NewActorMappingUseCase(repo)
		require.NoError(t, err)

		result, err := uc.UpsertActorMapping(testContext(), "", nil, nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, entities.ErrActorIDRequired)
		assert.Nil(t, result)
	})

	t.Run("repository error", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockActorMappingRepository(ctrl)
		repo.EXPECT().Upsert(gomock.Any(), gomock.Any()).Return(nil, errTestRepoFailure)

		uc, err := NewActorMappingUseCase(repo)
		require.NoError(t, err)

		displayName := "Jane"
		result, err := uc.UpsertActorMapping(testContext(), "actor-456", &displayName, nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, errTestRepoFailure)
		assert.Nil(t, result)
	})

	t.Run("nil persisted mapping returns sentinel error", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockActorMappingRepository(ctrl)
		repo.EXPECT().Upsert(gomock.Any(), gomock.Any()).Return(nil, nil)

		uc, err := NewActorMappingUseCase(repo)
		require.NoError(t, err)

		displayName := "Jane"
		result, err := uc.UpsertActorMapping(testContext(), "actor-789", &displayName, nil)
		require.ErrorIs(t, err, ErrNilPersistedActorMapping)
		assert.Nil(t, result)
	})
}

func TestPseudonymizeActor(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockActorMappingRepository(ctrl)
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		mock.ExpectBegin()
		mock.ExpectCommit()
		repo.EXPECT().PseudonymizeWithTx(gomock.Any(), gomock.AssignableToTypeOf(&sql.Tx{}), "actor-123").Return(nil)

		uc, err := NewActorMappingUseCase(
			repo,
			WithActorMappingInfrastructure(&actorMappingTestProvider{db: db}),
			WithActorMappingStreamingEmitter(streamingtest.NewMockEmitter()),
		)
		require.NoError(t, err)

		ctx := tmcore.ContextWithTenantID(testContext(), "018f4f95-0000-7000-8000-000000000001")
		err = uc.PseudonymizeActor(ctx, "actor-123")
		assert.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("repository error", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockActorMappingRepository(ctrl)
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		mock.ExpectBegin()
		mock.ExpectRollback()
		repo.EXPECT().PseudonymizeWithTx(gomock.Any(), gomock.AssignableToTypeOf(&sql.Tx{}), "actor-456").Return(errTestRepoFailure)

		uc, err := NewActorMappingUseCase(
			repo,
			WithActorMappingInfrastructure(&actorMappingTestProvider{db: db}),
			WithActorMappingStreamingEmitter(streamingtest.NewMockEmitter()),
		)
		require.NoError(t, err)

		ctx := tmcore.ContextWithTenantID(testContext(), "018f4f95-0000-7000-8000-000000000001")
		err = uc.PseudonymizeActor(ctx, "actor-456")
		require.Error(t, err)
		assert.ErrorIs(t, err, errTestRepoFailure)
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestPseudonymizeActor_StreamingFailureRollsBackTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	repo := mocks.NewMockActorMappingRepository(ctrl)

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectRollback()

	emitter := streamingtest.NewMockEmitter()
	emitter.SetError(errors.New("streaming unavailable"))

	uc, err := NewActorMappingUseCase(
		repo,
		WithActorMappingInfrastructure(&actorMappingTestProvider{db: db}),
		WithActorMappingStreamingEmitter(emitter),
	)
	require.NoError(t, err)

	ctx := tmcore.ContextWithTenantID(testContext(), "018f4f95-0000-7000-8000-000000000001")
	repo.EXPECT().PseudonymizeWithTx(gomock.Any(), gomock.AssignableToTypeOf(&sql.Tx{}), "actor-123").Return(nil)

	err = uc.PseudonymizeActor(ctx, "actor-123")
	require.Error(t, err)
	require.Contains(t, err.Error(), "emit actor pseudonymized")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPseudonymizeActor_StreamingPayloadIncludesTenantIDFromContext(t *testing.T) {
	ctrl := gomock.NewController(t)
	repo := mocks.NewMockActorMappingRepository(ctrl)

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	emitter := streamingtest.NewMockEmitter()
	uc, err := NewActorMappingUseCase(
		repo,
		WithActorMappingInfrastructure(&actorMappingTestProvider{db: db}),
		WithActorMappingStreamingEmitter(emitter),
	)
	require.NoError(t, err)

	tenantID := "018f4f95-0000-7000-8000-000000000001"
	ctx := tmcore.ContextWithTenantID(testContext(), tenantID)
	repo.EXPECT().PseudonymizeWithTx(gomock.Any(), gomock.AssignableToTypeOf(&sql.Tx{}), "actor-123").Return(nil)

	err = uc.PseudonymizeActor(ctx, "actor-123")
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())

	requests := emitter.Requests()
	require.Len(t, requests, 1)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(requests[0].Payload, &payload))
	require.Equal(t, tenantID, payload["tenant_id"])
}

func TestPseudonymizeActor_StreamingNilTxLeaseFailsBeforeMutation(t *testing.T) {
	ctrl := gomock.NewController(t)
	repo := mocks.NewMockActorMappingRepository(ctrl)
	emitter := streamingtest.NewMockEmitter()

	uc, err := NewActorMappingUseCase(
		repo,
		WithActorMappingInfrastructure(&nilActorMappingTxProvider{}),
		WithActorMappingStreamingEmitter(emitter),
	)
	require.NoError(t, err)

	ctx := tmcore.ContextWithTenantID(testContext(), "018f4f95-0000-7000-8000-000000000001")

	err = uc.PseudonymizeActor(ctx, "actor-123")

	require.ErrorIs(t, err, emission.ErrCriticalOutboxTxRequired)
	streamingtest.AssertNoEvents(t, emitter)
}

func TestPseudonymizeActor_NilEmitterFailsBeforeMutation(t *testing.T) {
	testCases := []struct {
		name    string
		emitter streaming.Emitter
	}{
		{name: "nil emitter", emitter: nil},
		{name: "typed nil emitter", emitter: (*typedNilActorEmitter)(nil)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			repo := mocks.NewMockActorMappingRepository(ctrl)

			uc, err := NewActorMappingUseCase(
				repo,
				WithActorMappingInfrastructure(&nilActorMappingTxProvider{}),
				WithActorMappingStreamingEmitter(tc.emitter),
			)
			require.NoError(t, err)

			ctx := tmcore.ContextWithTenantID(testContext(), "018f4f95-0000-7000-8000-000000000001")

			err = uc.PseudonymizeActor(ctx, "actor-123")

			require.ErrorIs(t, err, emission.ErrCriticalOutboxTxRequired)
		})
	}
}

type typedNilActorEmitter struct{}

func (*typedNilActorEmitter) Emit(context.Context, streaming.EmitRequest) error { return nil }

func (*typedNilActorEmitter) Close() error { return nil }

func (*typedNilActorEmitter) Healthy(context.Context) error { return nil }

type nilActorMappingTxProvider struct{}

func (*nilActorMappingTxProvider) BeginTx(context.Context) (*sharedPorts.TxLease, error) {
	return nil, nil
}

func (*nilActorMappingTxProvider) GetRedisConnection(context.Context) (*sharedPorts.RedisConnectionLease, error) {
	return nil, nil
}

func (*nilActorMappingTxProvider) GetReplicaDB(context.Context) (*sharedPorts.DBLease, error) {
	return nil, nil
}

func (*nilActorMappingTxProvider) GetPrimaryDB(context.Context) (*sharedPorts.DBLease, error) {
	return nil, nil
}

type actorMappingTestProvider struct {
	db *sql.DB
}

func (provider *actorMappingTestProvider) BeginTx(ctx context.Context) (*sharedPorts.TxLease, error) {
	tx, err := provider.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	return sharedPorts.NewTxLease(tx, func() {}), nil
}

func (*actorMappingTestProvider) GetRedisConnection(context.Context) (*sharedPorts.RedisConnectionLease, error) {
	return nil, nil
}

func (*actorMappingTestProvider) GetReplicaDB(context.Context) (*sharedPorts.DBLease, error) {
	return nil, nil
}

func (*actorMappingTestProvider) GetPrimaryDB(context.Context) (*sharedPorts.DBLease, error) {
	return nil, nil
}

func TestDeleteActorMapping(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockActorMappingRepository(ctrl)
		repo.EXPECT().Delete(gomock.Any(), "actor-123").Return(nil)

		uc, err := NewActorMappingUseCase(repo)
		require.NoError(t, err)

		err = uc.DeleteActorMapping(testContext(), "actor-123")
		assert.NoError(t, err)
	})

	t.Run("repository error", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockActorMappingRepository(ctrl)
		repo.EXPECT().Delete(gomock.Any(), "actor-789").Return(errTestRepoFailure)

		uc, err := NewActorMappingUseCase(repo)
		require.NoError(t, err)

		err = uc.DeleteActorMapping(testContext(), "actor-789")
		require.Error(t, err)
		assert.ErrorIs(t, err, errTestRepoFailure)
	})
}

func TestSafeActorIDPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{name: "empty string", input: "", expected: "***"},
		{name: "long ID", input: "user@example.com", expected: "user***"},
		{name: "short ID", input: "ab", expected: "a***"},
		{name: "exact 4", input: "abcd", expected: "a***"},
		{name: "5 chars", input: "abcde", expected: "abcd***"},
		{name: "single char", input: "x", expected: "x***"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, entities.SafeActorIDPrefix(tt.input))
		})
	}
}

func TestErrNilActorMappingRepository(t *testing.T) {
	t.Parallel()

	require.Error(t, ErrNilActorMappingRepository)
	assert.Equal(t, "actor mapping repository is required", ErrNilActorMappingRepository.Error())
}
