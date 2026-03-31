//go:build unit

package command

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/mock/gomock"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"

	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	"github.com/LerianStudio/matcher/internal/governance/domain/repositories/mocks"
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
		repo.EXPECT().Pseudonymize(gomock.Any(), "actor-123").Return(nil)

		uc, err := NewActorMappingUseCase(repo)
		require.NoError(t, err)

		err = uc.PseudonymizeActor(testContext(), "actor-123")
		assert.NoError(t, err)
	})

	t.Run("repository error", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockActorMappingRepository(ctrl)
		repo.EXPECT().Pseudonymize(gomock.Any(), "actor-456").Return(errTestRepoFailure)

		uc, err := NewActorMappingUseCase(repo)
		require.NoError(t, err)

		err = uc.PseudonymizeActor(testContext(), "actor-456")
		require.Error(t, err)
		assert.ErrorIs(t, err, errTestRepoFailure)
	})
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
