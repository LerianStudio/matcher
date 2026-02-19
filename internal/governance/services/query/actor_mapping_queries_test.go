//go:build unit

package query

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/mock/gomock"

	libCommons "github.com/LerianStudio/lib-uncommons/v2/uncommons"

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

func TestNewActorMappingQueryUseCase(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockActorMappingRepository(ctrl)

		uc, err := NewActorMappingQueryUseCase(repo)
		require.NoError(t, err)
		require.NotNil(t, uc)
	})

	t.Run("nil repository", func(t *testing.T) {
		t.Parallel()

		uc, err := NewActorMappingQueryUseCase(nil)
		require.ErrorIs(t, err, ErrNilActorMappingRepository)
		require.Nil(t, uc)
	})
}

func TestGetActorMapping(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		now := time.Now().UTC()
		displayName := "John Doe"
		email := "john@example.com"

		expected := &entities.ActorMapping{
			ActorID:     "actor-123",
			DisplayName: &displayName,
			Email:       &email,
			CreatedAt:   now,
			UpdatedAt:   now,
		}

		repo := mocks.NewMockActorMappingRepository(ctrl)
		repo.EXPECT().GetByActorID(gomock.Any(), "actor-123").Return(expected, nil)

		uc, err := NewActorMappingQueryUseCase(repo)
		require.NoError(t, err)

		result, err := uc.GetActorMapping(testContext(), "actor-123")
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, "actor-123", result.ActorID)
		assert.Equal(t, &displayName, result.DisplayName)
		assert.Equal(t, &email, result.Email)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockActorMappingRepository(ctrl)

		notFoundErr := errors.New("actor mapping not found")
		repo.EXPECT().GetByActorID(gomock.Any(), "nonexistent").Return(nil, notFoundErr)

		uc, err := NewActorMappingQueryUseCase(repo)
		require.NoError(t, err)

		result, err := uc.GetActorMapping(testContext(), "nonexistent")
		require.Error(t, err)
		require.Nil(t, result)
	})

	t.Run("repository error", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockActorMappingRepository(ctrl)
		repo.EXPECT().GetByActorID(gomock.Any(), "actor-456").Return(nil, errTestRepoFailure)

		uc, err := NewActorMappingQueryUseCase(repo)
		require.NoError(t, err)

		result, err := uc.GetActorMapping(testContext(), "actor-456")
		require.Error(t, err)
		require.Nil(t, result)
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
