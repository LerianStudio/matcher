//go:build unit

package command

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/LerianStudio/matcher/internal/configuration/domain/repositories"
	repoMocks "github.com/LerianStudio/matcher/internal/configuration/domain/repositories/mocks"
	"github.com/LerianStudio/matcher/internal/configuration/ports"
	portMocks "github.com/LerianStudio/matcher/internal/configuration/ports/mocks"
)

func TestSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		message string
	}{
		{"ErrNilContextRepository", ErrNilContextRepository, "context repository is required"},
		{"ErrNilSourceRepository", ErrNilSourceRepository, "source repository is required"},
		{"ErrNilFieldMapRepository", ErrNilFieldMapRepository, "field map repository is required"},
		{
			"ErrNilMatchRuleRepository",
			ErrNilMatchRuleRepository,
			"match rule repository is required",
		},
		{"ErrRuleIDsRequired", ErrRuleIDsRequired, "rule IDs are required"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			assert.Equal(t, tt.message, tt.err.Error())
		})
	}
}

func TestNewUseCase(t *testing.T) {
	t.Parallel()

	validDeps := func(ctrl *gomock.Controller) (
		repositories.ContextRepository,
		repositories.SourceRepository,
		repositories.FieldMapRepository,
		repositories.MatchRuleRepository,
	) {
		return repoMocks.NewMockContextRepository(ctrl),
			repoMocks.NewMockSourceRepository(ctrl),
			repoMocks.NewMockFieldMapRepository(ctrl),
			repoMocks.NewMockMatchRuleRepository(ctrl)
	}

	t.Run("success with all dependencies", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)

		ctxRepo, src, fm, mr := validDeps(ctrl)

		uc, err := NewUseCase(ctxRepo, src, fm, mr)

		require.NoError(t, err)
		require.NotNil(t, uc)
	})

	t.Run("nil context repository returns error", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)

		_, src, fm, mr := validDeps(ctrl)

		uc, err := NewUseCase(nil, src, fm, mr)

		assert.Nil(t, uc)
		require.ErrorIs(t, err, ErrNilContextRepository)
	})

	t.Run("nil source repository returns error", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)

		ctxRepo, _, fm, mr := validDeps(ctrl)

		uc, err := NewUseCase(ctxRepo, nil, fm, mr)

		assert.Nil(t, uc)
		require.ErrorIs(t, err, ErrNilSourceRepository)
	})

	t.Run("nil field map repository returns error", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)

		ctxRepo, src, _, mr := validDeps(ctrl)

		uc, err := NewUseCase(ctxRepo, src, nil, mr)

		assert.Nil(t, uc)
		require.ErrorIs(t, err, ErrNilFieldMapRepository)
	})

	t.Run("nil match rule repository returns error", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)

		ctxRepo, src, fm, _ := validDeps(ctrl)

		uc, err := NewUseCase(ctxRepo, src, fm, nil)

		assert.Nil(t, uc)
		require.ErrorIs(t, err, ErrNilMatchRuleRepository)
	})
}

func TestUseCaseFieldsInitialized(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	uc, err := NewUseCase(
		repoMocks.NewMockContextRepository(ctrl),
		repoMocks.NewMockSourceRepository(ctrl),
		repoMocks.NewMockFieldMapRepository(ctrl),
		repoMocks.NewMockMatchRuleRepository(ctrl),
	)

	require.NoError(t, err)
	require.NotNil(t, uc)

	assert.NotNil(t, uc.contextRepo)
	assert.NotNil(t, uc.sourceRepo)
	assert.NotNil(t, uc.fieldMapRepo)
	assert.NotNil(t, uc.matchRuleRepo)
}

func TestNewUseCase_WithAuditPublisher(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)
	mockAuditPub := portMocks.NewMockAuditPublisher(ctrl)

	uc, err := NewUseCase(
		mockCtxRepo,
		mockSrcRepo,
		mockFmRepo,
		mockMrRepo,
		WithAuditPublisher(mockAuditPub),
	)

	require.NoError(t, err)
	require.NotNil(t, uc)
	assert.NotNil(t, uc.auditPublisher)
}

func TestNewUseCase_WithMultipleOptions(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)
	mockAuditPub1 := portMocks.NewMockAuditPublisher(ctrl)
	mockAuditPub2 := portMocks.NewMockAuditPublisher(ctrl)

	uc, err := NewUseCase(
		mockCtxRepo,
		mockSrcRepo,
		mockFmRepo,
		mockMrRepo,
		WithAuditPublisher(mockAuditPub1),
		WithAuditPublisher(mockAuditPub2),
	)

	require.NoError(t, err)
	require.NotNil(t, uc)
	assert.Equal(t, mockAuditPub2, uc.auditPublisher)
}

func TestPublishAudit_NilPublisher(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		auditPublisher: nil,
	}

	uc.publishAudit(context.Background(), "context", uuid.New(), "create", nil)
}

func TestPublishAudit_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	mockAuditPub := portMocks.NewMockAuditPublisher(ctrl)
	entityID := uuid.New()

	mockAuditPub.EXPECT().
		Publish(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, event ports.AuditEvent) error {
			assert.Equal(t, "context", event.EntityType)
			assert.Equal(t, entityID, event.EntityID)
			assert.Equal(t, "create", event.Action)
			return nil
		})

	uc := &UseCase{
		auditPublisher: mockAuditPub,
	}

	uc.publishAudit(
		context.Background(),
		"context",
		entityID,
		"create",
		map[string]any{"name": "test"},
	)
}

// TestPublishAudit_FailedPublishLogsError is a smoke test that verifies publishAudit
// does not panic when the underlying Publish call fails. The gomock expectation
// implicitly asserts that Publish was called exactly once with the returned error.
func TestPublishAudit_FailedPublishLogsError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	mockAuditPub := portMocks.NewMockAuditPublisher(ctrl)
	entityID := uuid.New()

	mockAuditPub.EXPECT().
		Publish(gomock.Any(), gomock.Any()).
		Return(assert.AnError)

	uc := &UseCase{
		auditPublisher: mockAuditPub,
	}

	assert.NotPanics(t, func() {
		uc.publishAudit(context.Background(), "context", entityID, "update", nil)
	})
}
