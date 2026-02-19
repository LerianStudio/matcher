//go:build unit

package command

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/exception/domain/repositories"
	"github.com/LerianStudio/matcher/internal/exception/ports"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

func TestNewUseCase_Success(t *testing.T) {
	t.Parallel()

	repo := &stubExceptionRepo{}
	exec := &stubResolutionExecutor{}
	audit := &stubAuditPublisher{}
	actor := actorExtractor("analyst-1")
	infra := &stubInfraProvider{}

	uc, err := NewUseCase(repo, exec, audit, actor, infra)

	require.NoError(t, err)
	require.NotNil(t, uc)
	assert.Equal(t, repo, uc.exceptionRepo)
	assert.Equal(t, exec, uc.resolutionExecutor)
	assert.Equal(t, audit, uc.auditPublisher)
	assert.Equal(t, actor, uc.actorExtractor)
	assert.Equal(t, infra, uc.infraProvider)
}

func TestNewUseCase_NilExceptionRepository(t *testing.T) {
	t.Parallel()

	exec := &stubResolutionExecutor{}
	audit := &stubAuditPublisher{}
	actor := actorExtractor("analyst-1")
	infra := &stubInfraProvider{}

	uc, err := NewUseCase(nil, exec, audit, actor, infra)

	require.ErrorIs(t, err, ErrNilExceptionRepository)
	assert.Nil(t, uc)
}

func TestNewUseCase_NilResolutionExecutor(t *testing.T) {
	t.Parallel()

	repo := &stubExceptionRepo{}
	audit := &stubAuditPublisher{}
	actor := actorExtractor("analyst-1")
	infra := &stubInfraProvider{}

	uc, err := NewUseCase(repo, nil, audit, actor, infra)

	require.ErrorIs(t, err, ErrNilResolutionExecutor)
	assert.Nil(t, uc)
}

func TestNewUseCase_NilAuditPublisher(t *testing.T) {
	t.Parallel()

	repo := &stubExceptionRepo{}
	exec := &stubResolutionExecutor{}
	actor := actorExtractor("analyst-1")
	infra := &stubInfraProvider{}

	uc, err := NewUseCase(repo, exec, nil, actor, infra)

	require.ErrorIs(t, err, ErrNilAuditPublisher)
	assert.Nil(t, uc)
}

func TestNewUseCase_NilActorExtractor(t *testing.T) {
	t.Parallel()

	repo := &stubExceptionRepo{}
	exec := &stubResolutionExecutor{}
	audit := &stubAuditPublisher{}
	infra := &stubInfraProvider{}

	uc, err := NewUseCase(repo, exec, audit, nil, infra)

	require.ErrorIs(t, err, ErrNilActorExtractor)
	assert.Nil(t, uc)
}

func TestNewUseCase_NilInfraProvider(t *testing.T) {
	t.Parallel()

	repo := &stubExceptionRepo{}
	exec := &stubResolutionExecutor{}
	audit := &stubAuditPublisher{}
	actor := actorExtractor("analyst-1")

	uc, err := NewUseCase(repo, exec, audit, actor, nil)

	require.ErrorIs(t, err, ErrNilInfraProvider)
	assert.Nil(t, uc)
}

func TestNewUseCase_AllDependenciesNil(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(nil, nil, nil, nil, nil)

	require.ErrorIs(t, err, ErrNilExceptionRepository)
	assert.Nil(t, uc)
}

func TestNewUseCase_ValidationOrder(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		repo        repositories.ExceptionRepository
		exec        ports.ResolutionExecutor
		audit       ports.AuditPublisher
		actor       ports.ActorExtractor
		infra       sharedPorts.InfrastructureProvider
		expectedErr error
	}{
		{
			name:        "nil repo returns first",
			repo:        nil,
			exec:        &stubResolutionExecutor{},
			audit:       &stubAuditPublisher{},
			actor:       actorExtractor("a"),
			infra:       &stubInfraProvider{},
			expectedErr: ErrNilExceptionRepository,
		},
		{
			name:        "nil executor returns second",
			repo:        &stubExceptionRepo{},
			exec:        nil,
			audit:       &stubAuditPublisher{},
			actor:       actorExtractor("a"),
			infra:       &stubInfraProvider{},
			expectedErr: ErrNilResolutionExecutor,
		},
		{
			name:        "nil audit returns third",
			repo:        &stubExceptionRepo{},
			exec:        &stubResolutionExecutor{},
			audit:       nil,
			actor:       actorExtractor("a"),
			infra:       &stubInfraProvider{},
			expectedErr: ErrNilAuditPublisher,
		},
		{
			name:        "nil actor returns fourth",
			repo:        &stubExceptionRepo{},
			exec:        &stubResolutionExecutor{},
			audit:       &stubAuditPublisher{},
			actor:       nil,
			infra:       &stubInfraProvider{},
			expectedErr: ErrNilActorExtractor,
		},
		{
			name:        "nil infra returns fifth",
			repo:        &stubExceptionRepo{},
			exec:        &stubResolutionExecutor{},
			audit:       &stubAuditPublisher{},
			actor:       actorExtractor("a"),
			infra:       nil,
			expectedErr: ErrNilInfraProvider,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			uc, err := NewUseCase(tc.repo, tc.exec, tc.audit, tc.actor, tc.infra)

			require.ErrorIs(t, err, tc.expectedErr)
			assert.Nil(t, uc)
		})
	}
}

func TestErrorDefinitions(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "ErrNilExceptionRepository",
			err:      ErrNilExceptionRepository,
			expected: "exception repository is required",
		},
		{
			name:     "ErrNilResolutionExecutor",
			err:      ErrNilResolutionExecutor,
			expected: "resolution executor is required",
		},
		{
			name:     "ErrNilAuditPublisher",
			err:      ErrNilAuditPublisher,
			expected: "audit publisher is required",
		},
		{
			name:     "ErrNilActorExtractor",
			err:      ErrNilActorExtractor,
			expected: "actor extractor is required",
		},
		{
			name:     "ErrExceptionIDRequired",
			err:      ErrExceptionIDRequired,
			expected: "exception id is required",
		},
		{
			name:     "ErrActorRequired",
			err:      ErrActorRequired,
			expected: "actor is required",
		},
		{
			name:     "ErrZeroAdjustmentAmount",
			err:      ErrZeroAdjustmentAmount,
			expected: "adjustment amount cannot be zero",
		},
		{
			name:     "ErrNegativeAdjustmentAmount",
			err:      ErrNegativeAdjustmentAmount,
			expected: "adjustment amount cannot be negative",
		},
		{
			name:     "ErrInvalidCurrency",
			err:      ErrInvalidCurrency,
			expected: "invalid currency code",
		},
		{
			name:     "ErrNilDisputeRepository",
			err:      ErrNilDisputeRepository,
			expected: "dispute repository is required",
		},
		{
			name:     "ErrDisputeIDRequired",
			err:      ErrDisputeIDRequired,
			expected: "dispute id is required",
		},
		{
			name:     "ErrDisputeCategoryRequired",
			err:      ErrDisputeCategoryRequired,
			expected: "dispute category is required",
		},
		{
			name:     "ErrDisputeDescriptionRequired",
			err:      ErrDisputeDescriptionRequired,
			expected: "dispute description is required",
		},
		{
			name:     "ErrDisputeCommentRequired",
			err:      ErrDisputeCommentRequired,
			expected: "evidence comment is required",
		},
		{
			name:     "ErrDisputeResolutionRequired",
			err:      ErrDisputeResolutionRequired,
			expected: "dispute resolution is required",
		},
		{
			name:     "ErrCallbackExternalSystem",
			err:      ErrCallbackExternalSystem,
			expected: "external system is required",
		},
		{
			name:     "ErrCallbackExternalIssueID",
			err:      ErrCallbackExternalIssueID,
			expected: "external issue id is required",
		},
		{
			name:     "ErrCallbackStatusRequired",
			err:      ErrCallbackStatusRequired,
			expected: "callback status is required",
		},
		{
			name:     "ErrCallbackAssigneeRequired",
			err:      ErrCallbackAssigneeRequired,
			expected: "callback assignee is required",
		},
		{
			name:     "ErrCallbackStatusUnsupported",
			err:      ErrCallbackStatusUnsupported,
			expected: "callback status is unsupported",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.Error(t, tc.err, "error should not be nil")
			assert.Equal(t, tc.expected, tc.err.Error(), "error message mismatch")
		})
	}
}

func TestErrorsAreDistinct(t *testing.T) {
	t.Parallel()

	errors := []error{
		ErrNilExceptionRepository,
		ErrNilResolutionExecutor,
		ErrNilAuditPublisher,
		ErrNilActorExtractor,
		ErrExceptionIDRequired,
		ErrActorRequired,
		ErrZeroAdjustmentAmount,
		ErrNegativeAdjustmentAmount,
		ErrInvalidCurrency,
		ErrNilDisputeRepository,
		ErrDisputeIDRequired,
		ErrDisputeCategoryRequired,
		ErrDisputeDescriptionRequired,
		ErrDisputeCommentRequired,
		ErrDisputeResolutionRequired,
		ErrCallbackExternalSystem,
		ErrCallbackExternalIssueID,
		ErrCallbackStatusRequired,
		ErrCallbackAssigneeRequired,
		ErrCallbackStatusUnsupported,
	}

	seen := make(map[string]int)

	for i, err := range errors {
		msg := err.Error()
		if existingIdx, ok := seen[msg]; ok {
			t.Errorf("duplicate error message %q at index %d and %d", msg, i, existingIdx)
		}

		seen[msg] = i
	}
}
