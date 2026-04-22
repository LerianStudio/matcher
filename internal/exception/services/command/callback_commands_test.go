//go:build unit

package command

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedexception "github.com/LerianStudio/matcher/internal/shared/domain/exception"
)

var errTestIdempotency = errors.New("test: idempotency failed")

type stubIdempotencyRepo struct {
	acquired        bool
	tryAcquireErr   error
	reacquired      bool
	reacquireErr    error
	reacquireCalls  int
	markCompleteErr error
	markFailedErr   error
	markFailedCalls int
	cachedResult    *shared.IdempotencyResult
	getCachedErr    error
}

func (repo *stubIdempotencyRepo) TryAcquire(
	_ context.Context,
	_ shared.IdempotencyKey,
) (bool, error) {
	if repo.tryAcquireErr != nil {
		return false, repo.tryAcquireErr
	}

	return repo.acquired, nil
}

func (repo *stubIdempotencyRepo) TryReacquireFromFailed(
	_ context.Context,
	_ shared.IdempotencyKey,
) (bool, error) {
	repo.reacquireCalls++
	if repo.reacquireErr != nil {
		return false, repo.reacquireErr
	}

	return repo.reacquired, nil
}

func (repo *stubIdempotencyRepo) MarkComplete(
	_ context.Context,
	_ shared.IdempotencyKey,
	_ []byte,
	_ int,
) error {
	return repo.markCompleteErr
}

func (repo *stubIdempotencyRepo) MarkFailed(
	_ context.Context,
	_ shared.IdempotencyKey,
) error {
	repo.markFailedCalls++
	return repo.markFailedErr
}

func (repo *stubIdempotencyRepo) GetCachedResult(
	_ context.Context,
	_ shared.IdempotencyKey,
) (*shared.IdempotencyResult, error) {
	if repo.getCachedErr != nil {
		return nil, repo.getCachedErr
	}

	if repo.cachedResult != nil {
		return repo.cachedResult, nil
	}

	return &shared.IdempotencyResult{Status: shared.IdempotencyStatusComplete}, nil
}

func TestCallbackIdempotency_FirstCall_Processes(t *testing.T) {
	t.Parallel()

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	idempotencyRepo := &stubIdempotencyRepo{acquired: true}
	exceptionRepo := &stubExceptionRepo{exception: exception}
	audit := &stubAuditPublisher{}

	uc, err := NewExceptionUseCase(exceptionRepo, actorExtractor("system"), audit, &stubInfraProvider{}, WithIdempotencyRepository(idempotencyRepo), WithCallbackRateLimiter(&stubCallbackRateLimiter{allowed: true}))
	require.NoError(t, err)

	err = uc.ProcessCallback(context.Background(), ProcessCallbackCommand{
		IdempotencyKey:  "jira:MATCH-123:callback",
		ExceptionID:     exception.ID,
		CallbackType:    "jira",
		ExternalSystem:  "JIRA",
		ExternalIssueID: "PROJ-123",
		Status:          "RESOLVED",
		ResolutionNotes: "Resolved by JIRA",
		Payload:         map[string]any{"status": "Done"},
	})
	require.NoError(t, err)
	require.Equal(t, value_objects.ExceptionStatusResolved, exception.Status)
	require.NotNil(t, exception.ExternalSystem)
	require.Equal(t, "JIRA", *exception.ExternalSystem)

	require.NotNil(t, audit.lastEvent)
	require.Equal(t, "CALLBACK_PROCESSED", audit.lastEvent.Action)
	require.Equal(t, "system", audit.lastEvent.Actor)
	parsedKey, parseErr := shared.ParseIdempotencyKey("jira:MATCH-123:callback")
	require.NoError(t, parseErr)
	require.Equal(t, idempotencyKeyHash(parsedKey), audit.lastEvent.Metadata["idempotency_key_hash"])
	require.Empty(t, audit.lastEvent.Metadata["idempotency_key"])
	require.Equal(t, "jira", audit.lastEvent.Metadata["callback_type"])
}

func TestProcessCallback_AssignedRequiresAssignee(t *testing.T) {
	t.Parallel()

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	idempotencyRepo := &stubIdempotencyRepo{acquired: true}
	exceptionRepo := &stubExceptionRepo{exception: exception}
	audit := &stubAuditPublisher{}

	uc, err := NewExceptionUseCase(exceptionRepo, actorExtractor("system"), audit, &stubInfraProvider{}, WithIdempotencyRepository(idempotencyRepo), WithCallbackRateLimiter(&stubCallbackRateLimiter{allowed: true}))
	require.NoError(t, err)

	err = uc.ProcessCallback(context.Background(), ProcessCallbackCommand{
		IdempotencyKey:  "webhook:MATCH-123:callback",
		ExceptionID:     exception.ID,
		CallbackType:    "webhook",
		ExternalSystem:  "WEBHOOK",
		ExternalIssueID: "EXT-200",
		Status:          "ASSIGNED",
	})
	require.ErrorIs(t, err, ErrCallbackAssigneeRequired)
	require.Equal(t, 1, idempotencyRepo.markFailedCalls)
}

func TestProcessCallback_PayloadFallback_Assigns(t *testing.T) {
	t.Parallel()

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	idempotencyRepo := &stubIdempotencyRepo{acquired: true}
	exceptionRepo := &stubExceptionRepo{exception: exception}
	audit := &stubAuditPublisher{}

	uc, err := NewExceptionUseCase(exceptionRepo, actorExtractor("system"), audit, &stubInfraProvider{}, WithIdempotencyRepository(idempotencyRepo), WithCallbackRateLimiter(&stubCallbackRateLimiter{allowed: true}))
	require.NoError(t, err)

	dueAt := time.Now().UTC().Add(2 * time.Hour).Format(time.RFC3339)
	payload := map[string]any{
		"external_system":   "JIRA",
		"external_issue_id": "PROJ-789",
		"status":            "ASSIGNED",
		"assignee":          "analyst-5",
		"due_at":            dueAt,
	}

	err = uc.ProcessCallback(context.Background(), ProcessCallbackCommand{
		IdempotencyKey: "jira:MATCH-123:callback",
		ExceptionID:    exception.ID,
		CallbackType:   "jira",
		Payload:        payload,
	})
	require.NoError(t, err)
	require.Equal(t, value_objects.ExceptionStatusAssigned, exception.Status)
	require.NotNil(t, exception.AssignedTo)
	require.Equal(t, "analyst-5", *exception.AssignedTo)
	require.NotNil(t, exception.DueAt)
}

func TestProcessCallback_UpdateErrorMarksFailed(t *testing.T) {
	t.Parallel()

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	idempotencyRepo := &stubIdempotencyRepo{acquired: true}
	exceptionRepo := &stubExceptionRepo{exception: exception, updateErr: errTestUpdate}
	audit := &stubAuditPublisher{}

	uc, err := NewExceptionUseCase(exceptionRepo, actorExtractor("system"), audit, &stubInfraProvider{}, WithIdempotencyRepository(idempotencyRepo), WithCallbackRateLimiter(&stubCallbackRateLimiter{allowed: true}))
	require.NoError(t, err)

	err = uc.ProcessCallback(context.Background(), ProcessCallbackCommand{
		IdempotencyKey:  "jira:MATCH-123:callback",
		ExceptionID:     exception.ID,
		CallbackType:    "jira",
		ExternalSystem:  "JIRA",
		ExternalIssueID: "PROJ-999",
		Status:          "OPEN",
	})
	require.ErrorIs(t, err, errTestUpdate)
	require.Equal(t, 1, idempotencyRepo.markFailedCalls)
}

func TestCallbackIdempotency_DuplicateCall_Ignored(t *testing.T) {
	t.Parallel()

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	idempotencyRepo := &stubIdempotencyRepo{acquired: false}
	exceptionRepo := &stubExceptionRepo{exception: exception}
	audit := &stubAuditPublisher{}

	uc, err := NewExceptionUseCase(exceptionRepo, actorExtractor("system"), audit, &stubInfraProvider{}, WithIdempotencyRepository(idempotencyRepo), WithCallbackRateLimiter(&stubCallbackRateLimiter{allowed: true}))
	require.NoError(t, err)

	err = uc.ProcessCallback(context.Background(), ProcessCallbackCommand{
		IdempotencyKey:  "jira:MATCH-123:callback",
		ExceptionID:     exception.ID,
		CallbackType:    "jira",
		ExternalSystem:  "JIRA",
		ExternalIssueID: "PROJ-123",
		Status:          "OPEN",
		Payload:         map[string]any{"status": "Done"},
	})
	require.NoError(t, err)

	require.NotNil(t, audit.lastEvent)
	require.Equal(t, "CALLBACK_DUPLICATE_IGNORED", audit.lastEvent.Action)
	require.Equal(t, "system", audit.lastEvent.Actor)
	parsedKey, parseErr := shared.ParseIdempotencyKey("jira:MATCH-123:callback")
	require.NoError(t, parseErr)
	require.Equal(t, idempotencyKeyHash(parsedKey), audit.lastEvent.Metadata["idempotency_key_hash"])
	require.Empty(t, audit.lastEvent.Metadata["idempotency_key"])
}

func TestCallbackIdempotency_InvalidKey_Rejected(t *testing.T) {
	t.Parallel()

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	idempotencyRepo := &stubIdempotencyRepo{acquired: true}
	exceptionRepo := &stubExceptionRepo{exception: exception}
	audit := &stubAuditPublisher{}

	uc, err := NewExceptionUseCase(exceptionRepo, actorExtractor("system"), audit, &stubInfraProvider{}, WithIdempotencyRepository(idempotencyRepo), WithCallbackRateLimiter(&stubCallbackRateLimiter{allowed: true}))
	require.NoError(t, err)

	err = uc.ProcessCallback(context.Background(), ProcessCallbackCommand{
		IdempotencyKey:  "",
		ExceptionID:     exception.ID,
		CallbackType:    "jira",
		ExternalSystem:  "JIRA",
		ExternalIssueID: "PROJ-123",
		Status:          "OPEN",
	})
	require.ErrorIs(t, err, shared.ErrEmptyIdempotencyKey)
	require.Equal(t, shared.ErrEmptyIdempotencyKey.Error(), err.Error())

	err = uc.ProcessCallback(context.Background(), ProcessCallbackCommand{
		IdempotencyKey:  "invalid key!",
		ExceptionID:     exception.ID,
		CallbackType:    "jira",
		ExternalSystem:  "JIRA",
		ExternalIssueID: "PROJ-123",
		Status:          "OPEN",
	})
	require.ErrorIs(t, err, shared.ErrInvalidIdempotencyKey)
	require.Equal(t, shared.ErrInvalidIdempotencyKey.Error(), err.Error())
}

func TestCallbackIdempotency_ExceptionIDRequired(t *testing.T) {
	t.Parallel()

	idempotencyRepo := &stubIdempotencyRepo{acquired: true}
	exceptionRepo := &stubExceptionRepo{}
	audit := &stubAuditPublisher{}

	uc, err := NewExceptionUseCase(exceptionRepo, actorExtractor("system"), audit, &stubInfraProvider{}, WithIdempotencyRepository(idempotencyRepo), WithCallbackRateLimiter(&stubCallbackRateLimiter{allowed: true}))
	require.NoError(t, err)

	err = uc.ProcessCallback(context.Background(), ProcessCallbackCommand{
		IdempotencyKey:  "valid-key",
		ExceptionID:     uuid.Nil,
		CallbackType:    "jira",
		ExternalSystem:  "JIRA",
		ExternalIssueID: "PROJ-123",
		Status:          "OPEN",
	})
	require.ErrorIs(t, err, ErrExceptionIDRequired)
}

func TestCallbackIdempotency_TryAcquireError(t *testing.T) {
	t.Parallel()

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	idempotencyRepo := &stubIdempotencyRepo{tryAcquireErr: errTestIdempotency}
	exceptionRepo := &stubExceptionRepo{exception: exception}
	audit := &stubAuditPublisher{}

	uc, err := NewExceptionUseCase(exceptionRepo, actorExtractor("system"), audit, &stubInfraProvider{}, WithIdempotencyRepository(idempotencyRepo), WithCallbackRateLimiter(&stubCallbackRateLimiter{allowed: true}))
	require.NoError(t, err)

	err = uc.ProcessCallback(context.Background(), ProcessCallbackCommand{
		IdempotencyKey:  "valid-key",
		ExceptionID:     exception.ID,
		CallbackType:    "jira",
		ExternalSystem:  "JIRA",
		ExternalIssueID: "PROJ-123",
		Status:          "OPEN",
	})
	require.ErrorIs(t, err, errTestIdempotency)
}

func TestCallbackIdempotency_AlreadyProcessing_ReturnsInProgress(t *testing.T) {
	t.Parallel()

	idempotencyRepo := &stubIdempotencyRepo{
		acquired: false,
		cachedResult: &shared.IdempotencyResult{
			Status: shared.IdempotencyStatusPending,
		},
	}
	exceptionRepo := &stubExceptionRepo{}
	audit := &stubAuditPublisher{}

	uc, err := NewExceptionUseCase(exceptionRepo, actorExtractor("system"), audit, &stubInfraProvider{}, WithIdempotencyRepository(idempotencyRepo), WithCallbackRateLimiter(&stubCallbackRateLimiter{allowed: true}))
	require.NoError(t, err)

	err = uc.ProcessCallback(context.Background(), ProcessCallbackCommand{
		IdempotencyKey:  "valid-key",
		ExceptionID:     uuid.New(),
		CallbackType:    "jira",
		ExternalSystem:  "JIRA",
		ExternalIssueID: "PROJ-123",
		Status:          "OPEN",
	})
	require.ErrorIs(t, err, ErrCallbackInProgress)
}

func TestCallbackIdempotency_PreviousFailure_ReturnsRetryable(t *testing.T) {
	t.Parallel()

	idempotencyRepo := &stubIdempotencyRepo{
		acquired: false,
		cachedResult: &shared.IdempotencyResult{
			Status: shared.IdempotencyStatusFailed,
		},
	}
	exceptionRepo := &stubExceptionRepo{}
	audit := &stubAuditPublisher{}

	uc, err := NewExceptionUseCase(exceptionRepo, actorExtractor("system"), audit, &stubInfraProvider{}, WithIdempotencyRepository(idempotencyRepo), WithCallbackRateLimiter(&stubCallbackRateLimiter{allowed: true}))
	require.NoError(t, err)

	err = uc.ProcessCallback(context.Background(), ProcessCallbackCommand{
		IdempotencyKey:  "valid-key",
		ExceptionID:     uuid.New(),
		CallbackType:    "jira",
		ExternalSystem:  "JIRA",
		ExternalIssueID: "PROJ-123",
		Status:          "OPEN",
	})
	require.ErrorIs(t, err, ErrCallbackInProgress)
	require.Equal(t, 1, idempotencyRepo.reacquireCalls)
}

func TestCallbackIdempotency_PreviousFailure_ReacquiresAndProcesses(t *testing.T) {
	t.Parallel()

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	idempotencyRepo := &stubIdempotencyRepo{
		acquired:   false,
		reacquired: true,
		cachedResult: &shared.IdempotencyResult{
			Status: shared.IdempotencyStatusFailed,
		},
	}
	exceptionRepo := &stubExceptionRepo{exception: exception}
	audit := &stubAuditPublisher{}

	uc, err := NewExceptionUseCase(exceptionRepo, actorExtractor("system"), audit, &stubInfraProvider{}, WithIdempotencyRepository(idempotencyRepo), WithCallbackRateLimiter(&stubCallbackRateLimiter{allowed: true}))
	require.NoError(t, err)

	err = uc.ProcessCallback(context.Background(), ProcessCallbackCommand{
		IdempotencyKey:  "jira:MATCH-123:callback",
		ExceptionID:     exception.ID,
		CallbackType:    "jira",
		ExternalSystem:  "JIRA",
		ExternalIssueID: "PROJ-123",
		Status:          "RESOLVED",
		ResolutionNotes: "Resolved by retry",
	})
	require.NoError(t, err)
	require.Equal(t, 1, idempotencyRepo.reacquireCalls)
	require.Equal(t, value_objects.ExceptionStatusResolved, exception.Status)
}

func TestCallbackIdempotency_PreviousFailure_ReacquireError(t *testing.T) {
	t.Parallel()

	idempotencyRepo := &stubIdempotencyRepo{
		acquired:     false,
		reacquireErr: errTestIdempotency,
		cachedResult: &shared.IdempotencyResult{
			Status: shared.IdempotencyStatusFailed,
		},
	}
	exceptionRepo := &stubExceptionRepo{}
	audit := &stubAuditPublisher{}

	uc, err := NewExceptionUseCase(exceptionRepo, actorExtractor("system"), audit, &stubInfraProvider{}, WithIdempotencyRepository(idempotencyRepo), WithCallbackRateLimiter(&stubCallbackRateLimiter{allowed: true}))
	require.NoError(t, err)

	err = uc.ProcessCallback(context.Background(), ProcessCallbackCommand{
		IdempotencyKey:  "valid-key",
		ExceptionID:     uuid.New(),
		CallbackType:    "jira",
		ExternalSystem:  "JIRA",
		ExternalIssueID: "PROJ-123",
		Status:          "OPEN",
	})
	require.ErrorIs(t, err, errTestIdempotency)
	require.Equal(t, 1, idempotencyRepo.reacquireCalls)
}

func TestCallbackIdempotency_GetCachedResultError(t *testing.T) {
	t.Parallel()

	idempotencyRepo := &stubIdempotencyRepo{acquired: false, getCachedErr: errTestIdempotency}
	exceptionRepo := &stubExceptionRepo{}
	audit := &stubAuditPublisher{}

	uc, err := NewExceptionUseCase(exceptionRepo, actorExtractor("system"), audit, &stubInfraProvider{}, WithIdempotencyRepository(idempotencyRepo), WithCallbackRateLimiter(&stubCallbackRateLimiter{allowed: true}))
	require.NoError(t, err)

	err = uc.ProcessCallback(context.Background(), ProcessCallbackCommand{
		IdempotencyKey:  "valid-key",
		ExceptionID:     uuid.New(),
		CallbackType:    "jira",
		ExternalSystem:  "JIRA",
		ExternalIssueID: "PROJ-123",
		Status:          "OPEN",
	})
	require.ErrorIs(t, err, errTestIdempotency)
}

func TestCallbackIdempotency_FindExceptionError(t *testing.T) {
	t.Parallel()

	idempotencyRepo := &stubIdempotencyRepo{acquired: true}
	exceptionRepo := &stubExceptionRepo{findErr: errTestFind}
	audit := &stubAuditPublisher{}

	uc, err := NewExceptionUseCase(exceptionRepo, actorExtractor("system"), audit, &stubInfraProvider{}, WithIdempotencyRepository(idempotencyRepo), WithCallbackRateLimiter(&stubCallbackRateLimiter{allowed: true}))
	require.NoError(t, err)

	err = uc.ProcessCallback(context.Background(), ProcessCallbackCommand{
		IdempotencyKey:  "valid-key",
		ExceptionID:     uuid.New(),
		CallbackType:    "jira",
		ExternalSystem:  "JIRA",
		ExternalIssueID: "PROJ-123",
		Status:          "OPEN",
	})
	require.ErrorIs(t, err, errTestFind)
	require.Equal(t, 1, idempotencyRepo.markFailedCalls)
}

func TestCallbackIdempotency_MarkCompleteError(t *testing.T) {
	t.Parallel()

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	idempotencyRepo := &stubIdempotencyRepo{acquired: true, markCompleteErr: errTestIdempotency}
	exceptionRepo := &stubExceptionRepo{exception: exception}
	audit := &stubAuditPublisher{}

	uc, err := NewExceptionUseCase(exceptionRepo, actorExtractor("system"), audit, &stubInfraProvider{}, WithIdempotencyRepository(idempotencyRepo), WithCallbackRateLimiter(&stubCallbackRateLimiter{allowed: true}))
	require.NoError(t, err)

	// MarkComplete failure after successful processing should NOT return error
	// (business operation succeeded, just idempotency completion failed - logged as warning)
	err = uc.ProcessCallback(context.Background(), ProcessCallbackCommand{
		IdempotencyKey:  "valid-key",
		ExceptionID:     exception.ID,
		CallbackType:    "jira",
		ExternalSystem:  "JIRA",
		ExternalIssueID: "PROJ-123",
		Status:          "OPEN",
	})
	require.NoError(
		t,
		err,
		"MarkComplete failure should not cause ProcessCallback to return error since business operation succeeded",
	)
	require.Equal(
		t,
		0,
		idempotencyRepo.markFailedCalls,
		"markIdempotencyFailed should not be called when MarkComplete fails after success",
	)
}

// TestNewCallbackUseCase_Validations verifies dependency validation for the
// callback path. The four required dependencies (exception repo, actor,
// audit, infra) are validated by the merged constructor; the two callback-
// specific dependencies (idempotency repo, rate limiter) are optional at
// construction time and their nil checks now live on ProcessCallback
// itself.
func TestNewCallbackUseCase_Validations(t *testing.T) {
	t.Parallel()

	idempotencyRepo := &stubIdempotencyRepo{}
	exceptionRepo := &stubExceptionRepo{}
	audit := &stubAuditPublisher{}
	actor := actorExtractor("system")
	infra := &stubInfraProvider{}
	rl := &stubCallbackRateLimiter{allowed: true}
	cmd := ProcessCallbackCommand{
		IdempotencyKey:  "valid-key",
		ExceptionID:     uuid.New(),
		ExternalSystem:  "JIRA",
		ExternalIssueID: "PROJ-123",
		Status:          "OPEN",
	}

	// Missing idempotency repository surfaces on ProcessCallback.
	uc, err := NewExceptionUseCase(exceptionRepo, actor, audit, infra, WithCallbackRateLimiter(rl))
	require.NoError(t, err)
	require.ErrorIs(t, uc.ProcessCallback(context.Background(), cmd), ErrNilIdempotencyRepository)

	// Missing rate limiter surfaces on ProcessCallback.
	uc, err = NewExceptionUseCase(exceptionRepo, actor, audit, infra, WithIdempotencyRepository(idempotencyRepo))
	require.NoError(t, err)
	require.ErrorIs(t, uc.ProcessCallback(context.Background(), cmd), ErrNilCallbackRateLimiter)

	// Required dependencies still fail at construction time.
	_, err = NewExceptionUseCase(nil, actor, audit, infra, WithIdempotencyRepository(idempotencyRepo), WithCallbackRateLimiter(rl))
	require.ErrorIs(t, err, ErrNilExceptionRepository)

	_, err = NewExceptionUseCase(exceptionRepo, actor, nil, infra, WithIdempotencyRepository(idempotencyRepo), WithCallbackRateLimiter(rl))
	require.ErrorIs(t, err, ErrNilAuditPublisher)

	_, err = NewExceptionUseCase(exceptionRepo, actor, audit, nil, WithIdempotencyRepository(idempotencyRepo), WithCallbackRateLimiter(rl))
	require.ErrorIs(t, err, ErrNilInfraProvider)
}

func TestProcessCallback_RateLimitKey_IsTenantScoped(t *testing.T) {
	t.Parallel()

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	idempotencyRepo := &stubIdempotencyRepo{acquired: true}
	exceptionRepo := &stubExceptionRepo{exception: exception}
	audit := &stubAuditPublisher{}
	rateLimiter := &stubCallbackRateLimiter{allowed: true}

	uc, err := NewExceptionUseCase(exceptionRepo, actorExtractor("system"), audit, &stubInfraProvider{}, WithIdempotencyRepository(idempotencyRepo), WithCallbackRateLimiter(rateLimiter))
	require.NoError(t, err)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "tenant-A")

	err = uc.ProcessCallback(ctx, ProcessCallbackCommand{
		IdempotencyKey:  "jira:MATCH-123:callback",
		ExceptionID:     exception.ID,
		CallbackType:    "jira",
		ExternalSystem:  "JIRA",
		ExternalIssueID: "PROJ-123",
		Status:          "RESOLVED",
	})
	require.NoError(t, err)
	require.Equal(t, "JIRA", rateLimiter.lastKey)
}

func TestProcessCallback_AllCallbackTypes(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		callbackType string
	}{
		{name: "Jira", callbackType: "jira"},
		{name: "Webhook", callbackType: "webhook"},
		{name: "ServiceNow", callbackType: "servicenow"},
		{name: "Custom", callbackType: "custom"},
		{name: "Email", callbackType: "email"},
		{name: "Slack", callbackType: "slack"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			exception, err := entities.NewException(
				context.Background(),
				uuid.New(),
				sharedexception.ExceptionSeverityHigh,
				nil,
			)
			require.NoError(t, err)

			idempotencyRepo := &stubIdempotencyRepo{acquired: true}
			exceptionRepo := &stubExceptionRepo{exception: exception}
			audit := &stubAuditPublisher{}

			uc, err := NewExceptionUseCase(exceptionRepo, actorExtractor("system"), audit, &stubInfraProvider{}, WithIdempotencyRepository(idempotencyRepo), WithCallbackRateLimiter(&stubCallbackRateLimiter{allowed: true}))
			require.NoError(t, err)

			err = uc.ProcessCallback(context.Background(), ProcessCallbackCommand{
				IdempotencyKey:  tc.callbackType + ":MATCH-123:callback",
				ExceptionID:     exception.ID,
				CallbackType:    tc.callbackType,
				ExternalSystem:  strings.ToUpper(tc.callbackType),
				ExternalIssueID: "EXT-123",
				Status:          "OPEN",
				Payload:         map[string]any{"status": "Done"},
			})
			require.NoError(t, err)

			require.NotNil(t, audit.lastEvent)
			require.Equal(t, "CALLBACK_PROCESSED", audit.lastEvent.Action)
			require.Equal(t, "system", audit.lastEvent.Actor)
			require.Equal(t, tc.callbackType, audit.lastEvent.Metadata["callback_type"])
		})
	}
}

func TestProcessCallback_PayloadVariations(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		payload map[string]any
	}{
		{name: "NilPayload", payload: nil},
		{name: "EmptyPayload", payload: map[string]any{}},
		{name: "SimplePayload", payload: map[string]any{"status": "Done"}},
		{name: "ComplexPayload", payload: map[string]any{
			"status":      "Done",
			"resolution":  "Fixed",
			"assignee":    "analyst-1",
			"timestamp":   "2026-01-20T10:00:00Z",
			"priority":    1,
			"isUrgent":    true,
			"amount":      123.45,
			"tags":        []string{"urgent", "billing"},
			"nestedField": map[string]any{"key": "value", "nested": map[string]any{"deep": true}},
		}},
		{name: "NestedArrayPayload", payload: map[string]any{
			"items": []map[string]any{
				{"id": 1, "name": "item1"},
				{"id": 2, "name": "item2"},
			},
		}},
		{name: "SpecialCharactersPayload", payload: map[string]any{
			"message": "Special chars: @#$%^&*(){}[]|\\:\";<>?,./",
			"unicode": "日本語テスト",
		}},
		{name: "LargePayload", payload: func() map[string]any {
			p := make(map[string]any)
			for i := 0; i < 100; i++ {
				p[uuid.New().String()] = uuid.New().String()
			}

			return p
		}()},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			exception, err := entities.NewException(
				context.Background(),
				uuid.New(),
				sharedexception.ExceptionSeverityHigh,
				nil,
			)
			require.NoError(t, err)

			idempotencyRepo := &stubIdempotencyRepo{acquired: true}
			exceptionRepo := &stubExceptionRepo{exception: exception}
			audit := &stubAuditPublisher{}

			uc, err := NewExceptionUseCase(exceptionRepo, actorExtractor("system"), audit, &stubInfraProvider{}, WithIdempotencyRepository(idempotencyRepo), WithCallbackRateLimiter(&stubCallbackRateLimiter{allowed: true}))
			require.NoError(t, err)

			idempotencyKey := "webhook:" + uuid.New().String() + ":callback"
			err = uc.ProcessCallback(context.Background(), ProcessCallbackCommand{
				IdempotencyKey:  idempotencyKey,
				ExceptionID:     exception.ID,
				CallbackType:    "webhook",
				ExternalSystem:  "WEBHOOK",
				ExternalIssueID: "EXT-999",
				Status:          "OPEN",
				Payload:         tc.payload,
			})
			require.NoError(t, err)

			require.NotNil(t, audit.lastEvent)
			require.Equal(t, "CALLBACK_PROCESSED", audit.lastEvent.Action)
		})
	}
}
