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
)

var errTestIdempotency = errors.New("test: idempotency failed")

type stubIdempotencyRepo struct {
	acquired        bool
	tryAcquireErr   error
	markCompleteErr error
	markFailedErr   error
	markFailedCalls int
	cachedResult    *value_objects.IdempotencyResult
	getCachedErr    error
}

func (repo *stubIdempotencyRepo) TryAcquire(
	_ context.Context,
	_ value_objects.IdempotencyKey,
) (bool, error) {
	if repo.tryAcquireErr != nil {
		return false, repo.tryAcquireErr
	}

	return repo.acquired, nil
}

func (repo *stubIdempotencyRepo) TryReacquireFromFailed(
	_ context.Context,
	_ value_objects.IdempotencyKey,
) (bool, error) {
	return false, nil
}

func (repo *stubIdempotencyRepo) MarkComplete(
	_ context.Context,
	_ value_objects.IdempotencyKey,
	_ []byte,
	_ int,
) error {
	return repo.markCompleteErr
}

func (repo *stubIdempotencyRepo) MarkFailed(
	_ context.Context,
	_ value_objects.IdempotencyKey,
) error {
	repo.markFailedCalls++
	return repo.markFailedErr
}

func (repo *stubIdempotencyRepo) GetCachedResult(
	_ context.Context,
	_ value_objects.IdempotencyKey,
) (*value_objects.IdempotencyResult, error) {
	if repo.getCachedErr != nil {
		return nil, repo.getCachedErr
	}

	if repo.cachedResult != nil {
		return repo.cachedResult, nil
	}

	return &value_objects.IdempotencyResult{Status: value_objects.IdempotencyStatusComplete}, nil
}

func TestCallbackIdempotency_FirstCall_Processes(t *testing.T) {
	t.Parallel()

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	idempotencyRepo := &stubIdempotencyRepo{acquired: true}
	exceptionRepo := &stubExceptionRepo{exception: exception}
	audit := &stubAuditPublisher{}

	uc, err := NewCallbackUseCase(idempotencyRepo, exceptionRepo, audit, &stubInfraProvider{}, &stubCallbackRateLimiter{allowed: true})
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
	parsedKey, parseErr := value_objects.ParseIdempotencyKey("jira:MATCH-123:callback")
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
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	idempotencyRepo := &stubIdempotencyRepo{acquired: true}
	exceptionRepo := &stubExceptionRepo{exception: exception}
	audit := &stubAuditPublisher{}

	uc, err := NewCallbackUseCase(idempotencyRepo, exceptionRepo, audit, &stubInfraProvider{}, &stubCallbackRateLimiter{allowed: true})
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
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	idempotencyRepo := &stubIdempotencyRepo{acquired: true}
	exceptionRepo := &stubExceptionRepo{exception: exception}
	audit := &stubAuditPublisher{}

	uc, err := NewCallbackUseCase(idempotencyRepo, exceptionRepo, audit, &stubInfraProvider{}, &stubCallbackRateLimiter{allowed: true})
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
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	idempotencyRepo := &stubIdempotencyRepo{acquired: true}
	exceptionRepo := &stubExceptionRepo{exception: exception, updateErr: errTestUpdate}
	audit := &stubAuditPublisher{}

	uc, err := NewCallbackUseCase(idempotencyRepo, exceptionRepo, audit, &stubInfraProvider{}, &stubCallbackRateLimiter{allowed: true})
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
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	idempotencyRepo := &stubIdempotencyRepo{acquired: false}
	exceptionRepo := &stubExceptionRepo{exception: exception}
	audit := &stubAuditPublisher{}

	uc, err := NewCallbackUseCase(idempotencyRepo, exceptionRepo, audit, &stubInfraProvider{}, &stubCallbackRateLimiter{allowed: true})
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
	parsedKey, parseErr := value_objects.ParseIdempotencyKey("jira:MATCH-123:callback")
	require.NoError(t, parseErr)
	require.Equal(t, idempotencyKeyHash(parsedKey), audit.lastEvent.Metadata["idempotency_key_hash"])
	require.Empty(t, audit.lastEvent.Metadata["idempotency_key"])
}

func TestCallbackIdempotency_InvalidKey_Rejected(t *testing.T) {
	t.Parallel()

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	idempotencyRepo := &stubIdempotencyRepo{acquired: true}
	exceptionRepo := &stubExceptionRepo{exception: exception}
	audit := &stubAuditPublisher{}

	uc, err := NewCallbackUseCase(idempotencyRepo, exceptionRepo, audit, &stubInfraProvider{}, &stubCallbackRateLimiter{allowed: true})
	require.NoError(t, err)

	err = uc.ProcessCallback(context.Background(), ProcessCallbackCommand{
		IdempotencyKey:  "",
		ExceptionID:     exception.ID,
		CallbackType:    "jira",
		ExternalSystem:  "JIRA",
		ExternalIssueID: "PROJ-123",
		Status:          "OPEN",
	})
	require.ErrorIs(t, err, value_objects.ErrEmptyIdempotencyKey)

	err = uc.ProcessCallback(context.Background(), ProcessCallbackCommand{
		IdempotencyKey:  "invalid key!",
		ExceptionID:     exception.ID,
		CallbackType:    "jira",
		ExternalSystem:  "JIRA",
		ExternalIssueID: "PROJ-123",
		Status:          "OPEN",
	})
	require.ErrorIs(t, err, value_objects.ErrInvalidIdempotencyKey)
}

func TestCallbackIdempotency_ExceptionIDRequired(t *testing.T) {
	t.Parallel()

	idempotencyRepo := &stubIdempotencyRepo{acquired: true}
	exceptionRepo := &stubExceptionRepo{}
	audit := &stubAuditPublisher{}

	uc, err := NewCallbackUseCase(idempotencyRepo, exceptionRepo, audit, &stubInfraProvider{}, &stubCallbackRateLimiter{allowed: true})
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
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	idempotencyRepo := &stubIdempotencyRepo{tryAcquireErr: errTestIdempotency}
	exceptionRepo := &stubExceptionRepo{exception: exception}
	audit := &stubAuditPublisher{}

	uc, err := NewCallbackUseCase(idempotencyRepo, exceptionRepo, audit, &stubInfraProvider{}, &stubCallbackRateLimiter{allowed: true})
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
		cachedResult: &value_objects.IdempotencyResult{
			Status: value_objects.IdempotencyStatusPending,
		},
	}
	exceptionRepo := &stubExceptionRepo{}
	audit := &stubAuditPublisher{}

	uc, err := NewCallbackUseCase(idempotencyRepo, exceptionRepo, audit, &stubInfraProvider{}, &stubCallbackRateLimiter{allowed: true})
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
		cachedResult: &value_objects.IdempotencyResult{
			Status: value_objects.IdempotencyStatusFailed,
		},
	}
	exceptionRepo := &stubExceptionRepo{}
	audit := &stubAuditPublisher{}

	uc, err := NewCallbackUseCase(idempotencyRepo, exceptionRepo, audit, &stubInfraProvider{}, &stubCallbackRateLimiter{allowed: true})
	require.NoError(t, err)

	err = uc.ProcessCallback(context.Background(), ProcessCallbackCommand{
		IdempotencyKey:  "valid-key",
		ExceptionID:     uuid.New(),
		CallbackType:    "jira",
		ExternalSystem:  "JIRA",
		ExternalIssueID: "PROJ-123",
		Status:          "OPEN",
	})
	require.ErrorIs(t, err, ErrCallbackRetryable)
}

func TestCallbackIdempotency_GetCachedResultError(t *testing.T) {
	t.Parallel()

	idempotencyRepo := &stubIdempotencyRepo{acquired: false, getCachedErr: errTestIdempotency}
	exceptionRepo := &stubExceptionRepo{}
	audit := &stubAuditPublisher{}

	uc, err := NewCallbackUseCase(idempotencyRepo, exceptionRepo, audit, &stubInfraProvider{}, &stubCallbackRateLimiter{allowed: true})
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

	uc, err := NewCallbackUseCase(idempotencyRepo, exceptionRepo, audit, &stubInfraProvider{}, &stubCallbackRateLimiter{allowed: true})
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
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	idempotencyRepo := &stubIdempotencyRepo{acquired: true, markCompleteErr: errTestIdempotency}
	exceptionRepo := &stubExceptionRepo{exception: exception}
	audit := &stubAuditPublisher{}

	uc, err := NewCallbackUseCase(idempotencyRepo, exceptionRepo, audit, &stubInfraProvider{}, &stubCallbackRateLimiter{allowed: true})
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

func TestNewCallbackUseCase_Validations(t *testing.T) {
	t.Parallel()

	idempotencyRepo := &stubIdempotencyRepo{}
	exceptionRepo := &stubExceptionRepo{}
	audit := &stubAuditPublisher{}

	infra := &stubInfraProvider{}

	rl := &stubCallbackRateLimiter{allowed: true}

	_, err := NewCallbackUseCase(nil, exceptionRepo, audit, infra, rl)
	require.ErrorIs(t, err, ErrNilIdempotencyRepository)

	_, err = NewCallbackUseCase(idempotencyRepo, nil, audit, infra, rl)
	require.ErrorIs(t, err, ErrNilExceptionRepository)

	_, err = NewCallbackUseCase(idempotencyRepo, exceptionRepo, nil, infra, rl)
	require.ErrorIs(t, err, ErrNilAuditPublisher)

	_, err = NewCallbackUseCase(idempotencyRepo, exceptionRepo, audit, nil, rl)
	require.ErrorIs(t, err, ErrNilInfraProvider)

	_, err = NewCallbackUseCase(idempotencyRepo, exceptionRepo, audit, infra, nil)
	require.ErrorIs(t, err, ErrNilCallbackRateLimiter)
}

func TestProcessCallback_RateLimitKey_IsTenantScoped(t *testing.T) {
	t.Parallel()

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	idempotencyRepo := &stubIdempotencyRepo{acquired: true}
	exceptionRepo := &stubExceptionRepo{exception: exception}
	audit := &stubAuditPublisher{}
	rateLimiter := &stubCallbackRateLimiter{allowed: true}

	uc, err := NewCallbackUseCase(idempotencyRepo, exceptionRepo, audit, &stubInfraProvider{}, rateLimiter)
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
				value_objects.ExceptionSeverityHigh,
				nil,
			)
			require.NoError(t, err)

			idempotencyRepo := &stubIdempotencyRepo{acquired: true}
			exceptionRepo := &stubExceptionRepo{exception: exception}
			audit := &stubAuditPublisher{}

			uc, err := NewCallbackUseCase(idempotencyRepo, exceptionRepo, audit, &stubInfraProvider{}, &stubCallbackRateLimiter{allowed: true})
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
				value_objects.ExceptionSeverityHigh,
				nil,
			)
			require.NoError(t, err)

			idempotencyRepo := &stubIdempotencyRepo{acquired: true}
			exceptionRepo := &stubExceptionRepo{exception: exception}
			audit := &stubAuditPublisher{}

			uc, err := NewCallbackUseCase(idempotencyRepo, exceptionRepo, audit, &stubInfraProvider{}, &stubCallbackRateLimiter{allowed: true})
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
