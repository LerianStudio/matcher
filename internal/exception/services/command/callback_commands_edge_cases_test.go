//go:build unit

package command

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
)

// Test payload fallback edge cases.
func TestProcessCallback_PayloadFallback_ExternalSystem(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name                  string
		externalSystem        string
		callbackType          string
		payload               map[string]any
		expectedExternalError bool
	}{
		{
			name:                  "external system from direct field",
			externalSystem:        "JIRA",
			callbackType:          "",
			payload:               nil,
			expectedExternalError: false,
		},
		{
			name:                  "external system from callback type",
			externalSystem:        "",
			callbackType:          "webhook",
			payload:               nil,
			expectedExternalError: false,
		},
		{
			name:           "external system from payload snake_case",
			externalSystem: "",
			callbackType:   "",
			payload: map[string]any{
				"external_system": "SERVICENOW",
			},
			expectedExternalError: false,
		},
		{
			name:           "external system from payload camelCase",
			externalSystem: "",
			callbackType:   "",
			payload: map[string]any{
				"externalSystem": "MANUAL",
			},
			expectedExternalError: false,
		},
		{
			name:                  "external system empty - error",
			externalSystem:        "",
			callbackType:          "",
			payload:               nil,
			expectedExternalError: true,
		},
		{
			name:                  "external system whitespace only - error",
			externalSystem:        "   ",
			callbackType:          "   ",
			payload:               nil,
			expectedExternalError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			exception, err := entities.NewException(
				ctx,
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

			err = uc.ProcessCallback(ctx, ProcessCallbackCommand{
				IdempotencyKey:  "test:" + uuid.New().String() + ":callback",
				ExceptionID:     exception.ID,
				CallbackType:    tc.callbackType,
				ExternalSystem:  tc.externalSystem,
				ExternalIssueID: "EXT-123",
				Status:          "OPEN",
				Payload:         tc.payload,
			})

			if tc.expectedExternalError {
				require.ErrorIs(t, err, ErrCallbackExternalSystem)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestProcessCallback_PayloadFallback_ExternalIssueID(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name                    string
		externalIssueID         string
		payload                 map[string]any
		expectedExternalIDError bool
	}{
		{
			name:                    "external issue id from direct field",
			externalIssueID:         "PROJ-123",
			payload:                 nil,
			expectedExternalIDError: false,
		},
		{
			name:            "external issue id from payload snake_case",
			externalIssueID: "",
			payload: map[string]any{
				"external_issue_id": "PROJ-456",
			},
			expectedExternalIDError: false,
		},
		{
			name:            "external issue id from payload camelCase",
			externalIssueID: "",
			payload: map[string]any{
				"externalIssueID": "PROJ-789",
			},
			expectedExternalIDError: false,
		},
		{
			name:                    "external issue id empty - error",
			externalIssueID:         "",
			payload:                 nil,
			expectedExternalIDError: true,
		},
		{
			name:                    "external issue id whitespace only - error",
			externalIssueID:         "   ",
			payload:                 nil,
			expectedExternalIDError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			exception, err := entities.NewException(
				ctx,
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

			err = uc.ProcessCallback(ctx, ProcessCallbackCommand{
				IdempotencyKey:  "test:" + uuid.New().String() + ":callback",
				ExceptionID:     exception.ID,
				CallbackType:    "webhook",
				ExternalSystem:  "WEBHOOK",
				ExternalIssueID: tc.externalIssueID,
				Status:          "OPEN",
				Payload:         tc.payload,
			})

			if tc.expectedExternalIDError {
				require.ErrorIs(t, err, ErrCallbackExternalIssueID)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestProcessCallback_PayloadFallback_Status(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name                string
		status              string
		payload             map[string]any
		expectedStatusError bool
	}{
		{
			name:                "status from direct field",
			status:              "OPEN",
			payload:             nil,
			expectedStatusError: false,
		},
		{
			name:   "status from payload",
			status: "",
			payload: map[string]any{
				"status": "OPEN",
			},
			expectedStatusError: false,
		},
		{
			name:                "status empty - error",
			status:              "",
			payload:             nil,
			expectedStatusError: true,
		},
		{
			name:                "status whitespace only - error",
			status:              "   ",
			payload:             nil,
			expectedStatusError: true,
		},
		{
			name:                "invalid status",
			status:              "INVALID_STATUS",
			payload:             nil,
			expectedStatusError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			exception, err := entities.NewException(
				ctx,
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

			err = uc.ProcessCallback(ctx, ProcessCallbackCommand{
				IdempotencyKey:  "test:" + uuid.New().String() + ":callback",
				ExceptionID:     exception.ID,
				CallbackType:    "webhook",
				ExternalSystem:  "WEBHOOK",
				ExternalIssueID: "EXT-123",
				Status:          tc.status,
				Payload:         tc.payload,
			})

			if tc.expectedStatusError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestProcessCallback_PayloadTime_Parsing(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	testCases := []struct {
		name        string
		dueAt       *time.Time
		updatedAt   *time.Time
		payload     map[string]any
		expectError bool
	}{
		{
			name:        "due_at from direct field",
			dueAt:       &now,
			updatedAt:   nil,
			payload:     nil,
			expectError: false,
		},
		{
			name:      "due_at from payload as time.Time",
			dueAt:     nil,
			updatedAt: nil,
			payload: map[string]any{
				"due_at": now,
			},
			expectError: false,
		},
		{
			name:      "due_at from payload as *time.Time",
			dueAt:     nil,
			updatedAt: nil,
			payload: map[string]any{
				"due_at": &now,
			},
			expectError: false,
		},
		{
			name:      "due_at from payload as nil *time.Time",
			dueAt:     nil,
			updatedAt: nil,
			payload: map[string]any{
				"due_at": (*time.Time)(nil),
			},
			expectError: false,
		},
		{
			name:      "due_at from payload as RFC3339 string",
			dueAt:     nil,
			updatedAt: nil,
			payload: map[string]any{
				"due_at": "2026-01-20T10:00:00Z",
			},
			expectError: false,
		},
		{
			name:      "due_at from payload as empty string",
			dueAt:     nil,
			updatedAt: nil,
			payload: map[string]any{
				"due_at": "",
			},
			expectError: false,
		},
		{
			name:      "due_at from payload as whitespace string",
			dueAt:     nil,
			updatedAt: nil,
			payload: map[string]any{
				"due_at": "   ",
			},
			expectError: false,
		},
		{
			name:      "due_at from payload as invalid date format",
			dueAt:     nil,
			updatedAt: nil,
			payload: map[string]any{
				"due_at": "2026/01/20 10:00:00",
			},
			expectError: true,
		},
		{
			name:      "dueAt camelCase key",
			dueAt:     nil,
			updatedAt: nil,
			payload: map[string]any{
				"dueAt": "2026-01-20T10:00:00Z",
			},
			expectError: false,
		},
		{
			name:        "updated_at from direct field",
			dueAt:       nil,
			updatedAt:   &now,
			payload:     nil,
			expectError: false,
		},
		{
			name:      "updated_at from payload as RFC3339 string",
			dueAt:     nil,
			updatedAt: nil,
			payload: map[string]any{
				"updated_at": "2026-01-20T10:00:00Z",
			},
			expectError: false,
		},
		{
			name:      "updatedAt camelCase key",
			dueAt:     nil,
			updatedAt: nil,
			payload: map[string]any{
				"updatedAt": "2026-01-20T10:00:00Z",
			},
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			exception, err := entities.NewException(
				ctx,
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

			err = uc.ProcessCallback(ctx, ProcessCallbackCommand{
				IdempotencyKey:  "test:" + uuid.New().String() + ":callback",
				ExceptionID:     exception.ID,
				CallbackType:    "webhook",
				ExternalSystem:  "WEBHOOK",
				ExternalIssueID: "EXT-123",
				Status:          "OPEN",
				DueAt:           tc.dueAt,
				UpdatedAt:       tc.updatedAt,
				Payload:         tc.payload,
			})

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestProcessCallback_DuplicateCallback_AuditError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	exception, err := entities.NewException(
		ctx,
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	idempotencyRepo := &stubIdempotencyRepo{acquired: false}
	exceptionRepo := &stubExceptionRepo{exception: exception}
	audit := &stubAuditPublisher{err: errTestAudit}

	uc, err := NewCallbackUseCase(idempotencyRepo, exceptionRepo, audit, &stubInfraProvider{}, &stubCallbackRateLimiter{allowed: true})
	require.NoError(t, err)

	err = uc.ProcessCallback(ctx, ProcessCallbackCommand{
		IdempotencyKey:  "test:callback:key",
		ExceptionID:     exception.ID,
		CallbackType:    "webhook",
		ExternalSystem:  "WEBHOOK",
		ExternalIssueID: "EXT-123",
		Status:          "OPEN",
	})

	require.ErrorIs(t, err, errTestAudit)
	assert.Contains(t, err.Error(), "publish duplicate audit")
}

func TestProcessCallback_AuditPublishError_MarksIdempotencyFailed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	exception, err := entities.NewException(
		ctx,
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	idempotencyRepo := &stubIdempotencyRepo{acquired: true}
	exceptionRepo := &stubExceptionRepo{exception: exception}
	audit := &stubAuditPublisher{err: errTestAudit}

	uc, err := NewCallbackUseCase(idempotencyRepo, exceptionRepo, audit, &stubInfraProvider{}, &stubCallbackRateLimiter{allowed: true})
	require.NoError(t, err)

	err = uc.ProcessCallback(ctx, ProcessCallbackCommand{
		IdempotencyKey:  "test:callback:key",
		ExceptionID:     exception.ID,
		CallbackType:    "webhook",
		ExternalSystem:  "WEBHOOK",
		ExternalIssueID: "EXT-123",
		Status:          "OPEN",
	})

	require.ErrorIs(t, err, errTestAudit)
	assert.Equal(t, 1, idempotencyRepo.markFailedCalls)
}

func TestProcessCallback_SameStatus_NoTransition(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	exception, err := entities.NewException(
		ctx,
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

	// Exception is in OPEN status by default, and we're updating to OPEN
	err = uc.ProcessCallback(ctx, ProcessCallbackCommand{
		IdempotencyKey:  "test:callback:key",
		ExceptionID:     exception.ID,
		CallbackType:    "webhook",
		ExternalSystem:  "WEBHOOK",
		ExternalIssueID: "EXT-123",
		Status:          "OPEN",
	})

	require.NoError(t, err)
	assert.Equal(t, value_objects.ExceptionStatusOpen, exception.Status)
}

func TestProcessCallback_UnsupportedStatusTransition(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	exception, err := entities.NewException(
		ctx,
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	// Set exception to resolved status to test invalid transitions
	exception.Status = value_objects.ExceptionStatusResolved

	idempotencyRepo := &stubIdempotencyRepo{acquired: true}
	exceptionRepo := &stubExceptionRepo{exception: exception}
	audit := &stubAuditPublisher{}

	uc, err := NewCallbackUseCase(idempotencyRepo, exceptionRepo, audit, &stubInfraProvider{}, &stubCallbackRateLimiter{allowed: true})
	require.NoError(t, err)

	// Try to transition from Resolved back to Open
	err = uc.ProcessCallback(ctx, ProcessCallbackCommand{
		IdempotencyKey:  "test:callback:key",
		ExceptionID:     exception.ID,
		CallbackType:    "webhook",
		ExternalSystem:  "WEBHOOK",
		ExternalIssueID: "EXT-123",
		Status:          "OPEN",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "validate resolution transition")
	assert.Equal(t, 1, idempotencyRepo.markFailedCalls)
}

func TestProcessCallback_ResolutionNotes_WithPayload(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	exception, err := entities.NewException(
		ctx,
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

	err = uc.ProcessCallback(ctx, ProcessCallbackCommand{
		IdempotencyKey:  "test:callback:key",
		ExceptionID:     exception.ID,
		CallbackType:    "webhook",
		ExternalSystem:  "WEBHOOK",
		ExternalIssueID: "EXT-123",
		Status:          "RESOLVED",
		Payload: map[string]any{
			"resolution_notes": "Resolved via automated process",
		},
	})

	require.NoError(t, err)
	assert.Equal(t, value_objects.ExceptionStatusResolved, exception.Status)
}

func TestProcessCallback_ResolutionNotes_DefaultGenerated(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	exception, err := entities.NewException(
		ctx,
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

	err = uc.ProcessCallback(ctx, ProcessCallbackCommand{
		IdempotencyKey:  "test:callback:key",
		ExceptionID:     exception.ID,
		CallbackType:    "webhook",
		ExternalSystem:  "JIRA",
		ExternalIssueID: "EXT-123",
		Status:          "RESOLVED",
		// No resolution notes provided
	})

	require.NoError(t, err)
	assert.Equal(t, value_objects.ExceptionStatusResolved, exception.Status)
}

func TestProcessCallback_AssignedStatus_WithAssignee(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	exception, err := entities.NewException(
		ctx,
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

	dueAt := time.Now().UTC().Add(24 * time.Hour)
	err = uc.ProcessCallback(ctx, ProcessCallbackCommand{
		IdempotencyKey:  "test:callback:key",
		ExceptionID:     exception.ID,
		CallbackType:    "webhook",
		ExternalSystem:  "WEBHOOK",
		ExternalIssueID: "EXT-123",
		Status:          "ASSIGNED",
		Assignee:        "analyst-1",
		DueAt:           &dueAt,
	})

	require.NoError(t, err)
	assert.Equal(t, value_objects.ExceptionStatusAssigned, exception.Status)
	require.NotNil(t, exception.AssignedTo)
	assert.Equal(t, "analyst-1", *exception.AssignedTo)
}

func TestProcessCallback_CallbackTypeEmptyUsesExternalSystem(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	exception, err := entities.NewException(
		ctx,
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

	err = uc.ProcessCallback(ctx, ProcessCallbackCommand{
		IdempotencyKey:  "test:callback:key",
		ExceptionID:     exception.ID,
		CallbackType:    "",
		ExternalSystem:  "SERVICENOW",
		ExternalIssueID: "INC-123",
		Status:          "OPEN",
	})

	require.NoError(t, err)
	require.NotNil(t, audit.lastEvent)
	assert.Equal(t, "SERVICENOW", audit.lastEvent.Metadata["callback_type"])
}

func TestProcessCallback_PayloadStringWithStringer(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	exception, err := entities.NewException(
		ctx,
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

	// Use a uuid.UUID which implements fmt.Stringer
	err = uc.ProcessCallback(ctx, ProcessCallbackCommand{
		IdempotencyKey:  "test:callback:key",
		ExceptionID:     exception.ID,
		CallbackType:    "webhook",
		ExternalSystem:  "WEBHOOK",
		ExternalIssueID: "EXT-123",
		Status:          "OPEN",
		Payload: map[string]any{
			"stringer_value": uuid.New(),
		},
	})

	require.NoError(t, err)
}

func TestProcessCallback_AuditMetadataIncludes_DueAtAndUpdatedAt(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	exception, err := entities.NewException(
		ctx,
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

	dueAt := time.Date(2026, 2, 1, 10, 0, 0, 0, time.UTC)
	updatedAt := time.Date(2026, 1, 30, 15, 30, 0, 0, time.UTC)

	err = uc.ProcessCallback(ctx, ProcessCallbackCommand{
		IdempotencyKey:  "test:callback:key",
		ExceptionID:     exception.ID,
		CallbackType:    "webhook",
		ExternalSystem:  "WEBHOOK",
		ExternalIssueID: "EXT-123",
		Status:          "ASSIGNED",
		Assignee:        "analyst-1",
		DueAt:           &dueAt,
		UpdatedAt:       &updatedAt,
	})

	require.NoError(t, err)
	require.NotNil(t, audit.lastEvent)
	assert.Equal(t, "2026-02-01T10:00:00Z", audit.lastEvent.Metadata["due_at"])
	assert.Equal(t, "2026-01-30T15:30:00Z", audit.lastEvent.Metadata["updated_at"])
	assert.Equal(t, "analyst-1", audit.lastEvent.Metadata["assignee"])
}

func TestProcessCallback_ApplyCallback_NilException(t *testing.T) {
	t.Parallel()

	uc := &CallbackUseCase{
		idempotencyRepo: &stubIdempotencyRepo{},
		exceptionRepo:   &stubExceptionRepo{},
		auditPublisher:  &stubAuditPublisher{},
	}

	ctx := context.Background()
	params := &callbackParams{
		externalSystem:  "JIRA",
		externalIssueID: "PROJ-123",
		status:          value_objects.ExceptionStatusOpen,
	}

	err := uc.applyCallback(ctx, nil, params)

	require.ErrorIs(t, err, entities.ErrExceptionNil)
}

func TestProcessCallback_MarkIdempotencyFailed_LogsError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	exception, err := entities.NewException(
		ctx,
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	idempotencyRepo := &stubIdempotencyRepo{
		acquired:      true,
		markFailedErr: errTestIdempotency,
	}
	exceptionRepo := &stubExceptionRepo{exception: exception, findErr: errTestFind}
	audit := &stubAuditPublisher{}

	uc, err := NewCallbackUseCase(idempotencyRepo, exceptionRepo, audit, &stubInfraProvider{}, &stubCallbackRateLimiter{allowed: true})
	require.NoError(t, err)

	err = uc.ProcessCallback(ctx, ProcessCallbackCommand{
		IdempotencyKey:  "test:callback:key",
		ExceptionID:     exception.ID,
		CallbackType:    "webhook",
		ExternalSystem:  "WEBHOOK",
		ExternalIssueID: "EXT-123",
		Status:          "OPEN",
	})

	require.ErrorIs(t, err, errTestFind)
	assert.Equal(t, 1, idempotencyRepo.markFailedCalls)
}

func TestProcessCallback_OpenNotValidTargetFromPendingResolution(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	exception, err := entities.NewException(
		ctx,
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	// Put exception in PENDING_RESOLUTION status.
	// The transition table allows PENDING_RESOLUTION -> OPEN,
	// but callbacks should never drive an exception back to OPEN.
	exception.Status = value_objects.ExceptionStatusPendingResolution

	idempotencyRepo := &stubIdempotencyRepo{acquired: true}
	exceptionRepo := &stubExceptionRepo{exception: exception}
	audit := &stubAuditPublisher{}

	uc, err := NewCallbackUseCase(idempotencyRepo, exceptionRepo, audit, &stubInfraProvider{}, &stubCallbackRateLimiter{allowed: true})
	require.NoError(t, err)

	err = uc.ProcessCallback(ctx, ProcessCallbackCommand{
		IdempotencyKey:  "test:callback:key",
		ExceptionID:     exception.ID,
		CallbackType:    "webhook",
		ExternalSystem:  "WEBHOOK",
		ExternalIssueID: "EXT-123",
		Status:          "OPEN",
	})

	require.ErrorIs(t, err, ErrCallbackOpenNotValidTarget)
	assert.Contains(t, err.Error(), "OPEN is not a valid callback resolution target")
	assert.Equal(t, 1, idempotencyRepo.markFailedCalls)
}
