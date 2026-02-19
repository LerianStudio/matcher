//go:build unit

package connectors

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/exception/domain/services"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
)

func TestBuildWebhookPayload_HappyPath(t *testing.T) {
	t.Parallel()

	exceptionID := uuid.New()
	transactionID := uuid.New()
	eventID := uuid.New()
	createdAt := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	dueAt := time.Date(2024, 1, 20, 10, 30, 0, 0, time.UTC)
	timestamp := time.Date(2024, 1, 16, 14, 0, 0, 0, time.UTC)

	ctx := &DispatchContext{
		Snapshot: ExceptionSnapshot{
			ID:            exceptionID,
			TransactionID: transactionID,
			Severity:      value_objects.ExceptionSeverityHigh,
			Status:        value_objects.ExceptionStatusOpen,
			Amount:        decimal.NewFromInt(25000),
			Currency:      "USD",
			Reason:        "Duplicate transaction",
			SourceType:    "BANK_STATEMENT",
			CreatedAt:     createdAt,
			DueAt:         &dueAt,
		},
		Decision: services.RoutingDecision{
			Target:   services.RoutingTargetWebhook,
			Queue:    "webhook-queue",
			Assignee: "ops-team",
			RuleName: "high-amount-webhook-rule",
		},
		Timestamp: timestamp,
		TraceID:   "trace-webhook-123",
	}

	cfg := WebhookConfig{
		EventType: "exception.created",
	}

	payload, err := BuildWebhookPayload(ctx, cfg, eventID)

	require.NoError(t, err)
	require.NotNil(t, payload)
	require.Equal(t, eventID.String(), payload.EventID)
	require.Equal(t, "exception.created", payload.EventType)
	require.Equal(t, "2024-01-16T14:00:00Z", payload.Timestamp)

	require.Equal(t, exceptionID.String(), payload.Data.ExceptionID)
	require.Equal(t, transactionID.String(), payload.Data.TransactionID)
	require.Equal(t, "HIGH", payload.Data.Severity)
	require.Equal(t, "OPEN", payload.Data.Status)
	require.Equal(t, "25000", payload.Data.Amount)
	require.Equal(t, "USD", payload.Data.Currency)
	require.Equal(t, "Duplicate transaction", payload.Data.Reason)
	require.Equal(t, "BANK_STATEMENT", payload.Data.SourceType)
	require.Equal(t, "2024-01-15T10:30:00Z", payload.Data.CreatedAt)
	require.Equal(t, "2024-01-20T10:30:00Z", payload.Data.DueAt)

	require.Equal(t, "trace-webhook-123", payload.Metadata.TraceID)
	require.Equal(t, "WEBHOOK", payload.Metadata.Target)
	require.Equal(t, "webhook-queue", payload.Metadata.Queue)
	require.Equal(t, "high-amount-webhook-rule", payload.Metadata.RuleName)
	require.Equal(t, "ops-team", payload.Metadata.Assignee)
}

func TestBuildWebhookPayload_ValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		ctx         *DispatchContext
		cfg         WebhookConfig
		expectedErr error
	}{
		{
			name:        "nil context",
			ctx:         nil,
			cfg:         WebhookConfig{EventType: "exception.created"},
			expectedErr: ErrNilDispatchContext,
		},
		{
			name: "missing event type",
			ctx: &DispatchContext{
				Snapshot: ExceptionSnapshot{
					ID:       uuid.New(),
					Severity: value_objects.ExceptionSeverityMedium,
					Status:   value_objects.ExceptionStatusOpen,
				},
			},
			cfg:         WebhookConfig{EventType: ""},
			expectedErr: ErrMissingWebhookEventType,
		},
		{
			name: "whitespace event type",
			ctx: &DispatchContext{
				Snapshot: ExceptionSnapshot{
					ID:       uuid.New(),
					Severity: value_objects.ExceptionSeverityMedium,
					Status:   value_objects.ExceptionStatusOpen,
				},
			},
			cfg:         WebhookConfig{EventType: "   "},
			expectedErr: ErrMissingWebhookEventType,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			payload, err := BuildWebhookPayload(tt.ctx, tt.cfg, uuid.New())

			require.ErrorIs(t, err, tt.expectedErr)
			require.Nil(t, payload)
		})
	}
}

func TestBuildWebhookPayload_JSONMarshal(t *testing.T) {
	t.Parallel()

	exceptionID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	transactionID := uuid.MustParse("44444444-4444-4444-4444-444444444444")
	eventID := uuid.MustParse("55555555-5555-5555-5555-555555555555")

	ctx := &DispatchContext{
		Snapshot: ExceptionSnapshot{
			ID:            exceptionID,
			TransactionID: transactionID,
			Severity:      value_objects.ExceptionSeverityCritical,
			Status:        value_objects.ExceptionStatusAssigned,
			Amount:        decimal.NewFromInt(100000),
			Currency:      "EUR",
			Reason:        "Regulatory violation",
			SourceType:    "REGULATORY",
			CreatedAt:     time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		Decision: services.RoutingDecision{
			Target:   services.RoutingTargetWebhook,
			Queue:    "critical-queue",
			RuleName: "critical-rule",
		},
		Timestamp: time.Date(2024, 1, 16, 12, 0, 0, 0, time.UTC),
		TraceID:   "trace-xyz",
	}

	cfg := WebhookConfig{
		EventType: "exception.escalated",
	}

	payload, err := BuildWebhookPayload(ctx, cfg, eventID)
	require.NoError(t, err)

	jsonBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	var unmarshaled map[string]any

	err = json.Unmarshal(jsonBytes, &unmarshaled)
	require.NoError(t, err)

	require.Equal(t, "55555555-5555-5555-5555-555555555555", unmarshaled["eventId"])
	require.Equal(t, "exception.escalated", unmarshaled["eventType"])
	require.Equal(t, "2024-01-16T12:00:00Z", unmarshaled["timestamp"])

	data, ok := unmarshaled["data"].(map[string]any)
	require.True(t, ok, "expected data object")
	require.Equal(t, "33333333-3333-3333-3333-333333333333", data["exceptionId"])
	require.Equal(t, "44444444-4444-4444-4444-444444444444", data["transactionId"])
	require.Equal(t, "CRITICAL", data["severity"])
	require.Equal(t, "ASSIGNED", data["status"])
	require.Equal(t, "100000", data["amount"])
	require.Equal(t, "EUR", data["currency"])
	require.Equal(t, "Regulatory violation", data["reason"])
	require.Equal(t, "REGULATORY", data["sourceType"])

	metadata, ok := unmarshaled["metadata"].(map[string]any)
	require.True(t, ok, "expected metadata object")
	require.Equal(t, "trace-xyz", metadata["traceId"])
	require.Equal(t, "WEBHOOK", metadata["target"])
	require.Equal(t, "critical-queue", metadata["queue"])
	require.Equal(t, "critical-rule", metadata["ruleName"])
}

func TestBuildWebhookPayload_NoDueAt(t *testing.T) {
	t.Parallel()

	ctx := &DispatchContext{
		Snapshot: ExceptionSnapshot{
			ID:        uuid.New(),
			Severity:  value_objects.ExceptionSeverityLow,
			Status:    value_objects.ExceptionStatusOpen,
			CreatedAt: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
			DueAt:     nil,
		},
		Timestamp: time.Now(),
	}

	cfg := WebhookConfig{
		EventType: "exception.created",
	}

	payload, err := BuildWebhookPayload(ctx, cfg, uuid.New())

	require.NoError(t, err)
	require.Empty(t, payload.Data.DueAt)
}

func TestBuildWebhookPayload_EmptyOptionalMetadata(t *testing.T) {
	t.Parallel()

	ctx := &DispatchContext{
		Snapshot: ExceptionSnapshot{
			ID:        uuid.New(),
			Severity:  value_objects.ExceptionSeverityMedium,
			Status:    value_objects.ExceptionStatusOpen,
			CreatedAt: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		Decision: services.RoutingDecision{
			Target: services.RoutingTargetWebhook,
		},
		Timestamp: time.Now(),
	}

	cfg := WebhookConfig{
		EventType: "exception.created",
	}

	payload, err := BuildWebhookPayload(ctx, cfg, uuid.New())

	require.NoError(t, err)
	require.Empty(t, payload.Metadata.TraceID)
	require.Empty(t, payload.Metadata.Queue)
	require.Empty(t, payload.Metadata.RuleName)
	require.Empty(t, payload.Metadata.Assignee)
	require.Equal(t, "WEBHOOK", payload.Metadata.Target)
}
