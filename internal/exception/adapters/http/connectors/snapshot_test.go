//go:build unit

package connectors

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"

	"github.com/LerianStudio/matcher/internal/exception/domain/services"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
)

func TestExceptionSnapshot_Fields(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	dueAt := now.Add(24 * time.Hour)
	id := uuid.New()
	txID := uuid.New()

	snapshot := ExceptionSnapshot{
		ID:            id,
		TransactionID: txID,
		Severity:      value_objects.ExceptionSeverityHigh,
		Status:        value_objects.ExceptionStatusOpen,
		Amount:        decimal.NewFromInt(1000),
		Currency:      "USD",
		Reason:        "Missing counterparty",
		SourceType:    "BANK",
		CreatedAt:     now,
		DueAt:         &dueAt,
	}

	assert.Equal(t, id, snapshot.ID)
	assert.Equal(t, txID, snapshot.TransactionID)
	assert.Equal(t, value_objects.ExceptionSeverityHigh, snapshot.Severity)
	assert.Equal(t, value_objects.ExceptionStatusOpen, snapshot.Status)
	assert.True(t, decimal.NewFromInt(1000).Equal(snapshot.Amount))
	assert.Equal(t, "USD", snapshot.Currency)
	assert.Equal(t, "Missing counterparty", snapshot.Reason)
	assert.Equal(t, "BANK", snapshot.SourceType)
	assert.Equal(t, now, snapshot.CreatedAt)
	assert.NotNil(t, snapshot.DueAt)
	assert.Equal(t, dueAt, *snapshot.DueAt)
}

func TestExceptionSnapshot_NilDueAt(t *testing.T) {
	t.Parallel()

	snapshot := ExceptionSnapshot{
		DueAt: nil,
	}

	assert.Nil(t, snapshot.DueAt)
}

func TestExceptionSnapshot_AllSeverities(t *testing.T) {
	t.Parallel()

	severities := []value_objects.ExceptionSeverity{
		value_objects.ExceptionSeverityLow,
		value_objects.ExceptionSeverityMedium,
		value_objects.ExceptionSeverityHigh,
		value_objects.ExceptionSeverityCritical,
	}

	for _, severity := range severities {
		t.Run(severity.String(), func(t *testing.T) {
			t.Parallel()

			snapshot := ExceptionSnapshot{
				Severity: severity,
			}

			assert.Equal(t, severity, snapshot.Severity)
			assert.True(t, snapshot.Severity.IsValid())
		})
	}
}

func TestExceptionSnapshot_AllStatuses(t *testing.T) {
	t.Parallel()

	statuses := []value_objects.ExceptionStatus{
		value_objects.ExceptionStatusOpen,
		value_objects.ExceptionStatusAssigned,
		value_objects.ExceptionStatusResolved,
	}

	for _, status := range statuses {
		t.Run(status.String(), func(t *testing.T) {
			t.Parallel()

			snapshot := ExceptionSnapshot{
				Status: status,
			}

			assert.Equal(t, status, snapshot.Status)
			assert.True(t, snapshot.Status.IsValid())
		})
	}
}

func TestDispatchContext_Fields(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	traceID := "trace-abc-123"

	snapshot := ExceptionSnapshot{
		ID:            uuid.New(),
		TransactionID: uuid.New(),
		Severity:      value_objects.ExceptionSeverityCritical,
		Status:        value_objects.ExceptionStatusOpen,
		Amount:        decimal.NewFromInt(500000),
		Currency:      "USD",
		Reason:        "Regulatory exception",
		SourceType:    "REGULATORY",
		CreatedAt:     now,
	}

	decision := services.RoutingDecision{
		Target:    services.RoutingTargetJira,
		Queue:     "high-priority",
		Assignee:  "analyst@example.com",
		RuleName:  "critical-routing",
		RuleIndex: 0,
	}

	dispatchCtx := DispatchContext{
		Snapshot:  snapshot,
		Decision:  decision,
		Timestamp: now,
		TraceID:   traceID,
	}

	assert.Equal(t, snapshot, dispatchCtx.Snapshot)
	assert.Equal(t, decision, dispatchCtx.Decision)
	assert.Equal(t, now, dispatchCtx.Timestamp)
	assert.Equal(t, traceID, dispatchCtx.TraceID)
}

func TestDispatchContext_AllRoutingTargets(t *testing.T) {
	t.Parallel()

	targets := []services.RoutingTarget{
		services.RoutingTargetManual,
		services.RoutingTargetJira,
		services.RoutingTargetServiceNow,
		services.RoutingTargetWebhook,
	}

	for _, target := range targets {
		t.Run(string(target), func(t *testing.T) {
			t.Parallel()

			dispatchCtx := DispatchContext{
				Decision: services.RoutingDecision{
					Target: target,
				},
			}

			assert.Equal(t, target, dispatchCtx.Decision.Target)
			assert.True(t, dispatchCtx.Decision.Target.IsValid())
		})
	}
}

func TestDispatchContext_EmptyTraceID(t *testing.T) {
	t.Parallel()

	dispatchCtx := DispatchContext{
		TraceID: "",
	}

	assert.Empty(t, dispatchCtx.TraceID)
}

func TestExceptionSnapshot_ZeroAmount(t *testing.T) {
	t.Parallel()

	snapshot := ExceptionSnapshot{
		Amount: decimal.Zero,
	}

	assert.True(t, snapshot.Amount.IsZero())
}

func TestExceptionSnapshot_NegativeAmount(t *testing.T) {
	t.Parallel()

	snapshot := ExceptionSnapshot{
		Amount: decimal.NewFromInt(-100),
	}

	assert.True(t, snapshot.Amount.IsNegative())
}
