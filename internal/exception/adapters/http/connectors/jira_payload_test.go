//go:build unit

package connectors

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/exception/domain/services"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
)

func TestBuildJiraPayload_HappyPath(t *testing.T) {
	t.Parallel()

	exceptionID := uuid.New()
	transactionID := uuid.New()
	createdAt := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	dueAt := time.Date(2024, 1, 20, 10, 30, 0, 0, time.UTC)

	ctx := &DispatchContext{
		Snapshot: ExceptionSnapshot{
			ID:            exceptionID,
			TransactionID: transactionID,
			Severity:      value_objects.ExceptionSeverityCritical,
			Status:        value_objects.ExceptionStatusOpen,
			Amount:        decimal.NewFromInt(50000),
			Currency:      "USD",
			Reason:        "Amount mismatch",
			SourceType:    "BANK_STATEMENT",
			CreatedAt:     createdAt,
			DueAt:         &dueAt,
		},
		Decision: services.RoutingDecision{
			Target:   services.RoutingTargetJira,
			Queue:    "reconciliation-queue",
			Assignee: "john.doe",
			RuleName: "critical-amount-rule",
		},
		Timestamp: time.Now(),
		TraceID:   "trace-123-abc",
	}

	cfg := JiraConfig{
		ProjectKey: "RECON",
		IssueType:  "Bug",
	}

	payload, err := BuildJiraPayload(ctx, cfg)

	require.NoError(t, err)
	require.NotNil(t, payload)
	require.Equal(t, "RECON", payload.Fields.Project.Key)
	require.Equal(t, "Bug", payload.Fields.IssueType.Name)
	require.Contains(t, payload.Fields.Summary, "CRITICAL")
	require.Contains(t, payload.Fields.Summary, "Amount mismatch")
	require.Contains(t, payload.Fields.Description, exceptionID.String())
	require.Contains(t, payload.Fields.Description, transactionID.String())
	require.Contains(t, payload.Fields.Description, "50000")
	require.Contains(t, payload.Fields.Description, "USD")
	require.Contains(t, payload.Fields.Description, "trace-123-abc")
	require.NotNil(t, payload.Fields.Priority)
	require.Equal(t, "Highest", payload.Fields.Priority.Name)
	require.NotNil(t, payload.Fields.Assignee)
	require.Equal(t, "john.doe", payload.Fields.Assignee.Name)
}

func TestBuildJiraPayload_ValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		ctx         *DispatchContext
		cfg         JiraConfig
		expectedErr error
	}{
		{
			name:        "nil context",
			ctx:         nil,
			cfg:         JiraConfig{ProjectKey: "PROJ", IssueType: "Bug"},
			expectedErr: ErrNilDispatchContext,
		},
		{
			name: "missing project key",
			ctx: &DispatchContext{
				Snapshot: ExceptionSnapshot{
					ID:       uuid.New(),
					Severity: value_objects.ExceptionSeverityMedium,
					Status:   value_objects.ExceptionStatusOpen,
				},
			},
			cfg:         JiraConfig{ProjectKey: "", IssueType: "Bug"},
			expectedErr: ErrMissingJiraProjectKey,
		},
		{
			name: "whitespace project key",
			ctx: &DispatchContext{
				Snapshot: ExceptionSnapshot{
					ID:       uuid.New(),
					Severity: value_objects.ExceptionSeverityMedium,
					Status:   value_objects.ExceptionStatusOpen,
				},
			},
			cfg:         JiraConfig{ProjectKey: "   ", IssueType: "Bug"},
			expectedErr: ErrMissingJiraProjectKey,
		},
		{
			name: "missing issue type",
			ctx: &DispatchContext{
				Snapshot: ExceptionSnapshot{
					ID:       uuid.New(),
					Severity: value_objects.ExceptionSeverityMedium,
					Status:   value_objects.ExceptionStatusOpen,
				},
			},
			cfg:         JiraConfig{ProjectKey: "PROJ", IssueType: ""},
			expectedErr: ErrMissingJiraIssueType,
		},
		{
			name: "whitespace issue type",
			ctx: &DispatchContext{
				Snapshot: ExceptionSnapshot{
					ID:       uuid.New(),
					Severity: value_objects.ExceptionSeverityMedium,
					Status:   value_objects.ExceptionStatusOpen,
				},
			},
			cfg:         JiraConfig{ProjectKey: "PROJ", IssueType: "   "},
			expectedErr: ErrMissingJiraIssueType,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			payload, err := BuildJiraPayload(tt.ctx, tt.cfg)

			require.ErrorIs(t, err, tt.expectedErr)
			require.Nil(t, payload)
		})
	}
}

func TestBuildJiraPayload_JSONMarshal(t *testing.T) {
	t.Parallel()

	ctx := &DispatchContext{
		Snapshot: ExceptionSnapshot{
			ID:            uuid.MustParse("11111111-1111-1111-1111-111111111111"),
			TransactionID: uuid.MustParse("22222222-2222-2222-2222-222222222222"),
			Severity:      value_objects.ExceptionSeverityHigh,
			Status:        value_objects.ExceptionStatusAssigned,
			Amount:        decimal.NewFromInt(10000),
			Currency:      "EUR",
			Reason:        "Missing transaction",
			SourceType:    "LEDGER",
			CreatedAt:     time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		Decision: services.RoutingDecision{
			Target:   services.RoutingTargetJira,
			Queue:    "ops-queue",
			RuleName: "high-amount-rule",
		},
		TraceID: "trace-456",
	}

	cfg := JiraConfig{
		ProjectKey: "OPS",
		IssueType:  "Task",
	}

	payload, err := BuildJiraPayload(ctx, cfg)
	require.NoError(t, err)

	jsonBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	var unmarshaled map[string]any

	err = json.Unmarshal(jsonBytes, &unmarshaled)
	require.NoError(t, err)

	fields, ok := unmarshaled["fields"].(map[string]any)
	require.True(t, ok, "expected fields object")

	project, ok := fields["project"].(map[string]any)
	require.True(t, ok, "expected project object")
	require.Equal(t, "OPS", project["key"])

	issueType, ok := fields["issuetype"].(map[string]any)
	require.True(t, ok, "expected issuetype object")
	require.Equal(t, "Task", issueType["name"])

	priority, ok := fields["priority"].(map[string]any)
	require.True(t, ok, "expected priority object")
	require.Equal(t, "High", priority["name"])

	summary, ok := fields["summary"].(string)
	require.True(t, ok, "expected summary string")
	require.NotEmpty(t, summary)

	description, ok := fields["description"].(string)
	require.True(t, ok, "expected description string")
	require.NotEmpty(t, description)
}

func TestBuildJiraPayload_SeverityToPriorityMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		severity         value_objects.ExceptionSeverity
		expectedPriority string
	}{
		{value_objects.ExceptionSeverityCritical, "Highest"},
		{value_objects.ExceptionSeverityHigh, "High"},
		{value_objects.ExceptionSeverityMedium, "Medium"},
		{value_objects.ExceptionSeverityLow, "Low"},
		{value_objects.ExceptionSeverity("UNKNOWN"), "Medium"},
	}

	for _, tt := range tests {
		t.Run(string(tt.severity), func(t *testing.T) {
			t.Parallel()

			ctx := &DispatchContext{
				Snapshot: ExceptionSnapshot{
					ID:       uuid.New(),
					Severity: tt.severity,
					Status:   value_objects.ExceptionStatusOpen,
					Reason:   "Test reason",
				},
			}

			cfg := JiraConfig{
				ProjectKey: "TEST",
				IssueType:  "Bug",
			}

			payload, err := BuildJiraPayload(ctx, cfg)

			require.NoError(t, err)
			require.NotNil(t, payload.Fields.Priority)
			require.Equal(t, tt.expectedPriority, payload.Fields.Priority.Name)
		})
	}
}

func TestBuildJiraPayload_NoAssignee(t *testing.T) {
	t.Parallel()

	ctx := &DispatchContext{
		Snapshot: ExceptionSnapshot{
			ID:       uuid.New(),
			Severity: value_objects.ExceptionSeverityMedium,
			Status:   value_objects.ExceptionStatusOpen,
			Reason:   "Test",
		},
		Decision: services.RoutingDecision{
			Target:   services.RoutingTargetJira,
			Assignee: "",
		},
	}

	cfg := JiraConfig{
		ProjectKey: "TEST",
		IssueType:  "Bug",
	}

	payload, err := BuildJiraPayload(ctx, cfg)

	require.NoError(t, err)
	require.Nil(t, payload.Fields.Assignee)
}

func TestBuildJiraPayload_UnknownSeverityFallback(t *testing.T) {
	t.Parallel()

	ctx := &DispatchContext{
		Snapshot: ExceptionSnapshot{
			ID:       uuid.New(),
			Severity: value_objects.ExceptionSeverity("UNKNOWN"),
			Status:   value_objects.ExceptionStatusOpen,
			Reason:   "Test unknown severity",
		},
	}

	cfg := JiraConfig{
		ProjectKey: "TEST",
		IssueType:  "Bug",
	}

	payload, err := BuildJiraPayload(ctx, cfg)

	require.NoError(t, err)
	require.NotNil(t, payload.Fields.Priority)
	require.Equal(
		t,
		"Medium",
		payload.Fields.Priority.Name,
		"unknown severity should fall back to Medium priority",
	)
}

func TestBuildJiraPayload_SummaryTruncation(t *testing.T) {
	t.Parallel()

	longReason := strings.Repeat("x", 300)

	ctx := &DispatchContext{
		Snapshot: ExceptionSnapshot{
			ID:       uuid.New(),
			Severity: value_objects.ExceptionSeverityLow,
			Status:   value_objects.ExceptionStatusOpen,
			Reason:   longReason,
		},
	}

	cfg := JiraConfig{
		ProjectKey: "TEST",
		IssueType:  "Bug",
	}

	payload, err := BuildJiraPayload(ctx, cfg)

	require.NoError(t, err)
	require.LessOrEqual(t, len(payload.Fields.Summary), 255)
}
