//go:build unit

package entities

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
)

func testNewContextError(
	t *testing.T,
	tenantID uuid.UUID,
	input CreateReconciliationContextInput,
	expectedErr error,
) {
	t.Helper()

	_, err := NewReconciliationContext(context.Background(), tenantID, input)
	require.ErrorIs(t, err, expectedErr)
}

func testContextUpdateError(
	t *testing.T,
	ctx *ReconciliationContext,
	input UpdateReconciliationContextInput,
	expectedErr error,
) {
	t.Helper()

	originalName := ctx.Name
	originalType := ctx.Type
	originalStatus := ctx.Status
	originalInterval := ctx.Interval
	originalUpdatedAt := ctx.UpdatedAt

	err := ctx.Update(context.Background(), input)
	require.ErrorIs(t, err, expectedErr)
	assert.Equal(t, originalName, ctx.Name)
	assert.Equal(t, originalType, ctx.Type)
	assert.Equal(t, originalStatus, ctx.Status)
	assert.Equal(t, originalInterval, ctx.Interval)
	assert.Equal(t, originalUpdatedAt, ctx.UpdatedAt)
}

func TestNewReconciliationContext(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()

	t.Run("creates valid context in DRAFT status", func(t *testing.T) {
		t.Parallel()

		input := CreateReconciliationContextInput{
			Name:     "Ledger vs Bank",
			Type:     value_objects.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		}

		ctx := context.Background()
		contextEntity, err := NewReconciliationContext(ctx, tenantID, input)
		require.NoError(t, err)
		assert.NotEqual(t, uuid.Nil, contextEntity.ID)
		assert.Equal(t, tenantID, contextEntity.TenantID)
		assert.Equal(t, "Ledger vs Bank", contextEntity.Name)
		assert.Equal(t, value_objects.ContextTypeOneToOne, contextEntity.Type)
		assert.Equal(t, value_objects.ContextStatusDraft, contextEntity.Status)
		assert.True(t, contextEntity.IsDraft())
		assert.False(t, contextEntity.IsActive())
		assert.False(t, contextEntity.IsArchived())
		assert.False(t, contextEntity.CreatedAt.IsZero())
	})

	t.Run("accepts inline sources and rules in input", func(t *testing.T) {
		t.Parallel()

		input := CreateReconciliationContextInput{
			Name:     "Inline Setup",
			Type:     value_objects.ContextTypeOneToOne,
			Interval: "0 0 * * *",
			Sources: []CreateContextSourceInput{
				{Name: "Bank", Type: value_objects.SourceTypeBank},
			},
			Rules: []CreateMatchRuleInput{
				{Priority: 1, Type: value_objects.RuleTypeExact, Config: map[string]any{"matchAmount": true}},
			},
		}

		contextEntity, err := NewReconciliationContext(context.Background(), tenantID, input)
		require.NoError(t, err)
		require.NotNil(t, contextEntity)
		assert.Equal(t, "Inline Setup", contextEntity.Name)
		assert.Equal(t, value_objects.ContextStatusDraft, contextEntity.Status)
	})

	t.Run("fails with nil tenant", func(t *testing.T) {
		t.Parallel()

		input := CreateReconciliationContextInput{
			Name:     "Test",
			Type:     value_objects.ContextTypeOneToOne,
			Interval: "* * * * *",
		}

		_, err := NewReconciliationContext(context.Background(), uuid.Nil, input)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrContextTenantRequired)
	})

	t.Run("fails with whitespace name", func(t *testing.T) {
		t.Parallel()
		testNewContextError(
			t,
			tenantID,
			CreateReconciliationContextInput{
				Name:     "   ",
				Type:     value_objects.ContextTypeOneToOne,
				Interval: "* * * * *",
			},
			ErrContextNameRequired,
		)
	})

	t.Run("fails with empty name", func(t *testing.T) {
		t.Parallel()
		testNewContextError(
			t,
			tenantID,
			CreateReconciliationContextInput{
				Name:     "",
				Type:     value_objects.ContextTypeOneToOne,
				Interval: "* * * * *",
			},
			ErrContextNameRequired,
		)
	})

	t.Run("accepts name at max length", func(t *testing.T) {
		t.Parallel()

		maxName := strings.Repeat("a", 100)
		input := CreateReconciliationContextInput{
			Name:     maxName,
			Type:     value_objects.ContextTypeOneToOne,
			Interval: "* * * * *",
		}

		ctx := context.Background()
		contextEntity, err := NewReconciliationContext(ctx, tenantID, input)
		require.NoError(t, err)
		assert.Equal(t, maxName, contextEntity.Name)
	})

	t.Run("fails with name too long", func(t *testing.T) {
		t.Parallel()

		longName := strings.Repeat("a", 101)
		testNewContextError(
			t,
			tenantID,
			CreateReconciliationContextInput{
				Name:     longName,
				Type:     value_objects.ContextTypeOneToOne,
				Interval: "* * * * *",
			},
			ErrContextNameTooLong,
		)
	})

	t.Run("fails with invalid type", func(t *testing.T) {
		t.Parallel()
		testNewContextError(
			t,
			tenantID,
			CreateReconciliationContextInput{
				Name:     "Test",
				Type:     value_objects.ContextType("INVALID"),
				Interval: "* * * * *",
			},
			ErrContextTypeInvalid,
		)
	})

	t.Run("fails with empty interval", func(t *testing.T) {
		t.Parallel()
		testNewContextError(
			t,
			tenantID,
			CreateReconciliationContextInput{
				Name:     "Test",
				Type:     value_objects.ContextTypeOneToOne,
				Interval: "",
			},
			ErrContextIntervalRequired,
		)
	})

	t.Run("fails with whitespace interval", func(t *testing.T) {
		t.Parallel()
		testNewContextError(
			t,
			tenantID,
			CreateReconciliationContextInput{
				Name:     "Test",
				Type:     value_objects.ContextTypeOneToOne,
				Interval: "   ",
			},
			ErrContextIntervalRequired,
		)
	})
}

func createTestContextEntity(t *testing.T, tenantID uuid.UUID) *ReconciliationContext {
	t.Helper()

	input := CreateReconciliationContextInput{
		Name:     "Original",
		Type:     value_objects.ContextTypeOneToOne,
		Interval: "0 0 * * *",
	}

	ctx := context.Background()
	contextEntity, err := NewReconciliationContext(ctx, tenantID, input)
	require.NoError(t, err)

	return contextEntity
}

// createActiveTestContextEntity creates a test context and activates it,
// since contexts now start in DRAFT status.
func createActiveTestContextEntity(t *testing.T, tenantID uuid.UUID) *ReconciliationContext {
	t.Helper()

	contextEntity := createTestContextEntity(t, tenantID)
	require.NoError(t, contextEntity.Activate(context.Background()))
	require.Equal(t, value_objects.ContextStatusActive, contextEntity.Status)

	return contextEntity
}

func runSuccessfulUpdateTests(t *testing.T, tenantID uuid.UUID) {
	t.Helper()

	t.Run("updates name", func(t *testing.T) {
		t.Parallel()
		contextEntity := createTestContextEntity(t, tenantID)
		originalUpdatedAt := contextEntity.UpdatedAt
		newName := "Updated"
		err := contextEntity.Update(
			context.Background(),
			UpdateReconciliationContextInput{Name: &newName},
		)
		require.NoError(t, err)
		assert.Equal(t, "Updated", contextEntity.Name)
		assert.False(t, contextEntity.UpdatedAt.Before(originalUpdatedAt))
	})

	t.Run("updates type", func(t *testing.T) {
		t.Parallel()
		contextEntity := createTestContextEntity(t, tenantID)
		newType := value_objects.ContextTypeOneToMany
		err := contextEntity.Update(
			context.Background(),
			UpdateReconciliationContextInput{Type: &newType},
		)
		require.NoError(t, err)
		assert.Equal(t, value_objects.ContextTypeOneToMany, contextEntity.Type)
	})

	t.Run("updates status from draft to active", func(t *testing.T) {
		t.Parallel()
		contextEntity := createTestContextEntity(t, tenantID)
		active := value_objects.ContextStatusActive
		err := contextEntity.Update(
			context.Background(),
			UpdateReconciliationContextInput{Status: &active},
		)
		require.NoError(t, err)
		assert.Equal(t, value_objects.ContextStatusActive, contextEntity.Status)
	})

	t.Run("updates status from active to paused", func(t *testing.T) {
		t.Parallel()
		contextEntity := createActiveTestContextEntity(t, tenantID)
		paused := value_objects.ContextStatusPaused
		err := contextEntity.Update(
			context.Background(),
			UpdateReconciliationContextInput{Status: &paused},
		)
		require.NoError(t, err)
		assert.Equal(t, value_objects.ContextStatusPaused, contextEntity.Status)
	})

	t.Run("updates interval", func(t *testing.T) {
		t.Parallel()
		contextEntity := createTestContextEntity(t, tenantID)
		newInterval := "0 */6 * * *"
		err := contextEntity.Update(
			context.Background(),
			UpdateReconciliationContextInput{Interval: &newInterval},
		)
		require.NoError(t, err)
		assert.Equal(t, "0 */6 * * *", contextEntity.Interval)
	})

	t.Run("activates from paused status", func(t *testing.T) {
		t.Parallel()

		contextEntity := createActiveTestContextEntity(t, tenantID)

		err := contextEntity.Pause(context.Background())
		require.NoError(t, err)
		require.Equal(t, value_objects.ContextStatusPaused, contextEntity.Status)

		active := value_objects.ContextStatusActive
		err = contextEntity.Update(
			context.Background(),
			UpdateReconciliationContextInput{Status: &active},
		)
		require.NoError(t, err)
		assert.Equal(t, value_objects.ContextStatusActive, contextEntity.Status)
	})
}

func runValidationFailureTests(t *testing.T, tenantID uuid.UUID) {
	t.Helper()

	t.Run("fails with nil receiver", func(t *testing.T) {
		t.Parallel()

		newName := "Updated"

		err := (*ReconciliationContext)(
			nil,
		).Update(context.Background(), UpdateReconciliationContextInput{Name: &newName})
		require.Error(t, err)
		assert.Equal(t, ErrNilReconciliationContext, err)
	})

	t.Run("fails with empty name", func(t *testing.T) {
		t.Parallel()
		contextEntity := createTestContextEntity(t, tenantID)
		emptyName := ""
		testContextUpdateError(
			t,
			contextEntity,
			UpdateReconciliationContextInput{Name: &emptyName},
			ErrContextNameRequired,
		)
	})

	t.Run("fails with whitespace name", func(t *testing.T) {
		t.Parallel()
		contextEntity := createTestContextEntity(t, tenantID)
		whitespace := "   "
		testContextUpdateError(
			t,
			contextEntity,
			UpdateReconciliationContextInput{Name: &whitespace},
			ErrContextNameRequired,
		)
	})

	t.Run("fails with name too long", func(t *testing.T) {
		t.Parallel()
		contextEntity := createTestContextEntity(t, tenantID)
		longName := strings.Repeat("a", 101)
		testContextUpdateError(
			t,
			contextEntity,
			UpdateReconciliationContextInput{Name: &longName},
			ErrContextNameTooLong,
		)
	})

	t.Run("fails with invalid type", func(t *testing.T) {
		t.Parallel()
		contextEntity := createTestContextEntity(t, tenantID)
		invalidType := value_objects.ContextType("INVALID")
		testContextUpdateError(
			t,
			contextEntity,
			UpdateReconciliationContextInput{Type: &invalidType},
			ErrContextTypeInvalid,
		)
	})

	t.Run("fails with empty interval", func(t *testing.T) {
		t.Parallel()
		contextEntity := createTestContextEntity(t, tenantID)
		emptyInterval := ""
		testContextUpdateError(
			t,
			contextEntity,
			UpdateReconciliationContextInput{Interval: &emptyInterval},
			ErrContextIntervalRequired,
		)
	})

	t.Run("fails with whitespace interval", func(t *testing.T) {
		t.Parallel()
		contextEntity := createTestContextEntity(t, tenantID)
		whitespaceInterval := "   "
		testContextUpdateError(
			t,
			contextEntity,
			UpdateReconciliationContextInput{Interval: &whitespaceInterval},
			ErrContextIntervalRequired,
		)
	})

	t.Run("fails with invalid status", func(t *testing.T) {
		t.Parallel()
		contextEntity := createTestContextEntity(t, tenantID)

		invalidStatus := value_objects.ContextStatus("INVALID")
		testContextUpdateError(
			t,
			contextEntity,
			UpdateReconciliationContextInput{Status: &invalidStatus},
			ErrContextStatusInvalid,
		)
	})

	t.Run("fails to update archived context", func(t *testing.T) {
		t.Parallel()

		contextEntity := createActiveTestContextEntity(t, tenantID)
		require.NoError(t, contextEntity.Archive(context.Background()))
		require.Equal(t, value_objects.ContextStatusArchived, contextEntity.Status)

		newName := "Should Fail"
		err := contextEntity.Update(context.Background(), UpdateReconciliationContextInput{Name: &newName})
		require.ErrorIs(t, err, ErrArchivedContextCannotBeModified)
		assert.Equal(t, "Original", contextEntity.Name)
	})
}

func TestReconciliationContext_Update(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()

	t.Run("successful updates", func(t *testing.T) {
		t.Parallel()
		runSuccessfulUpdateTests(t, tenantID)
	})

	t.Run("validation failures", func(t *testing.T) {
		t.Parallel()
		runValidationFailureTests(t, tenantID)
	})
}

func TestNewReconciliationContext_InvalidFeeNormalization(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	invalidMode := "INVALID_MODE"

	_, err := NewReconciliationContext(context.Background(), tenantID, CreateReconciliationContextInput{
		Name:             "Test",
		Type:             value_objects.ContextTypeOneToOne,
		Interval:         "0 0 * * *",
		FeeNormalization: &invalidMode,
	})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrFeeNormalizationInvalid)
}

func TestUpdateReconciliationContext_InvalidFeeNormalization(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextEntity := createTestContextEntity(t, tenantID)

	invalidMode := "INVALID_MODE"
	testContextUpdateError(
		t,
		contextEntity,
		UpdateReconciliationContextInput{FeeNormalization: &invalidMode},
		ErrFeeNormalizationInvalid,
	)
}

func TestReconciliationContext_FullLifecycle(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	input := CreateReconciliationContextInput{
		Name:     "Lifecycle Test",
		Type:     value_objects.ContextTypeOneToOne,
		Interval: "0 0 * * *",
	}

	contextEntity, err := NewReconciliationContext(context.Background(), tenantID, input)
	require.NoError(t, err)

	// Starts in DRAFT
	assert.True(t, contextEntity.IsDraft())
	assert.False(t, contextEntity.IsActive())
	assert.False(t, contextEntity.IsArchived())

	// DRAFT -> ACTIVE
	require.NoError(t, contextEntity.Activate(context.Background()))
	assert.Equal(t, value_objects.ContextStatusActive, contextEntity.Status)
	assert.True(t, contextEntity.IsActive())

	// ACTIVE -> PAUSED
	require.NoError(t, contextEntity.Pause(context.Background()))
	assert.Equal(t, value_objects.ContextStatusPaused, contextEntity.Status)
	assert.False(t, contextEntity.IsActive())

	// PAUSED -> ACTIVE
	require.NoError(t, contextEntity.Activate(context.Background()))
	assert.Equal(t, value_objects.ContextStatusActive, contextEntity.Status)
	assert.True(t, contextEntity.IsActive())

	// ACTIVE -> ARCHIVED
	require.NoError(t, contextEntity.Archive(context.Background()))
	assert.Equal(t, value_objects.ContextStatusArchived, contextEntity.Status)
	assert.True(t, contextEntity.IsArchived())
	assert.False(t, contextEntity.IsActive())
}

func TestReconciliationContext_PauseActivate(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	input := CreateReconciliationContextInput{
		Name:     "Test",
		Type:     value_objects.ContextTypeOneToOne,
		Interval: "0 0 * * *",
	}
	contextEntity, err := NewReconciliationContext(context.Background(), tenantID, input)
	require.NoError(t, err)

	assert.True(t, contextEntity.IsDraft())
	assert.False(t, contextEntity.IsActive())

	require.NoError(t, contextEntity.Activate(context.Background()))
	assert.Equal(t, value_objects.ContextStatusActive, contextEntity.Status)
	assert.True(t, contextEntity.IsActive())

	require.NoError(t, contextEntity.Pause(context.Background()))
	assert.Equal(t, value_objects.ContextStatusPaused, contextEntity.Status)
	assert.False(t, contextEntity.IsActive())

	require.NoError(t, contextEntity.Activate(context.Background()))
	assert.Equal(t, value_objects.ContextStatusActive, contextEntity.Status)
	assert.True(t, contextEntity.IsActive())
}

func TestReconciliationContext_StateTransitionGuards(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()

	t.Run("cannot pause from DRAFT", func(t *testing.T) {
		t.Parallel()
		contextEntity := createTestContextEntity(t, tenantID)
		require.Equal(t, value_objects.ContextStatusDraft, contextEntity.Status)
		err := contextEntity.Pause(context.Background())
		require.ErrorIs(t, err, ErrInvalidStateTransition)
	})

	t.Run("cannot pause from PAUSED", func(t *testing.T) {
		t.Parallel()
		contextEntity := createActiveTestContextEntity(t, tenantID)
		require.NoError(t, contextEntity.Pause(context.Background()))
		err := contextEntity.Pause(context.Background())
		require.ErrorIs(t, err, ErrInvalidStateTransition)
	})

	t.Run("cannot pause from ARCHIVED", func(t *testing.T) {
		t.Parallel()
		contextEntity := createActiveTestContextEntity(t, tenantID)
		require.NoError(t, contextEntity.Archive(context.Background()))
		err := contextEntity.Pause(context.Background())
		require.ErrorIs(t, err, ErrInvalidStateTransition)
	})

	t.Run("cannot activate from ACTIVE", func(t *testing.T) {
		t.Parallel()
		contextEntity := createActiveTestContextEntity(t, tenantID)
		err := contextEntity.Activate(context.Background())
		require.ErrorIs(t, err, ErrInvalidStateTransition)
	})

	t.Run("cannot activate from ARCHIVED", func(t *testing.T) {
		t.Parallel()
		contextEntity := createActiveTestContextEntity(t, tenantID)
		require.NoError(t, contextEntity.Archive(context.Background()))
		err := contextEntity.Activate(context.Background())
		require.ErrorIs(t, err, ErrInvalidStateTransition)
	})

	t.Run("cannot archive from DRAFT", func(t *testing.T) {
		t.Parallel()
		contextEntity := createTestContextEntity(t, tenantID)
		err := contextEntity.Archive(context.Background())
		require.ErrorIs(t, err, ErrInvalidStateTransition)
	})

	t.Run("cannot archive from ARCHIVED", func(t *testing.T) {
		t.Parallel()
		contextEntity := createActiveTestContextEntity(t, tenantID)
		require.NoError(t, contextEntity.Archive(context.Background()))
		err := contextEntity.Archive(context.Background())
		require.ErrorIs(t, err, ErrInvalidStateTransition)
	})

	t.Run("can archive from ACTIVE", func(t *testing.T) {
		t.Parallel()
		contextEntity := createActiveTestContextEntity(t, tenantID)
		require.NoError(t, contextEntity.Archive(context.Background()))
		assert.Equal(t, value_objects.ContextStatusArchived, contextEntity.Status)
	})

	t.Run("can archive from PAUSED", func(t *testing.T) {
		t.Parallel()
		contextEntity := createActiveTestContextEntity(t, tenantID)
		require.NoError(t, contextEntity.Pause(context.Background()))
		require.NoError(t, contextEntity.Archive(context.Background()))
		assert.Equal(t, value_objects.ContextStatusArchived, contextEntity.Status)
	})

	t.Run("state transition error includes current status", func(t *testing.T) {
		t.Parallel()
		contextEntity := createActiveTestContextEntity(t, tenantID)
		err := contextEntity.Activate(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ACTIVE")
		assert.ErrorIs(t, err, ErrInvalidStateTransition)
	})
}

// ---------------------------------------------------------------------------
// Security audit tests (Taura Security, 2026-03)
//
// These tests exhaustively verify the PAUSED -> ACTIVE recovery path and
// prove that paused contexts can be updated (including status change) through
// the domain entity's Update method, which is the code path used by the
// configuration UpdateContext handler.
// ---------------------------------------------------------------------------

// TestPausedContextRecoveryPath_SecurityAudit proves the complete recovery
// scenario: ACTIVE -> PAUSED -> ACTIVE via Update() with status=ACTIVE.
// This is the exact code path exercised by PATCH /v1/config/contexts/:contextId
// with body {"status": "ACTIVE"}.
func TestPausedContextRecoveryPath_SecurityAudit(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextEntity := createActiveTestContextEntity(t, tenantID)

	// Step 1: Pause the context (simulates an admin pausing for maintenance).
	require.NoError(t, contextEntity.Pause(context.Background()))
	require.Equal(t, value_objects.ContextStatusPaused, contextEntity.Status)

	// Step 2: Re-activate via Update() (simulates the PATCH handler).
	activeStatus := value_objects.ContextStatusActive
	err := contextEntity.Update(context.Background(), UpdateReconciliationContextInput{
		Status: &activeStatus,
	})
	require.NoError(t, err, "PAUSED context must be recoverable via Update(status=ACTIVE)")
	assert.Equal(t, value_objects.ContextStatusActive, contextEntity.Status)
}

// TestPausedContextCanBeUpdatedWithNonStatusFields proves that non-status
// fields (name, type, interval, fee settings) can be modified on a paused
// context. This ensures administrators can fix configuration issues on paused
// contexts before re-activating them.
func TestPausedContextCanBeUpdatedWithNonStatusFields(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextEntity := createActiveTestContextEntity(t, tenantID)
	require.NoError(t, contextEntity.Pause(context.Background()))
	require.Equal(t, value_objects.ContextStatusPaused, contextEntity.Status)

	// Update name on a paused context.
	newName := "Renamed While Paused"
	err := contextEntity.Update(context.Background(), UpdateReconciliationContextInput{
		Name: &newName,
	})
	require.NoError(t, err, "PAUSED context must accept name updates")
	assert.Equal(t, "Renamed While Paused", contextEntity.Name)
	assert.Equal(t, value_objects.ContextStatusPaused, contextEntity.Status, "status must remain PAUSED")

	// Update interval on a paused context.
	newInterval := "0 */12 * * *"
	err = contextEntity.Update(context.Background(), UpdateReconciliationContextInput{
		Interval: &newInterval,
	})
	require.NoError(t, err, "PAUSED context must accept interval updates")
	assert.Equal(t, "0 */12 * * *", contextEntity.Interval)
}

// TestPausedContextCanBeArchived proves the PAUSED -> ARCHIVED path works,
// which is the other valid outbound transition from PAUSED.
func TestPausedContextCanBeArchived(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	contextEntity := createActiveTestContextEntity(t, tenantID)
	require.NoError(t, contextEntity.Pause(context.Background()))

	archivedStatus := value_objects.ContextStatusArchived
	err := contextEntity.Update(context.Background(), UpdateReconciliationContextInput{
		Status: &archivedStatus,
	})
	require.NoError(t, err, "PAUSED -> ARCHIVED transition must succeed")
	assert.Equal(t, value_objects.ContextStatusArchived, contextEntity.Status)
}

// TestContextStateMachine_AllValidTransitions exhaustively tests every valid
// state transition defined in the domain state machine. This serves as the
// authoritative test for the state machine documentation in context_status.go.
func TestContextStateMachine_AllValidTransitions(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()

	tests := []struct {
		name       string
		fromStatus value_objects.ContextStatus
		toStatus   value_objects.ContextStatus
	}{
		{"DRAFT -> ACTIVE", value_objects.ContextStatusDraft, value_objects.ContextStatusActive},
		{"ACTIVE -> PAUSED", value_objects.ContextStatusActive, value_objects.ContextStatusPaused},
		{"ACTIVE -> ARCHIVED", value_objects.ContextStatusActive, value_objects.ContextStatusArchived},
		{"PAUSED -> ACTIVE (recovery)", value_objects.ContextStatusPaused, value_objects.ContextStatusActive},
		{"PAUSED -> ARCHIVED", value_objects.ContextStatusPaused, value_objects.ContextStatusArchived},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Build context entity in the desired fromStatus.
			contextEntity := createTestContextEntity(t, tenantID)

			switch tt.fromStatus {
			case value_objects.ContextStatusDraft:
				// Already in DRAFT from constructor.
			case value_objects.ContextStatusActive:
				require.NoError(t, contextEntity.Activate(context.Background()))
			case value_objects.ContextStatusPaused:
				require.NoError(t, contextEntity.Activate(context.Background()))
				require.NoError(t, contextEntity.Pause(context.Background()))
			case value_objects.ContextStatusArchived:
				require.NoError(t, contextEntity.Activate(context.Background()))
				require.NoError(t, contextEntity.Archive(context.Background()))
			}

			require.Equal(t, tt.fromStatus, contextEntity.Status)

			// Apply the transition via Update().
			err := contextEntity.Update(context.Background(), UpdateReconciliationContextInput{
				Status: &tt.toStatus,
			})
			require.NoError(t, err, "%s -> %s must be a valid transition", tt.fromStatus, tt.toStatus)
			assert.Equal(t, tt.toStatus, contextEntity.Status)
		})
	}
}

// TestContextStateMachine_AllInvalidTransitions exhaustively tests every
// invalid state transition to ensure the guards are watertight.
func TestContextStateMachine_AllInvalidTransitions(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()

	tests := []struct {
		name       string
		fromStatus value_objects.ContextStatus
		toStatus   value_objects.ContextStatus
	}{
		{"DRAFT -> DRAFT", value_objects.ContextStatusDraft, value_objects.ContextStatusDraft},
		{"DRAFT -> PAUSED", value_objects.ContextStatusDraft, value_objects.ContextStatusPaused},
		{"DRAFT -> ARCHIVED", value_objects.ContextStatusDraft, value_objects.ContextStatusArchived},
		{"ACTIVE -> DRAFT", value_objects.ContextStatusActive, value_objects.ContextStatusDraft},
		{"ACTIVE -> ACTIVE", value_objects.ContextStatusActive, value_objects.ContextStatusActive},
		{"PAUSED -> DRAFT", value_objects.ContextStatusPaused, value_objects.ContextStatusDraft},
		{"PAUSED -> PAUSED", value_objects.ContextStatusPaused, value_objects.ContextStatusPaused},
		{"ARCHIVED -> DRAFT", value_objects.ContextStatusArchived, value_objects.ContextStatusDraft},
		{"ARCHIVED -> ACTIVE", value_objects.ContextStatusArchived, value_objects.ContextStatusActive},
		{"ARCHIVED -> PAUSED", value_objects.ContextStatusArchived, value_objects.ContextStatusPaused},
		{"ARCHIVED -> ARCHIVED", value_objects.ContextStatusArchived, value_objects.ContextStatusArchived},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Build context entity in the desired fromStatus.
			contextEntity := createTestContextEntity(t, tenantID)

			switch tt.fromStatus {
			case value_objects.ContextStatusDraft:
				// Already in DRAFT from constructor.
			case value_objects.ContextStatusActive:
				require.NoError(t, contextEntity.Activate(context.Background()))
			case value_objects.ContextStatusPaused:
				require.NoError(t, contextEntity.Activate(context.Background()))
				require.NoError(t, contextEntity.Pause(context.Background()))
			case value_objects.ContextStatusArchived:
				require.NoError(t, contextEntity.Activate(context.Background()))
				require.NoError(t, contextEntity.Archive(context.Background()))
			}

			require.Equal(t, tt.fromStatus, contextEntity.Status)

			// Apply the invalid transition.
			err := contextEntity.Update(context.Background(), UpdateReconciliationContextInput{
				Status: &tt.toStatus,
			})

			switch {
			case tt.fromStatus == value_objects.ContextStatusArchived:
				// Archived contexts reject ALL updates (not just status changes).
				require.ErrorIs(t, err, ErrArchivedContextCannotBeModified,
					"%s -> %s must be rejected for archived contexts", tt.fromStatus, tt.toStatus)
			case tt.toStatus == value_objects.ContextStatusDraft:
				// DRAFT is not a valid target in updateStatus()'s switch (no case for it),
				// so it falls through to the default branch returning ErrContextStatusInvalid.
				require.ErrorIs(t, err, ErrContextStatusInvalid,
					"%s -> %s must be rejected as invalid target status", tt.fromStatus, tt.toStatus)
			default:
				require.ErrorIs(t, err, ErrInvalidStateTransition,
					"%s -> %s must be an invalid transition", tt.fromStatus, tt.toStatus)
			}
		})
	}
}

func TestReconciliationContext_NilReceiverGuards(t *testing.T) {
	t.Parallel()

	var nilContext *ReconciliationContext

	t.Run("Pause returns error on nil receiver", func(t *testing.T) {
		t.Parallel()
		err := nilContext.Pause(context.Background())
		require.Error(t, err)
		assert.Equal(t, ErrNilReconciliationContext, err)
	})

	t.Run("Activate returns error on nil receiver", func(t *testing.T) {
		t.Parallel()
		err := nilContext.Activate(context.Background())
		require.Error(t, err)
		assert.Equal(t, ErrNilReconciliationContext, err)
	})

	t.Run("Archive returns error on nil receiver", func(t *testing.T) {
		t.Parallel()
		err := nilContext.Archive(context.Background())
		require.Error(t, err)
		assert.Equal(t, ErrNilReconciliationContext, err)
	})

	t.Run("IsActive returns false on nil receiver", func(t *testing.T) {
		t.Parallel()
		assert.False(t, nilContext.IsActive())
	})

	t.Run("IsDraft returns false on nil receiver", func(t *testing.T) {
		t.Parallel()
		assert.False(t, nilContext.IsDraft())
	})

	t.Run("IsArchived returns false on nil receiver", func(t *testing.T) {
		t.Parallel()
		assert.False(t, nilContext.IsArchived())
	})
}
