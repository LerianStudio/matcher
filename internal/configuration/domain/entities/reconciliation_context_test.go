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
