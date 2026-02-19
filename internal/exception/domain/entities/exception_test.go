//go:build unit

package entities_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
)

func TestNewExceptionDefaults(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	reason := "  FX_RATE_UNAVAILABLE  "

	exception, err := entities.NewException(
		ctx,
		uuid.New(),
		value_objects.ExceptionSeverityMedium,
		&reason,
	)
	require.NoError(t, err)
	require.Equal(t, value_objects.ExceptionStatusOpen, exception.Status)
	require.Equal(t, value_objects.ExceptionSeverityMedium, exception.Severity)
	require.NotNil(t, exception.Reason)
	require.Equal(t, "FX_RATE_UNAVAILABLE", *exception.Reason)
	require.Nil(t, exception.AssignedTo)
	require.Nil(t, exception.ResolutionNotes)
	require.False(t, exception.CreatedAt.IsZero())
	require.False(t, exception.UpdatedAt.IsZero())
}

func TestNewExceptionValidation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	_, err := entities.NewException(ctx, uuid.Nil, value_objects.ExceptionSeverityMedium, nil)
	require.ErrorIs(t, err, entities.ErrTransactionIDRequired)

	_, err = entities.NewException(ctx, uuid.New(), value_objects.ExceptionSeverity("BAD"), nil)
	require.ErrorIs(t, err, entities.ErrInvalidExceptionSeverity)
}

func TestExceptionAssignAndResolveLifecycle(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	exception, err := entities.NewException(
		ctx,
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	dueAt := time.Now().UTC().Add(24 * time.Hour)
	require.NoError(t, exception.Assign(ctx, " analyst-1 ", &dueAt))
	require.Equal(t, value_objects.ExceptionStatusAssigned, exception.Status)
	require.NotNil(t, exception.AssignedTo)
	require.Equal(t, "analyst-1", *exception.AssignedTo)
	require.NotNil(t, exception.DueAt)

	require.NoError(t, exception.Resolve(ctx, "resolved after review"))
	require.Equal(t, value_objects.ExceptionStatusResolved, exception.Status)
	require.NotNil(t, exception.ResolutionNotes)
	require.Equal(t, "resolved after review", *exception.ResolutionNotes)
}

func TestExceptionResolveFromOpen(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	exception, err := entities.NewException(
		ctx,
		uuid.New(),
		value_objects.ExceptionSeverityLow,
		nil,
	)
	require.NoError(t, err)

	require.NoError(t, exception.Resolve(ctx, "auto-resolved"))
	require.Equal(t, value_objects.ExceptionStatusResolved, exception.Status)
	require.NotNil(t, exception.ResolutionNotes)
	require.Equal(t, "auto-resolved", *exception.ResolutionNotes)
}

func TestExceptionAssignValidation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	exception, err := entities.NewException(
		ctx,
		uuid.New(),
		value_objects.ExceptionSeverityLow,
		nil,
	)
	require.NoError(t, err)

	previousStatus := exception.Status
	require.ErrorIs(t, exception.Assign(ctx, "   ", nil), entities.ErrAssigneeRequired)
	require.Equal(t, previousStatus, exception.Status)

	require.NoError(t, exception.Resolve(ctx, "done"))
	require.ErrorIs(
		t,
		exception.Assign(ctx, "analyst-2", nil),
		entities.ErrExceptionMustBeOpenToAssign,
	)

	var nilException *entities.Exception
	require.ErrorIs(t, nilException.Assign(ctx, "analyst-1", nil), entities.ErrExceptionNil)
}

func TestExceptionResolveValidation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	exception, err := entities.NewException(
		ctx,
		uuid.New(),
		value_objects.ExceptionSeverityLow,
		nil,
	)
	require.NoError(t, err)

	require.ErrorIs(t, exception.Resolve(ctx, "  "), entities.ErrResolutionNotesRequired)

	require.NoError(t, exception.Assign(ctx, "analyst-1", nil))
	exception.AssignedTo = nil
	require.ErrorIs(
		t,
		exception.Resolve(ctx, "notes"),
		entities.ErrAssignedExceptionRequiresAssignee,
	)

	var nilException *entities.Exception
	require.ErrorIs(t, nilException.Resolve(ctx, "notes"), entities.ErrExceptionNil)
}

func TestException_StateTransitions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		name          string
		initialStatus value_objects.ExceptionStatus
		transition    string
		wantStatus    value_objects.ExceptionStatus
		wantErr       error
	}{
		{
			name:          "OPEN to ASSIGNED via Assign",
			initialStatus: value_objects.ExceptionStatusOpen,
			transition:    "assign",
			wantStatus:    value_objects.ExceptionStatusAssigned,
			wantErr:       nil,
		},
		{
			name:          "OPEN to RESOLVED via Resolve",
			initialStatus: value_objects.ExceptionStatusOpen,
			transition:    "resolve",
			wantStatus:    value_objects.ExceptionStatusResolved,
			wantErr:       nil,
		},
		{
			name:          "ASSIGNED to RESOLVED via Resolve",
			initialStatus: value_objects.ExceptionStatusAssigned,
			transition:    "resolve",
			wantStatus:    value_objects.ExceptionStatusResolved,
			wantErr:       nil,
		},
		{
			name:          "ASSIGNED to ASSIGNED via Assign is invalid",
			initialStatus: value_objects.ExceptionStatusAssigned,
			transition:    "assign",
			wantStatus:    value_objects.ExceptionStatusAssigned,
			wantErr:       entities.ErrExceptionMustBeOpenToAssign,
		},
		{
			name:          "RESOLVED to ASSIGNED via Assign is invalid",
			initialStatus: value_objects.ExceptionStatusResolved,
			transition:    "assign",
			wantStatus:    value_objects.ExceptionStatusResolved,
			wantErr:       entities.ErrExceptionMustBeOpenToAssign,
		},
		{
			name:          "RESOLVED to RESOLVED via Resolve is invalid",
			initialStatus: value_objects.ExceptionStatusResolved,
			transition:    "resolve",
			wantStatus:    value_objects.ExceptionStatusResolved,
			wantErr:       entities.ErrExceptionMustBeOpenOrAssignedToResolve,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			exception, err := entities.NewException(
				ctx,
				uuid.New(),
				value_objects.ExceptionSeverityMedium,
				nil,
			)
			require.NoError(t, err)

			switch tc.initialStatus {
			case value_objects.ExceptionStatusOpen:
				// Already in OPEN status from NewException
			case value_objects.ExceptionStatusAssigned:
				require.NoError(t, exception.Assign(ctx, "setup-analyst", nil))
			case value_objects.ExceptionStatusResolved:
				require.NoError(t, exception.Resolve(ctx, "setup resolution"))
			}

			require.Equal(t, tc.initialStatus, exception.Status)

			var transitionErr error

			switch tc.transition {
			case "assign":
				transitionErr = exception.Assign(ctx, "new-analyst", nil)
			case "resolve":
				transitionErr = exception.Resolve(ctx, "resolution notes")
			}

			if tc.wantErr != nil {
				require.ErrorIs(t, transitionErr, tc.wantErr)
				require.Equal(t, tc.initialStatus, exception.Status)
			} else {
				require.NoError(t, transitionErr)
				require.Equal(t, tc.wantStatus, exception.Status)
			}
		})
	}
}

func TestException_Assign_IdempotentFields(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("assignee is set correctly", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityHigh,
			nil,
		)
		require.NoError(t, err)
		require.Nil(t, exception.AssignedTo)

		require.NoError(t, exception.Assign(ctx, "analyst-alpha", nil))
		require.NotNil(t, exception.AssignedTo)
		require.Equal(t, "analyst-alpha", *exception.AssignedTo)
	})

	t.Run("assignee is trimmed", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityHigh,
			nil,
		)
		require.NoError(t, err)

		require.NoError(t, exception.Assign(ctx, "  analyst-beta  ", nil))
		require.NotNil(t, exception.AssignedTo)
		require.Equal(t, "analyst-beta", *exception.AssignedTo)
	})

	t.Run("dueAt is set when provided", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityHigh,
			nil,
		)
		require.NoError(t, err)
		require.Nil(t, exception.DueAt)

		dueAt := time.Now().UTC().Add(48 * time.Hour)
		require.NoError(t, exception.Assign(ctx, "analyst-gamma", &dueAt))
		require.NotNil(t, exception.DueAt)
		require.Equal(t, dueAt, *exception.DueAt)
	})

	t.Run("dueAt is nil when not provided", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityHigh,
			nil,
		)
		require.NoError(t, err)

		require.NoError(t, exception.Assign(ctx, "analyst-delta", nil))
		require.Nil(t, exception.DueAt)
	})
}

func TestException_Resolve_PreservesAssignee(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("assignee preserved after ASSIGNED to RESOLVED", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityMedium,
			nil,
		)
		require.NoError(t, err)

		dueAt := time.Now().UTC().Add(24 * time.Hour)
		require.NoError(t, exception.Assign(ctx, "original-analyst", &dueAt))
		require.NotNil(t, exception.AssignedTo)
		require.Equal(t, "original-analyst", *exception.AssignedTo)

		require.NoError(t, exception.Resolve(ctx, "issue resolved"))
		require.Equal(t, value_objects.ExceptionStatusResolved, exception.Status)
		require.NotNil(t, exception.AssignedTo)
		require.Equal(t, "original-analyst", *exception.AssignedTo)
		require.NotNil(t, exception.DueAt)
		require.Equal(t, dueAt, *exception.DueAt)
	})

	t.Run("assignee remains nil after OPEN to RESOLVED", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityLow,
			nil,
		)
		require.NoError(t, err)
		require.Nil(t, exception.AssignedTo)

		require.NoError(t, exception.Resolve(ctx, "auto-resolved"))
		require.Equal(t, value_objects.ExceptionStatusResolved, exception.Status)
		require.Nil(t, exception.AssignedTo)
	})
}

func TestException_TimestampInvariants(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("CreatedAt never changes on Assign", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityMedium,
			nil,
		)
		require.NoError(t, err)

		originalCreatedAt := exception.CreatedAt

		require.NoError(t, exception.Assign(ctx, "analyst", nil))

		require.Equal(t, originalCreatedAt, exception.CreatedAt)
	})

	t.Run("CreatedAt never changes on Resolve", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityMedium,
			nil,
		)
		require.NoError(t, err)

		originalCreatedAt := exception.CreatedAt

		require.NoError(t, exception.Resolve(ctx, "resolved"))

		require.Equal(t, originalCreatedAt, exception.CreatedAt)
	})

	t.Run("UpdatedAt changes on Assign", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityMedium,
			nil,
		)
		require.NoError(t, err)

		originalUpdatedAt := exception.UpdatedAt

		// Ensure time.Now() advances (sub-millisecond precision not guaranteed on all platforms)
		time.Sleep(1 * time.Millisecond)
		require.NoError(t, exception.Assign(ctx, "analyst", nil))

		require.True(t, exception.UpdatedAt.After(originalUpdatedAt))
	})

	t.Run("UpdatedAt changes on Resolve", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityMedium,
			nil,
		)
		require.NoError(t, err)

		originalUpdatedAt := exception.UpdatedAt

		// Ensure time.Now() advances (sub-millisecond precision not guaranteed on all platforms)
		time.Sleep(1 * time.Millisecond)
		require.NoError(t, exception.Resolve(ctx, "resolved"))

		require.True(t, exception.UpdatedAt.After(originalUpdatedAt))
	})

	t.Run("UpdatedAt changes on each transition", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityHigh,
			nil,
		)
		require.NoError(t, err)

		createdAt := exception.CreatedAt
		updatedAfterCreate := exception.UpdatedAt

		// Ensure time.Now() advances (sub-millisecond precision not guaranteed on all platforms)
		time.Sleep(1 * time.Millisecond)
		require.NoError(t, exception.Assign(ctx, "analyst", nil))
		updatedAfterAssign := exception.UpdatedAt

		require.Equal(t, createdAt, exception.CreatedAt)
		require.True(t, updatedAfterAssign.After(updatedAfterCreate))

		// Ensure time.Now() advances (sub-millisecond precision not guaranteed on all platforms)
		time.Sleep(1 * time.Millisecond)
		require.NoError(t, exception.Resolve(ctx, "resolved"))
		updatedAfterResolve := exception.UpdatedAt

		require.Equal(t, createdAt, exception.CreatedAt)
		require.True(t, updatedAfterResolve.After(updatedAfterAssign))
	})
}

func TestException_Repeated_Transitions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("multiple assign attempts on same exception", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityHigh,
			nil,
		)
		require.NoError(t, err)

		const attempts = 10

		var successCount int

		for i := 0; i < attempts; i++ {
			assignee := "analyst-" + string(rune('A'+i))

			err := exception.Assign(ctx, assignee, nil)
			if err == nil {
				successCount++
			} else {
				require.ErrorIs(t, err, entities.ErrExceptionMustBeOpenToAssign)
			}
		}

		require.Equal(t, value_objects.ExceptionStatusAssigned, exception.Status)
		require.NotNil(t, exception.AssignedTo)
		require.GreaterOrEqual(t, successCount, 1)
	})

	t.Run("multiple resolve attempts on same exception", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityMedium,
			nil,
		)
		require.NoError(t, err)

		const attempts = 10

		var successCount int

		for i := 0; i < attempts; i++ {
			notes := "resolved attempt " + string(rune('A'+i))

			err := exception.Resolve(ctx, notes)
			if err == nil {
				successCount++
			} else {
				require.ErrorIs(t, err, entities.ErrExceptionMustBeOpenOrAssignedToResolve)
			}
		}

		require.Equal(t, value_objects.ExceptionStatusResolved, exception.Status)
		require.NotNil(t, exception.ResolutionNotes)
		require.GreaterOrEqual(t, successCount, 1)
	})

	t.Run("mixed transitions on same exception", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityLow,
			nil,
		)
		require.NoError(t, err)

		const attempts = 20

		var transitionErrors []string

		for i := 0; i < attempts; i++ {
			if i%2 == 0 {
				if err := exception.Assign(ctx, "analyst", nil); err != nil {
					t.Logf("iteration %d: Assign failed: %v", i, err)
					transitionErrors = append(transitionErrors, fmt.Sprintf("iteration %d: Assign: %v", i, err))
				}
			} else {
				if err := exception.Resolve(ctx, "resolved"); err != nil {
					t.Logf("iteration %d: Resolve failed: %v", i, err)
					transitionErrors = append(transitionErrors, fmt.Sprintf("iteration %d: Resolve: %v", i, err))
				}
			}
		}

		t.Logf("total transition errors: %d/%d", len(transitionErrors), attempts)

		require.True(t,
			exception.Status == value_objects.ExceptionStatusAssigned ||
				exception.Status == value_objects.ExceptionStatusResolved,
			"final status should be ASSIGNED or RESOLVED, got: %s", exception.Status)
	})
}

func TestException_Unassign(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("ASSIGNED to OPEN via Unassign", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityMedium,
			nil,
		)
		require.NoError(t, err)

		dueAt := time.Now().UTC().Add(48 * time.Hour)
		require.NoError(t, exception.Assign(ctx, "analyst-1", &dueAt))
		require.Equal(t, value_objects.ExceptionStatusAssigned, exception.Status)
		require.NotNil(t, exception.AssignedTo)
		require.NotNil(t, exception.DueAt)

		require.NoError(t, exception.Unassign(ctx))
		require.Equal(t, value_objects.ExceptionStatusOpen, exception.Status)
		require.Nil(t, exception.AssignedTo)
		require.Nil(t, exception.DueAt)
	})

	t.Run("Unassign on OPEN returns error", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityLow,
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, value_objects.ExceptionStatusOpen, exception.Status)

		require.ErrorIs(t, exception.Unassign(ctx), entities.ErrExceptionMustBeAssignedToUnassign)
		require.Equal(t, value_objects.ExceptionStatusOpen, exception.Status)
	})

	t.Run("Unassign on RESOLVED returns error", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityLow,
			nil,
		)
		require.NoError(t, err)
		require.NoError(t, exception.Resolve(ctx, "auto-resolved"))
		require.Equal(t, value_objects.ExceptionStatusResolved, exception.Status)

		require.ErrorIs(t, exception.Unassign(ctx), entities.ErrExceptionMustBeAssignedToUnassign)
		require.Equal(t, value_objects.ExceptionStatusResolved, exception.Status)
	})

	t.Run("Unassign on nil exception returns error", func(t *testing.T) {
		t.Parallel()

		var nilException *entities.Exception
		require.ErrorIs(t, nilException.Unassign(ctx), entities.ErrExceptionNil)
	})

	t.Run("Unassign updates timestamp", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityHigh,
			nil,
		)
		require.NoError(t, err)

		require.NoError(t, exception.Assign(ctx, "analyst", nil))
		updatedAfterAssign := exception.UpdatedAt

		// Ensure time.Now() advances (sub-millisecond precision not guaranteed on all platforms)
		time.Sleep(1 * time.Millisecond)
		require.NoError(t, exception.Unassign(ctx))

		require.True(t, exception.UpdatedAt.After(updatedAfterAssign))
	})

	t.Run("Unassign preserves CreatedAt", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityMedium,
			nil,
		)
		require.NoError(t, err)

		originalCreatedAt := exception.CreatedAt

		require.NoError(t, exception.Assign(ctx, "analyst", nil))
		require.NoError(t, exception.Unassign(ctx))

		require.Equal(t, originalCreatedAt, exception.CreatedAt)
	})
}

func TestException_Unassign_ThenReassign(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("can reassign after unassign", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityHigh,
			nil,
		)
		require.NoError(t, err)

		require.NoError(t, exception.Assign(ctx, "analyst-1", nil))
		require.Equal(t, "analyst-1", *exception.AssignedTo)

		require.NoError(t, exception.Unassign(ctx))
		require.Nil(t, exception.AssignedTo)

		require.NoError(t, exception.Assign(ctx, "analyst-2", nil))
		require.Equal(t, "analyst-2", *exception.AssignedTo)
		require.Equal(t, value_objects.ExceptionStatusAssigned, exception.Status)
	})

	t.Run("full lifecycle: assign -> unassign -> assign -> resolve", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityMedium,
			nil,
		)
		require.NoError(t, err)

		require.NoError(t, exception.Assign(ctx, "analyst-alpha", nil))
		require.NoError(t, exception.Unassign(ctx))
		require.NoError(t, exception.Assign(ctx, "analyst-beta", nil))
		require.NoError(t, exception.Resolve(ctx, "final resolution"))

		require.Equal(t, value_objects.ExceptionStatusResolved, exception.Status)
		require.Equal(t, "analyst-beta", *exception.AssignedTo)
		require.Equal(t, "final resolution", *exception.ResolutionNotes)
	})
}

func TestException_Resolve_WithOptions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("resolve with resolution type", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityMedium,
			nil,
		)
		require.NoError(t, err)

		require.NoError(t, exception.Resolve(ctx, "manual fix applied",
			entities.WithResolutionType("MANUAL_ADJUSTMENT"),
		))

		require.Equal(t, value_objects.ExceptionStatusResolved, exception.Status)
		require.NotNil(t, exception.ResolutionType)
		require.Equal(t, "MANUAL_ADJUSTMENT", *exception.ResolutionType)
		require.Nil(t, exception.ResolutionReason)
	})

	t.Run("resolve with resolution reason", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityLow,
			nil,
		)
		require.NoError(t, err)

		require.NoError(t, exception.Resolve(ctx, "data corrected",
			entities.WithResolutionReason("DATA_ENTRY_ERROR"),
		))

		require.Equal(t, value_objects.ExceptionStatusResolved, exception.Status)
		require.Nil(t, exception.ResolutionType)
		require.NotNil(t, exception.ResolutionReason)
		require.Equal(t, "DATA_ENTRY_ERROR", *exception.ResolutionReason)
	})

	t.Run("resolve with both type and reason", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityHigh,
			nil,
		)
		require.NoError(t, err)

		require.NoError(t, exception.Resolve(ctx, "full correction applied",
			entities.WithResolutionType("OVERRIDE"),
			entities.WithResolutionReason("APPROVED_BY_SUPERVISOR"),
		))

		require.Equal(t, value_objects.ExceptionStatusResolved, exception.Status)
		require.NotNil(t, exception.ResolutionType)
		require.Equal(t, "OVERRIDE", *exception.ResolutionType)
		require.NotNil(t, exception.ResolutionReason)
		require.Equal(t, "APPROVED_BY_SUPERVISOR", *exception.ResolutionReason)
	})

	t.Run("resolve with empty options leaves fields nil", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityMedium,
			nil,
		)
		require.NoError(t, err)

		require.NoError(t, exception.Resolve(ctx, "simple resolution",
			entities.WithResolutionType(""),
			entities.WithResolutionReason(""),
		))

		require.Equal(t, value_objects.ExceptionStatusResolved, exception.Status)
		require.Nil(t, exception.ResolutionType)
		require.Nil(t, exception.ResolutionReason)
	})

	t.Run("resolve from assigned with options", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityCritical,
			nil,
		)
		require.NoError(t, err)

		require.NoError(t, exception.Assign(ctx, "senior-analyst", nil))

		require.NoError(t, exception.Resolve(ctx, "critical issue fixed",
			entities.WithResolutionType("EMERGENCY_FIX"),
			entities.WithResolutionReason("PRODUCTION_INCIDENT"),
		))

		require.Equal(t, value_objects.ExceptionStatusResolved, exception.Status)
		require.Equal(t, "senior-analyst", *exception.AssignedTo)
		require.Equal(t, "EMERGENCY_FIX", *exception.ResolutionType)
		require.Equal(t, "PRODUCTION_INCIDENT", *exception.ResolutionReason)
	})
}

func TestNewException_ReasonNormalization(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("nil reason stays nil", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityLow,
			nil,
		)
		require.NoError(t, err)
		require.Nil(t, exception.Reason)
	})

	t.Run("empty string reason becomes nil", func(t *testing.T) {
		t.Parallel()

		emptyReason := ""
		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityLow,
			&emptyReason,
		)
		require.NoError(t, err)
		require.Nil(t, exception.Reason)
	})

	t.Run("whitespace-only reason becomes nil", func(t *testing.T) {
		t.Parallel()

		whitespaceReason := "   \t\n  "
		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityLow,
			&whitespaceReason,
		)
		require.NoError(t, err)
		require.Nil(t, exception.Reason)
	})

	t.Run("reason with leading/trailing whitespace is trimmed", func(t *testing.T) {
		t.Parallel()

		reason := "  DUPLICATE_TRANSACTION  "
		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityMedium,
			&reason,
		)
		require.NoError(t, err)
		require.NotNil(t, exception.Reason)
		require.Equal(t, "DUPLICATE_TRANSACTION", *exception.Reason)
	})

	t.Run("reason with internal whitespace is preserved", func(t *testing.T) {
		t.Parallel()

		reason := "multiple word reason"
		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityHigh,
			&reason,
		)
		require.NoError(t, err)
		require.NotNil(t, exception.Reason)
		require.Equal(t, "multiple word reason", *exception.Reason)
	})
}

func TestNewException_AllSeverityLevels(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	severities := []value_objects.ExceptionSeverity{
		value_objects.ExceptionSeverityLow,
		value_objects.ExceptionSeverityMedium,
		value_objects.ExceptionSeverityHigh,
		value_objects.ExceptionSeverityCritical,
	}

	for _, severity := range severities {
		t.Run("creates exception with "+severity.String()+" severity", func(t *testing.T) {
			t.Parallel()

			exception, err := entities.NewException(ctx, uuid.New(), severity, nil)
			require.NoError(t, err)
			require.Equal(t, severity, exception.Severity)
			require.Equal(t, value_objects.ExceptionStatusOpen, exception.Status)
		})
	}
}

func TestException_Assign_EmptyAssigneeVariants(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	emptyVariants := []struct {
		name     string
		assignee string
	}{
		{"empty string", ""},
		{"single space", " "},
		{"multiple spaces", "     "},
		{"tabs", "\t\t"},
		{"newlines", "\n\n"},
		{"mixed whitespace", " \t\n "},
	}

	for _, tc := range emptyVariants {
		t.Run(tc.name+" returns error", func(t *testing.T) {
			t.Parallel()

			exception, err := entities.NewException(
				ctx,
				uuid.New(),
				value_objects.ExceptionSeverityLow,
				nil,
			)
			require.NoError(t, err)

			err = exception.Assign(ctx, tc.assignee, nil)
			require.ErrorIs(t, err, entities.ErrAssigneeRequired)
			require.Equal(t, value_objects.ExceptionStatusOpen, exception.Status)
		})
	}
}

func TestException_Resolve_EmptyNotesVariants(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	emptyVariants := []struct {
		name  string
		notes string
	}{
		{"empty string", ""},
		{"single space", " "},
		{"multiple spaces", "     "},
		{"tabs", "\t\t"},
		{"newlines", "\n\n"},
		{"mixed whitespace", " \t\n "},
	}

	for _, tc := range emptyVariants {
		t.Run(tc.name+" returns error", func(t *testing.T) {
			t.Parallel()

			exception, err := entities.NewException(
				ctx,
				uuid.New(),
				value_objects.ExceptionSeverityMedium,
				nil,
			)
			require.NoError(t, err)

			err = exception.Resolve(ctx, tc.notes)
			require.ErrorIs(t, err, entities.ErrResolutionNotesRequired)
			require.Equal(t, value_objects.ExceptionStatusOpen, exception.Status)
		})
	}
}

func TestException_Resolve_NotesTrimming(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("notes with leading/trailing whitespace are trimmed", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityLow,
			nil,
		)
		require.NoError(t, err)

		require.NoError(t, exception.Resolve(ctx, "  resolved with spaces  "))
		require.NotNil(t, exception.ResolutionNotes)
		require.Equal(t, "resolved with spaces", *exception.ResolutionNotes)
	})

	t.Run("notes with internal whitespace are preserved", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityMedium,
			nil,
		)
		require.NoError(t, err)

		require.NoError(t, exception.Resolve(ctx, "multi line\nresolution notes"))
		require.NotNil(t, exception.ResolutionNotes)
		require.Equal(t, "multi line\nresolution notes", *exception.ResolutionNotes)
	})
}

func TestException_AssignedExceptionWithEmptyAssignee(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("assigned with empty string assignee cannot resolve", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityHigh,
			nil,
		)
		require.NoError(t, err)

		require.NoError(t, exception.Assign(ctx, "valid-analyst", nil))
		emptyAssignee := ""
		exception.AssignedTo = &emptyAssignee

		err = exception.Resolve(ctx, "trying to resolve")
		require.ErrorIs(t, err, entities.ErrAssignedExceptionRequiresAssignee)
	})

	t.Run("assigned with whitespace-only assignee cannot resolve", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityMedium,
			nil,
		)
		require.NoError(t, err)

		require.NoError(t, exception.Assign(ctx, "valid-analyst", nil))
		whitespaceAssignee := "   "
		exception.AssignedTo = &whitespaceAssignee

		err = exception.Resolve(ctx, "trying to resolve")
		require.ErrorIs(t, err, entities.ErrAssignedExceptionRequiresAssignee)
	})
}

func TestNewException_IDGeneration(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("generates unique IDs for each exception", func(t *testing.T) {
		t.Parallel()

		const count = 100
		ids := make(map[uuid.UUID]bool)

		for i := 0; i < count; i++ {
			exception, err := entities.NewException(
				ctx,
				uuid.New(),
				value_objects.ExceptionSeverityLow,
				nil,
			)
			require.NoError(t, err)
			require.False(t, ids[exception.ID], "duplicate ID generated")
			ids[exception.ID] = true
		}

		require.Len(t, ids, count)
	})

	t.Run("ID is not nil", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityMedium,
			nil,
		)
		require.NoError(t, err)
		require.NotEqual(t, uuid.Nil, exception.ID)
	})
}

func TestNewException_TransactionIDPreserved(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("transaction ID is preserved", func(t *testing.T) {
		t.Parallel()

		txID := uuid.New()
		exception, err := entities.NewException(ctx, txID, value_objects.ExceptionSeverityHigh, nil)
		require.NoError(t, err)
		require.Equal(t, txID, exception.TransactionID)
	})
}

func TestException_Version(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("new exception has version 0", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityLow,
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, int64(0), exception.Version)
	})
}

func TestException_ExternalSystemFields(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("external fields are nil on creation", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityMedium,
			nil,
		)
		require.NoError(t, err)

		require.Nil(t, exception.ExternalSystem)
		require.Nil(t, exception.ExternalIssueID)
	})
}

func TestException_StartResolution(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("OPEN to PENDING_RESOLUTION", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityMedium,
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, value_objects.ExceptionStatusOpen, exception.Status)

		require.NoError(t, exception.StartResolution(ctx))
		require.Equal(t, value_objects.ExceptionStatusPendingResolution, exception.Status)
	})

	t.Run("ASSIGNED to PENDING_RESOLUTION", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityHigh,
			nil,
		)
		require.NoError(t, err)
		require.NoError(t, exception.Assign(ctx, "analyst-1", nil))
		require.Equal(t, value_objects.ExceptionStatusAssigned, exception.Status)

		require.NoError(t, exception.StartResolution(ctx))
		require.Equal(t, value_objects.ExceptionStatusPendingResolution, exception.Status)
		require.NotNil(t, exception.AssignedTo)
		require.Equal(t, "analyst-1", *exception.AssignedTo)
	})

	t.Run("PENDING_RESOLUTION to PENDING_RESOLUTION is rejected", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityLow,
			nil,
		)
		require.NoError(t, err)
		require.NoError(t, exception.StartResolution(ctx))

		err = exception.StartResolution(ctx)
		require.ErrorIs(t, err, entities.ErrExceptionPendingResolution)
		require.Equal(t, value_objects.ExceptionStatusPendingResolution, exception.Status)
	})

	t.Run("RESOLVED to PENDING_RESOLUTION is rejected", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityLow,
			nil,
		)
		require.NoError(t, err)
		require.NoError(t, exception.Resolve(ctx, "done"))

		err = exception.StartResolution(ctx)
		require.ErrorIs(t, err, entities.ErrExceptionMustBeOpenOrAssignedToResolve)
	})

	t.Run("nil exception returns error", func(t *testing.T) {
		t.Parallel()

		var nilException *entities.Exception
		require.ErrorIs(t, nilException.StartResolution(ctx), entities.ErrExceptionNil)
	})

	t.Run("UpdatedAt changes on StartResolution", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityMedium,
			nil,
		)
		require.NoError(t, err)

		updatedBefore := exception.UpdatedAt

		// Ensure time.Now() advances (sub-millisecond precision not guaranteed on all platforms)
		time.Sleep(1 * time.Millisecond)

		require.NoError(t, exception.StartResolution(ctx))
		require.True(t, exception.UpdatedAt.After(updatedBefore))
	})
}

func TestException_AbortResolution(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("revert to OPEN from PENDING_RESOLUTION", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityMedium,
			nil,
		)
		require.NoError(t, err)
		require.NoError(t, exception.StartResolution(ctx))
		require.Equal(t, value_objects.ExceptionStatusPendingResolution, exception.Status)

		require.NoError(t, exception.AbortResolution(ctx, value_objects.ExceptionStatusOpen))
		require.Equal(t, value_objects.ExceptionStatusOpen, exception.Status)
	})

	t.Run("revert to ASSIGNED from PENDING_RESOLUTION", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityHigh,
			nil,
		)
		require.NoError(t, err)
		require.NoError(t, exception.Assign(ctx, "analyst-1", nil))
		require.NoError(t, exception.StartResolution(ctx))

		require.NoError(t, exception.AbortResolution(ctx, value_objects.ExceptionStatusAssigned))
		require.Equal(t, value_objects.ExceptionStatusAssigned, exception.Status)
		require.NotNil(t, exception.AssignedTo)
		require.Equal(t, "analyst-1", *exception.AssignedTo)
	})

	t.Run("abort from non-PENDING status is rejected", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityLow,
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, value_objects.ExceptionStatusOpen, exception.Status)

		err = exception.AbortResolution(ctx, value_objects.ExceptionStatusOpen)
		require.ErrorIs(t, err, entities.ErrExceptionMustBePendingToAbort)
	})

	t.Run("nil exception returns error", func(t *testing.T) {
		t.Parallel()

		var nilException *entities.Exception
		require.ErrorIs(t,
			nilException.AbortResolution(ctx, value_objects.ExceptionStatusOpen),
			entities.ErrExceptionNil,
		)
	})

	t.Run("UpdatedAt changes on AbortResolution", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityMedium,
			nil,
		)
		require.NoError(t, err)
		require.NoError(t, exception.StartResolution(ctx))

		updatedBefore := exception.UpdatedAt

		// Ensure time.Now() advances (sub-millisecond precision not guaranteed on all platforms)
		time.Sleep(1 * time.Millisecond)

		require.NoError(t, exception.AbortResolution(ctx, value_objects.ExceptionStatusOpen))
		require.True(t, exception.UpdatedAt.After(updatedBefore))
	})
}

func TestException_AbortResolution_InvalidTargetStatus(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	invalidTargets := []struct {
		name   string
		status value_objects.ExceptionStatus
	}{
		{"RESOLVED is not a valid abort target", value_objects.ExceptionStatusResolved},
		{"PENDING_RESOLUTION is not a valid abort target", value_objects.ExceptionStatusPendingResolution},
		{"arbitrary string is not a valid abort target", value_objects.ExceptionStatus("BOGUS")},
	}

	for _, tc := range invalidTargets {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			exception, err := entities.NewException(
				ctx,
				uuid.New(),
				value_objects.ExceptionSeverityMedium,
				nil,
			)
			require.NoError(t, err)
			require.NoError(t, exception.StartResolution(ctx))
			require.Equal(t, value_objects.ExceptionStatusPendingResolution, exception.Status)

			err = exception.AbortResolution(ctx, tc.status)
			require.ErrorIs(t, err, entities.ErrInvalidAbortTargetStatus)
			require.Equal(t, value_objects.ExceptionStatusPendingResolution, exception.Status,
				"status must remain PENDING_RESOLUTION when abort target is invalid")
		})
	}
}

func TestException_ResolveFromPendingResolution(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("PENDING_RESOLUTION to RESOLVED", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityMedium,
			nil,
		)
		require.NoError(t, err)
		require.NoError(t, exception.StartResolution(ctx))
		require.Equal(t, value_objects.ExceptionStatusPendingResolution, exception.Status)

		require.NoError(t, exception.Resolve(ctx, "resolved after pending"))
		require.Equal(t, value_objects.ExceptionStatusResolved, exception.Status)
		require.NotNil(t, exception.ResolutionNotes)
		require.Equal(t, "resolved after pending", *exception.ResolutionNotes)
	})

	t.Run("PENDING_RESOLUTION to RESOLVED with options", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityHigh,
			nil,
		)
		require.NoError(t, err)
		require.NoError(t, exception.StartResolution(ctx))

		require.NoError(t, exception.Resolve(ctx, "force match applied",
			entities.WithResolutionType("FORCE_MATCH"),
			entities.WithResolutionReason("POLICY_EXCEPTION"),
		))

		require.Equal(t, value_objects.ExceptionStatusResolved, exception.Status)
		require.NotNil(t, exception.ResolutionType)
		require.Equal(t, "FORCE_MATCH", *exception.ResolutionType)
		require.NotNil(t, exception.ResolutionReason)
		require.Equal(t, "POLICY_EXCEPTION", *exception.ResolutionReason)
	})
}

func TestException_FullPendingResolutionLifecycle(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("OPEN -> PENDING_RESOLUTION -> RESOLVED", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityMedium,
			nil,
		)
		require.NoError(t, err)

		require.NoError(t, exception.StartResolution(ctx))
		require.Equal(t, value_objects.ExceptionStatusPendingResolution, exception.Status)

		require.NoError(t, exception.Resolve(ctx, "successfully resolved"))
		require.Equal(t, value_objects.ExceptionStatusResolved, exception.Status)
	})

	t.Run("ASSIGNED -> PENDING_RESOLUTION -> abort -> ASSIGNED", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityHigh,
			nil,
		)
		require.NoError(t, err)
		require.NoError(t, exception.Assign(ctx, "analyst-1", nil))

		require.NoError(t, exception.StartResolution(ctx))
		require.Equal(t, value_objects.ExceptionStatusPendingResolution, exception.Status)

		require.NoError(t, exception.AbortResolution(ctx, value_objects.ExceptionStatusAssigned))
		require.Equal(t, value_objects.ExceptionStatusAssigned, exception.Status)
		require.Equal(t, "analyst-1", *exception.AssignedTo)
	})

	t.Run("OPEN -> PENDING_RESOLUTION -> abort -> OPEN -> PENDING_RESOLUTION -> RESOLVED", func(t *testing.T) {
		t.Parallel()

		exception, err := entities.NewException(
			ctx,
			uuid.New(),
			value_objects.ExceptionSeverityLow,
			nil,
		)
		require.NoError(t, err)

		// First attempt: gateway fails.
		require.NoError(t, exception.StartResolution(ctx))
		require.NoError(t, exception.AbortResolution(ctx, value_objects.ExceptionStatusOpen))
		require.Equal(t, value_objects.ExceptionStatusOpen, exception.Status)

		// Second attempt: gateway succeeds.
		require.NoError(t, exception.StartResolution(ctx))
		require.NoError(t, exception.Resolve(ctx, "resolved on retry"))
		require.Equal(t, value_objects.ExceptionStatusResolved, exception.Status)
	})
}
