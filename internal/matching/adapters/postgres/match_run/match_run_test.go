//go:build unit

package match_run

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
)

func TestPostgreSQLModel_RoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	run, err := matchingEntities.NewMatchRun(ctx, uuid.New(), matchingVO.MatchRunModeCommit)
	require.NoError(t, err)

	reason := "boom"
	require.NoError(t, run.Fail(ctx, reason))

	model, err := NewPostgreSQLModel(run)
	require.NoError(t, err)

	again, err := model.ToEntity()
	require.NoError(t, err)

	require.Equal(t, run.ID, again.ID)
	require.Equal(t, run.ContextID, again.ContextID)
	require.Equal(t, run.Mode, again.Mode)
	require.Equal(t, run.Status, again.Status)
	require.NotNil(t, again.FailureReason)
	require.Equal(t, reason, *again.FailureReason)
}

func TestPostgreSQLModel_ToEntity_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		model   *PostgreSQLModel
		wantErr string
	}{
		{name: "nil model", model: nil, wantErr: ErrMatchRunModelNeeded.Error()},
		{name: "invalid id", model: &PostgreSQLModel{ID: "not-a-uuid"}, wantErr: "parse id"},
		{
			name:    "invalid context id",
			model:   &PostgreSQLModel{ID: uuid.NewString(), ContextID: "not-a-uuid"},
			wantErr: "parse context id",
		},
		{
			name: "invalid mode",
			model: &PostgreSQLModel{
				ID:        uuid.NewString(),
				ContextID: uuid.NewString(),
				Mode:      "BAD",
			},
			wantErr: "parse mode",
		},
		{
			name: "invalid status",
			model: &PostgreSQLModel{
				ID:        uuid.NewString(),
				ContextID: uuid.NewString(),
				Mode:      matchingVO.MatchRunModeCommit.String(),
				Status:    "BAD",
			},
			wantErr: "parse status",
		},
		{
			name: "invalid stats",
			model: &PostgreSQLModel{
				ID:        uuid.NewString(),
				ContextID: uuid.NewString(),
				Mode:      matchingVO.MatchRunModeCommit.String(),
				Status:    matchingVO.MatchRunStatusProcessing.String(),
				Stats:     []byte("not-json"),
			},
			wantErr: "unmarshal stats",
		},
	}

	for i := range tests {
		tc := tests[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := tc.model.ToEntity()
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestPostgreSQLModel_RoundTrip_Timestamps(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	run, err := matchingEntities.NewMatchRun(ctx, uuid.New(), matchingVO.MatchRunModeCommit)
	require.NoError(t, err)
	require.NoError(t, run.Complete(ctx, map[string]int{"matches": 1}))

	model, err := NewPostgreSQLModel(run)
	require.NoError(t, err)

	again, err := model.ToEntity()
	require.NoError(t, err)

	require.True(t, again.StartedAt.Equal(run.StartedAt))
	require.NotNil(t, again.CompletedAt)
	require.NotNil(t, run.CompletedAt)
	require.True(t, again.CompletedAt.Equal(*run.CompletedAt))
	require.Nil(t, again.FailureReason)
	require.True(t, again.CreatedAt.Equal(run.CreatedAt))
	require.True(t, again.UpdatedAt.Equal(run.UpdatedAt))
}

func TestPostgreSQLModel_RoundTrip_Processing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	run, err := matchingEntities.NewMatchRun(ctx, uuid.New(), matchingVO.MatchRunModeCommit)
	require.NoError(t, err)

	// Intentional direct assignment to exercise persistence model conversion
	// without requiring a domain state transition method.
	run.Status = matchingVO.MatchRunStatusProcessing
	model, err := NewPostgreSQLModel(run)
	require.NoError(t, err)

	again, err := model.ToEntity()
	require.NoError(t, err)

	require.Equal(t, matchingVO.MatchRunStatusProcessing, again.Status)
	require.Nil(t, again.CompletedAt)
	require.Nil(t, again.FailureReason)
}

func TestNewPostgreSQLModel_NilInput(t *testing.T) {
	t.Parallel()

	model, err := NewPostgreSQLModel(nil)
	require.Nil(t, model)
	require.ErrorIs(t, err, ErrMatchRunEntityNeeded)
}
