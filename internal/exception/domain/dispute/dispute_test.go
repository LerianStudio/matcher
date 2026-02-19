//go:build unit

package dispute_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/exception/domain/dispute"
)

func TestNewDispute_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	exceptionID := uuid.New()

	testDispute, err := dispute.NewDispute(
		ctx,
		exceptionID,
		dispute.DisputeCategoryBankFeeError,
		"Incorrect fee charged",
		"analyst@example.com",
	)
	require.NoError(t, err)
	require.NotNil(t, testDispute)
	require.NotEqual(t, uuid.Nil, testDispute.ID)
	require.Equal(t, exceptionID, testDispute.ExceptionID)
	require.Equal(t, dispute.DisputeCategoryBankFeeError, testDispute.Category)
	require.Equal(t, dispute.DisputeStateDraft, testDispute.State)
	require.Equal(t, "Incorrect fee charged", testDispute.Description)
	require.Equal(t, "analyst@example.com", testDispute.OpenedBy)
	require.Nil(t, testDispute.Resolution)
	require.Empty(t, testDispute.Evidence)
	require.False(t, testDispute.CreatedAt.IsZero())
	require.False(t, testDispute.UpdatedAt.IsZero())
}

func TestNewDispute_TrimsWhitespace(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	exceptionID := uuid.New()

	testDispute, err := dispute.NewDispute(
		ctx,
		exceptionID,
		dispute.DisputeCategoryOther,
		"  Description with spaces  ",
		"  user@test.com  ",
	)
	require.NoError(t, err)
	require.NotNil(t, testDispute)
	require.Equal(t, "Description with spaces", testDispute.Description)
	require.Equal(t, "user@test.com", testDispute.OpenedBy)
}

func TestNewDispute_Validation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		name        string
		exceptionID uuid.UUID
		category    dispute.DisputeCategory
		description string
		openedBy    string
		wantErr     error
	}{
		{
			name:        "nil exception ID",
			exceptionID: uuid.Nil,
			category:    dispute.DisputeCategoryBankFeeError,
			description: "Valid description",
			openedBy:    "user@test.com",
			wantErr:     dispute.ErrDisputeExceptionIDRequired,
		},
		{
			name:        "invalid category",
			exceptionID: uuid.New(),
			category:    dispute.DisputeCategory("INVALID"),
			description: "Valid description",
			openedBy:    "user@test.com",
			wantErr:     dispute.ErrInvalidDisputeCategory,
		},
		{
			name:        "empty description",
			exceptionID: uuid.New(),
			category:    dispute.DisputeCategoryBankFeeError,
			description: "",
			openedBy:    "user@test.com",
			wantErr:     dispute.ErrDisputeDescriptionRequired,
		},
		{
			name:        "whitespace-only description",
			exceptionID: uuid.New(),
			category:    dispute.DisputeCategoryBankFeeError,
			description: "   ",
			openedBy:    "user@test.com",
			wantErr:     dispute.ErrDisputeDescriptionRequired,
		},
		{
			name:        "empty opened by",
			exceptionID: uuid.New(),
			category:    dispute.DisputeCategoryBankFeeError,
			description: "Valid description",
			openedBy:    "",
			wantErr:     dispute.ErrDisputeOpenedByRequired,
		},
		{
			name:        "whitespace-only opened by",
			exceptionID: uuid.New(),
			category:    dispute.DisputeCategoryBankFeeError,
			description: "Valid description",
			openedBy:    "   ",
			wantErr:     dispute.ErrDisputeOpenedByRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			testDispute, err := dispute.NewDispute(
				ctx,
				tt.exceptionID,
				tt.category,
				tt.description,
				tt.openedBy,
			)
			require.Error(t, err)
			require.ErrorIs(t, err, tt.wantErr)
			require.Nil(t, testDispute)
		})
	}
}

func TestDispute_Open(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("Draft to Open succeeds", func(t *testing.T) {
		t.Parallel()

		testDispute, err := dispute.NewDispute(
			ctx,
			uuid.New(),
			dispute.DisputeCategoryBankFeeError,
			"Description",
			"user@test.com",
		)
		require.NoError(t, err)
		require.Equal(t, dispute.DisputeStateDraft, testDispute.State)

		err = testDispute.Open(ctx)
		require.NoError(t, err)
		require.Equal(t, dispute.DisputeStateOpen, testDispute.State)
	})

	t.Run("nil dispute returns error", func(t *testing.T) {
		t.Parallel()

		var nilDispute *dispute.Dispute

		err := nilDispute.Open(ctx)
		require.ErrorIs(t, err, dispute.ErrDisputeNil)
	})

	t.Run("Open from non-Draft fails", func(t *testing.T) {
		t.Parallel()

		testDispute, err := dispute.NewDispute(
			ctx,
			uuid.New(),
			dispute.DisputeCategoryBankFeeError,
			"Description",
			"user@test.com",
		)
		require.NoError(t, err)
		require.NoError(t, testDispute.Open(ctx))

		err = testDispute.Open(ctx)
		require.ErrorIs(t, err, dispute.ErrInvalidDisputeTransition)
	})
}

func TestDispute_RequestEvidence(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("Open to PendingEvidence succeeds", func(t *testing.T) {
		t.Parallel()

		testDispute, err := dispute.NewDispute(
			ctx,
			uuid.New(),
			dispute.DisputeCategoryBankFeeError,
			"Description",
			"user@test.com",
		)
		require.NoError(t, err)
		require.NoError(t, testDispute.Open(ctx))

		err = testDispute.RequestEvidence(ctx)
		require.NoError(t, err)
		require.Equal(t, dispute.DisputeStatePendingEvidence, testDispute.State)
	})

	t.Run("RequestEvidence from Draft fails", func(t *testing.T) {
		t.Parallel()

		testDispute, err := dispute.NewDispute(
			ctx,
			uuid.New(),
			dispute.DisputeCategoryBankFeeError,
			"Description",
			"user@test.com",
		)
		require.NoError(t, err)

		err = testDispute.RequestEvidence(ctx)
		require.ErrorIs(t, err, dispute.ErrInvalidDisputeTransition)
	})

	t.Run("nil dispute returns error", func(t *testing.T) {
		t.Parallel()

		var nilDispute *dispute.Dispute

		err := nilDispute.RequestEvidence(ctx)
		require.ErrorIs(t, err, dispute.ErrDisputeNil)
	})
}

func TestDispute_AddEvidence(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("AddEvidence in Open state succeeds", func(t *testing.T) {
		t.Parallel()

		testDispute, err := dispute.NewDispute(
			ctx,
			uuid.New(),
			dispute.DisputeCategoryBankFeeError,
			"Description",
			"user@test.com",
		)
		require.NoError(t, err)
		require.NoError(t, testDispute.Open(ctx))
		require.Empty(t, testDispute.Evidence)

		fileURL := "https://example.com/doc.pdf"

		err = testDispute.AddEvidence(ctx, "Bank statement", "analyst@test.com", &fileURL)
		require.NoError(t, err)
		require.Len(t, testDispute.Evidence, 1)
		require.Equal(t, "Bank statement", testDispute.Evidence[0].Comment)
		require.Equal(t, dispute.DisputeStateOpen, testDispute.State)
	})

	t.Run("AddEvidence in PendingEvidence state transitions to Open", func(t *testing.T) {
		t.Parallel()

		testDispute, err := dispute.NewDispute(
			ctx,
			uuid.New(),
			dispute.DisputeCategoryBankFeeError,
			"Description",
			"user@test.com",
		)
		require.NoError(t, err)
		require.NoError(t, testDispute.Open(ctx))
		require.NoError(t, testDispute.RequestEvidence(ctx))
		require.Equal(t, dispute.DisputeStatePendingEvidence, testDispute.State)

		err = testDispute.AddEvidence(ctx, "Supporting document", "analyst@test.com", nil)
		require.NoError(t, err)
		require.Len(t, testDispute.Evidence, 1)
		require.Equal(t, dispute.DisputeStateOpen, testDispute.State)
	})

	t.Run("AddEvidence in Draft state fails", func(t *testing.T) {
		t.Parallel()

		testDispute, err := dispute.NewDispute(
			ctx,
			uuid.New(),
			dispute.DisputeCategoryBankFeeError,
			"Description",
			"user@test.com",
		)
		require.NoError(t, err)

		err = testDispute.AddEvidence(ctx, "Comment", "user@test.com", nil)
		require.ErrorIs(t, err, dispute.ErrCannotAddEvidenceInCurrentState)
	})

	t.Run("AddEvidence in Won state fails", func(t *testing.T) {
		t.Parallel()

		testDispute, err := dispute.NewDispute(
			ctx,
			uuid.New(),
			dispute.DisputeCategoryBankFeeError,
			"Description",
			"user@test.com",
		)
		require.NoError(t, err)
		require.NoError(t, testDispute.Open(ctx))
		require.NoError(t, testDispute.Win(ctx, "Customer refunded"))

		err = testDispute.AddEvidence(ctx, "Comment", "user@test.com", nil)
		require.ErrorIs(t, err, dispute.ErrCannotAddEvidenceInCurrentState)
	})

	t.Run("nil dispute returns error", func(t *testing.T) {
		t.Parallel()

		var nilDispute *dispute.Dispute

		err := nilDispute.AddEvidence(ctx, "Comment", "user@test.com", nil)
		require.ErrorIs(t, err, dispute.ErrDisputeNil)
	})

	t.Run("AddEvidence with invalid evidence returns validation error", func(t *testing.T) {
		t.Parallel()

		testDispute, err := dispute.NewDispute(
			ctx,
			uuid.New(),
			dispute.DisputeCategoryBankFeeError,
			"Description",
			"user@test.com",
		)
		require.NoError(t, err)
		require.NoError(t, testDispute.Open(ctx))

		err = testDispute.AddEvidence(ctx, "", "user@test.com", nil)
		require.ErrorIs(t, err, dispute.ErrEvidenceCommentRequired)
	})
}

func TestDispute_Win(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("Win from Open succeeds", func(t *testing.T) {
		t.Parallel()

		testDispute, err := dispute.NewDispute(
			ctx,
			uuid.New(),
			dispute.DisputeCategoryBankFeeError,
			"Description",
			"user@test.com",
		)
		require.NoError(t, err)
		require.NoError(t, testDispute.Open(ctx))

		err = testDispute.Win(ctx, "Customer refunded $50")
		require.NoError(t, err)
		require.Equal(t, dispute.DisputeStateWon, testDispute.State)
		require.NotNil(t, testDispute.Resolution)
		require.Equal(t, "Customer refunded $50", *testDispute.Resolution)
	})

	t.Run("Win from PendingEvidence succeeds", func(t *testing.T) {
		t.Parallel()

		testDispute, err := dispute.NewDispute(
			ctx,
			uuid.New(),
			dispute.DisputeCategoryBankFeeError,
			"Description",
			"user@test.com",
		)
		require.NoError(t, err)
		require.NoError(t, testDispute.Open(ctx))
		require.NoError(t, testDispute.RequestEvidence(ctx))

		err = testDispute.Win(ctx, "Resolved in customer favor")
		require.NoError(t, err)
		require.Equal(t, dispute.DisputeStateWon, testDispute.State)
	})

	t.Run("Win without resolution fails", func(t *testing.T) {
		t.Parallel()

		testDispute, err := dispute.NewDispute(
			ctx,
			uuid.New(),
			dispute.DisputeCategoryBankFeeError,
			"Description",
			"user@test.com",
		)
		require.NoError(t, err)
		require.NoError(t, testDispute.Open(ctx))

		err = testDispute.Win(ctx, "")
		require.ErrorIs(t, err, dispute.ErrDisputeResolutionRequired)
	})

	t.Run("Win from Draft fails", func(t *testing.T) {
		t.Parallel()

		testDispute, err := dispute.NewDispute(
			ctx,
			uuid.New(),
			dispute.DisputeCategoryBankFeeError,
			"Description",
			"user@test.com",
		)
		require.NoError(t, err)

		err = testDispute.Win(ctx, "Resolution")
		require.ErrorIs(t, err, dispute.ErrInvalidDisputeTransition)
	})

	t.Run("nil dispute returns error", func(t *testing.T) {
		t.Parallel()

		var nilDispute *dispute.Dispute

		err := nilDispute.Win(ctx, "Resolution")
		require.ErrorIs(t, err, dispute.ErrDisputeNil)
	})
}

func TestDispute_Lose(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("Lose from Open succeeds", func(t *testing.T) {
		t.Parallel()

		testDispute, err := dispute.NewDispute(
			ctx,
			uuid.New(),
			dispute.DisputeCategoryBankFeeError,
			"Description",
			"user@test.com",
		)
		require.NoError(t, err)
		require.NoError(t, testDispute.Open(ctx))

		err = testDispute.Lose(ctx, "Bank fee was valid")
		require.NoError(t, err)
		require.Equal(t, dispute.DisputeStateLost, testDispute.State)
		require.NotNil(t, testDispute.Resolution)
		require.Equal(t, "Bank fee was valid", *testDispute.Resolution)
	})

	t.Run("Lose from PendingEvidence succeeds", func(t *testing.T) {
		t.Parallel()

		testDispute, err := dispute.NewDispute(
			ctx,
			uuid.New(),
			dispute.DisputeCategoryBankFeeError,
			"Description",
			"user@test.com",
		)
		require.NoError(t, err)
		require.NoError(t, testDispute.Open(ctx))
		require.NoError(t, testDispute.RequestEvidence(ctx))

		err = testDispute.Lose(ctx, "Insufficient evidence")
		require.NoError(t, err)
		require.Equal(t, dispute.DisputeStateLost, testDispute.State)
	})

	t.Run("Lose without resolution fails", func(t *testing.T) {
		t.Parallel()

		testDispute, err := dispute.NewDispute(
			ctx,
			uuid.New(),
			dispute.DisputeCategoryBankFeeError,
			"Description",
			"user@test.com",
		)
		require.NoError(t, err)
		require.NoError(t, testDispute.Open(ctx))

		err = testDispute.Lose(ctx, "   ")
		require.ErrorIs(t, err, dispute.ErrDisputeResolutionRequired)
	})

	t.Run("nil dispute returns error", func(t *testing.T) {
		t.Parallel()

		var nilDispute *dispute.Dispute

		err := nilDispute.Lose(ctx, "Resolution")
		require.ErrorIs(t, err, dispute.ErrDisputeNil)
	})
}

func TestDispute_Reopen(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("Reopen from Lost succeeds and records reason", func(t *testing.T) {
		t.Parallel()

		testDispute, err := dispute.NewDispute(
			ctx,
			uuid.New(),
			dispute.DisputeCategoryBankFeeError,
			"Description",
			"user@test.com",
		)
		require.NoError(t, err)
		require.NoError(t, testDispute.Open(ctx))
		require.NoError(t, testDispute.Lose(ctx, "Initially rejected"))
		require.NotNil(t, testDispute.Resolution)

		updatedBefore := testDispute.UpdatedAt

		err = testDispute.Reopen(ctx, "New evidence found")
		require.NoError(t, err)
		require.Equal(t, dispute.DisputeStateOpen, testDispute.State)
		require.Nil(t, testDispute.Resolution)
		require.NotNil(t, testDispute.ReopenReason)
		require.Equal(t, "New evidence found", *testDispute.ReopenReason)
		require.False(t, testDispute.UpdatedAt.Before(updatedBefore),
			"UpdatedAt after Reopen should not be before previous update")
	})

	t.Run("Reopen trims whitespace from reason", func(t *testing.T) {
		t.Parallel()

		testDispute, err := dispute.NewDispute(
			ctx,
			uuid.New(),
			dispute.DisputeCategoryBankFeeError,
			"Description",
			"user@test.com",
		)
		require.NoError(t, err)
		require.NoError(t, testDispute.Open(ctx))
		require.NoError(t, testDispute.Lose(ctx, "Rejected"))

		err = testDispute.Reopen(ctx, "  New evidence found  ")
		require.NoError(t, err)
		require.NotNil(t, testDispute.ReopenReason)
		require.Equal(t, "New evidence found", *testDispute.ReopenReason)
	})

	t.Run("Reopen with empty reason fails", func(t *testing.T) {
		t.Parallel()

		testDispute, err := dispute.NewDispute(
			ctx,
			uuid.New(),
			dispute.DisputeCategoryBankFeeError,
			"Description",
			"user@test.com",
		)
		require.NoError(t, err)
		require.NoError(t, testDispute.Open(ctx))
		require.NoError(t, testDispute.Lose(ctx, "Rejected"))

		err = testDispute.Reopen(ctx, "")
		require.ErrorIs(t, err, dispute.ErrDisputeReopenReasonRequired)
	})

	t.Run("Reopen with whitespace-only reason fails", func(t *testing.T) {
		t.Parallel()

		testDispute, err := dispute.NewDispute(
			ctx,
			uuid.New(),
			dispute.DisputeCategoryBankFeeError,
			"Description",
			"user@test.com",
		)
		require.NoError(t, err)
		require.NoError(t, testDispute.Open(ctx))
		require.NoError(t, testDispute.Lose(ctx, "Rejected"))

		err = testDispute.Reopen(ctx, "   ")
		require.ErrorIs(t, err, dispute.ErrDisputeReopenReasonRequired)
	})

	t.Run("Reopen from Won fails", func(t *testing.T) {
		t.Parallel()

		testDispute, err := dispute.NewDispute(
			ctx,
			uuid.New(),
			dispute.DisputeCategoryBankFeeError,
			"Description",
			"user@test.com",
		)
		require.NoError(t, err)
		require.NoError(t, testDispute.Open(ctx))
		require.NoError(t, testDispute.Win(ctx, "Customer refunded"))

		err = testDispute.Reopen(ctx, "Reason")
		require.ErrorIs(t, err, dispute.ErrInvalidDisputeTransition)
	})

	t.Run("Reopen from Open fails", func(t *testing.T) {
		t.Parallel()

		testDispute, err := dispute.NewDispute(
			ctx,
			uuid.New(),
			dispute.DisputeCategoryBankFeeError,
			"Description",
			"user@test.com",
		)
		require.NoError(t, err)
		require.NoError(t, testDispute.Open(ctx))

		err = testDispute.Reopen(ctx, "Reason")
		require.ErrorIs(t, err, dispute.ErrInvalidDisputeTransition)
	})

	t.Run("nil dispute returns error", func(t *testing.T) {
		t.Parallel()

		var nilDispute *dispute.Dispute

		err := nilDispute.Reopen(ctx, "Reason")
		require.ErrorIs(t, err, dispute.ErrDisputeNil)
	})
}

func TestDispute_StateTransitions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		name        string
		setupStates []string
		transition  string
		wantState   dispute.DisputeState
		wantErr     error
	}{
		{
			name:        "DRAFT to OPEN via Open",
			setupStates: []string{},
			transition:  "open",
			wantState:   dispute.DisputeStateOpen,
			wantErr:     nil,
		},
		{
			name:        "OPEN to PENDING_EVIDENCE via RequestEvidence",
			setupStates: []string{"open"},
			transition:  "request_evidence",
			wantState:   dispute.DisputeStatePendingEvidence,
			wantErr:     nil,
		},
		{
			name:        "OPEN to WON via Win",
			setupStates: []string{"open"},
			transition:  "win",
			wantState:   dispute.DisputeStateWon,
			wantErr:     nil,
		},
		{
			name:        "OPEN to LOST via Lose",
			setupStates: []string{"open"},
			transition:  "lose",
			wantState:   dispute.DisputeStateLost,
			wantErr:     nil,
		},
		{
			name:        "PENDING_EVIDENCE to WON via Win",
			setupStates: []string{"open", "request_evidence"},
			transition:  "win",
			wantState:   dispute.DisputeStateWon,
			wantErr:     nil,
		},
		{
			name:        "PENDING_EVIDENCE to LOST via Lose",
			setupStates: []string{"open", "request_evidence"},
			transition:  "lose",
			wantState:   dispute.DisputeStateLost,
			wantErr:     nil,
		},
		{
			name:        "LOST to OPEN via Reopen",
			setupStates: []string{"open", "lose"},
			transition:  "reopen",
			wantState:   dispute.DisputeStateOpen,
			wantErr:     nil,
		},
		{
			name:        "WON cannot transition to OPEN via Reopen",
			setupStates: []string{"open", "win"},
			transition:  "reopen",
			wantState:   dispute.DisputeStateWon,
			wantErr:     dispute.ErrInvalidDisputeTransition,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			testDispute, err := dispute.NewDispute(
				ctx,
				uuid.New(),
				dispute.DisputeCategoryBankFeeError,
				"Description",
				"user@test.com",
			)
			require.NoError(t, err)

			for _, state := range testCase.setupStates {
				switch state {
				case "open":
					require.NoError(t, testDispute.Open(ctx))
				case "request_evidence":
					require.NoError(t, testDispute.RequestEvidence(ctx))
				case "win":
					require.NoError(t, testDispute.Win(ctx, "Resolution"))
				case "lose":
					require.NoError(t, testDispute.Lose(ctx, "Resolution"))
				}
			}

			var transitionErr error

			switch testCase.transition {
			case "open":
				transitionErr = testDispute.Open(ctx)
			case "request_evidence":
				transitionErr = testDispute.RequestEvidence(ctx)
			case "win":
				transitionErr = testDispute.Win(ctx, "Resolution")
			case "lose":
				transitionErr = testDispute.Lose(ctx, "Resolution")
			case "reopen":
				transitionErr = testDispute.Reopen(ctx, "Reason")
			}

			if testCase.wantErr != nil {
				require.ErrorIs(t, transitionErr, testCase.wantErr)
			} else {
				require.NoError(t, transitionErr)
				require.Equal(t, testCase.wantState, testDispute.State)
			}
		})
	}
}

func TestDispute_TimestampInvariants(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("CreatedAt never changes on transitions", func(t *testing.T) {
		t.Parallel()

		testDispute, err := dispute.NewDispute(
			ctx,
			uuid.New(),
			dispute.DisputeCategoryBankFeeError,
			"Description",
			"user@test.com",
		)
		require.NoError(t, err)

		originalCreatedAt := testDispute.CreatedAt

		require.NoError(t, testDispute.Open(ctx))
		require.Equal(t, originalCreatedAt, testDispute.CreatedAt)

		require.NoError(t, testDispute.Win(ctx, "Resolution"))
		require.Equal(t, originalCreatedAt, testDispute.CreatedAt)
	})

	t.Run("UpdatedAt changes on each transition", func(t *testing.T) {
		t.Parallel()

		testDispute, err := dispute.NewDispute(
			ctx,
			uuid.New(),
			dispute.DisputeCategoryBankFeeError,
			"Description",
			"user@test.com",
		)
		require.NoError(t, err)

		updatedAfterCreate := testDispute.UpdatedAt

		require.NoError(t, testDispute.Open(ctx))
		updatedAfterOpen := testDispute.UpdatedAt
		require.False(t, updatedAfterOpen.Before(updatedAfterCreate),
			"UpdatedAt after Open should not be before creation time")

		require.NoError(t, testDispute.RequestEvidence(ctx))
		updatedAfterRequest := testDispute.UpdatedAt
		require.False(t, updatedAfterRequest.Before(updatedAfterOpen),
			"UpdatedAt after RequestEvidence should not be before Open time")

		require.NoError(t, testDispute.Lose(ctx, "Resolution"))
		updatedAfterLose := testDispute.UpdatedAt
		require.False(t, updatedAfterLose.Before(updatedAfterRequest),
			"UpdatedAt after Lose should not be before RequestEvidence time")

		require.NoError(t, testDispute.Reopen(ctx, "Reason"))
		updatedAfterReopen := testDispute.UpdatedAt
		require.False(t, updatedAfterReopen.Before(updatedAfterLose),
			"UpdatedAt after Reopen should not be before Lose time")
	})
}

func TestDispute_EvidenceAccumulates(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	testDispute, err := dispute.NewDispute(
		ctx,
		uuid.New(),
		dispute.DisputeCategoryBankFeeError,
		"Description",
		"user@test.com",
	)
	require.NoError(t, err)
	require.NoError(t, testDispute.Open(ctx))

	require.NoError(t, testDispute.AddEvidence(ctx, "First evidence", "user1@test.com", nil))
	require.Len(t, testDispute.Evidence, 1)

	require.NoError(t, testDispute.AddEvidence(ctx, "Second evidence", "user2@test.com", nil))
	require.Len(t, testDispute.Evidence, 2)

	require.NoError(t, testDispute.RequestEvidence(ctx))
	require.NoError(t, testDispute.AddEvidence(ctx, "Third evidence", "user3@test.com", nil))
	require.Len(t, testDispute.Evidence, 3)

	require.Equal(t, "First evidence", testDispute.Evidence[0].Comment)
	require.Equal(t, "Second evidence", testDispute.Evidence[1].Comment)
	require.Equal(t, "Third evidence", testDispute.Evidence[2].Comment)
}
