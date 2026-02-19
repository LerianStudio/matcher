//go:build unit

package dispute_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/exception/domain/dispute"
)

func TestNewEvidence_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	disputeID := uuid.New()
	fileURL := "https://example.com/evidence.pdf"

	evidence, err := dispute.NewEvidence(
		ctx,
		disputeID,
		"Bank statement showing error",
		"analyst@example.com",
		&fileURL,
	)
	require.NoError(t, err)
	require.NotNil(t, evidence)
	require.NotEqual(t, uuid.Nil, evidence.ID)
	require.Equal(t, disputeID, evidence.DisputeID)
	require.Equal(t, "Bank statement showing error", evidence.Comment)
	require.Equal(t, "analyst@example.com", evidence.SubmittedBy)
	require.NotNil(t, evidence.FileURL)
	require.Equal(t, "https://example.com/evidence.pdf", *evidence.FileURL)
	require.False(t, evidence.SubmittedAt.IsZero())
}

func TestNewEvidence_WithoutFileURL(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	disputeID := uuid.New()

	evidence, err := dispute.NewEvidence(
		ctx,
		disputeID,
		"Customer statement",
		"operator@example.com",
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, evidence)
	require.Nil(t, evidence.FileURL)
	require.Equal(t, "Customer statement", evidence.Comment)
}

func TestNewEvidence_TrimsWhitespace(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	disputeID := uuid.New()
	fileURL := "  https://example.com/doc.pdf  "

	evidence, err := dispute.NewEvidence(
		ctx,
		disputeID,
		"  Comment with whitespace  ",
		"  user@test.com  ",
		&fileURL,
	)
	require.NoError(t, err)
	require.NotNil(t, evidence)
	require.Equal(t, "Comment with whitespace", evidence.Comment)
	require.Equal(t, "user@test.com", evidence.SubmittedBy)
	require.NotNil(t, evidence.FileURL)
	require.Equal(t, "https://example.com/doc.pdf", *evidence.FileURL)
}

func TestNewEvidence_EmptyFileURLBecomesNil(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	disputeID := uuid.New()
	emptyURL := "   "

	evidence, err := dispute.NewEvidence(ctx, disputeID, "Comment", "user@test.com", &emptyURL)
	require.NoError(t, err)
	require.NotNil(t, evidence)
	require.Nil(t, evidence.FileURL)
}

func TestNewEvidence_Validation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		name        string
		disputeID   uuid.UUID
		comment     string
		submittedBy string
		fileURL     *string
		wantErr     error
	}{
		{
			name:        "nil dispute ID",
			disputeID:   uuid.Nil,
			comment:     "Valid comment",
			submittedBy: "user@test.com",
			fileURL:     nil,
			wantErr:     dispute.ErrEvidenceDisputeIDRequired,
		},
		{
			name:        "empty comment",
			disputeID:   uuid.New(),
			comment:     "",
			submittedBy: "user@test.com",
			fileURL:     nil,
			wantErr:     dispute.ErrEvidenceCommentRequired,
		},
		{
			name:        "whitespace-only comment",
			disputeID:   uuid.New(),
			comment:     "   ",
			submittedBy: "user@test.com",
			fileURL:     nil,
			wantErr:     dispute.ErrEvidenceCommentRequired,
		},
		{
			name:        "empty submitter",
			disputeID:   uuid.New(),
			comment:     "Valid comment",
			submittedBy: "",
			fileURL:     nil,
			wantErr:     dispute.ErrEvidenceSubmitterRequired,
		},
		{
			name:        "whitespace-only submitter",
			disputeID:   uuid.New(),
			comment:     "Valid comment",
			submittedBy: "   ",
			fileURL:     nil,
			wantErr:     dispute.ErrEvidenceSubmitterRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			evidence, err := dispute.NewEvidence(
				ctx,
				tt.disputeID,
				tt.comment,
				tt.submittedBy,
				tt.fileURL,
			)
			require.Error(t, err)
			require.ErrorIs(t, err, tt.wantErr)
			require.Nil(t, evidence)
		})
	}
}

func TestNewEvidence_GeneratesUniqueIDs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	disputeID := uuid.New()

	evidence1, err := dispute.NewEvidence(ctx, disputeID, "Comment 1", "user1@test.com", nil)
	require.NoError(t, err)

	evidence2, err := dispute.NewEvidence(ctx, disputeID, "Comment 2", "user2@test.com", nil)
	require.NoError(t, err)

	require.NotEqual(t, evidence1.ID, evidence2.ID)
}

func TestNewEvidence_URLValidation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	disputeID := uuid.New()

	tests := []struct {
		name    string
		fileURL string
		wantErr error
	}{
		{
			name:    "valid HTTPS URL",
			fileURL: "https://storage.example.com/evidence/doc.pdf",
			wantErr: nil,
		},
		{
			name:    "HTTPS with path and query",
			fileURL: "https://cdn.example.com/files/evidence.pdf?token=abc123",
			wantErr: nil,
		},
		{
			name:    "HTTP URL rejected",
			fileURL: "http://example.com/evidence.pdf",
			wantErr: dispute.ErrEvidenceInvalidFileURL,
		},
		{
			name:    "FTP URL rejected",
			fileURL: "ftp://files.example.com/evidence.pdf",
			wantErr: dispute.ErrEvidenceInvalidFileURL,
		},
		{
			name:    "file:// URL rejected",
			fileURL: "file:///etc/passwd",
			wantErr: dispute.ErrEvidenceInvalidFileURL,
		},
		{
			name:    "no scheme rejected",
			fileURL: "example.com/evidence.pdf",
			wantErr: dispute.ErrEvidenceInvalidFileURL,
		},
		{
			name:    "HTTPS without host rejected",
			fileURL: "https:///path/only",
			wantErr: dispute.ErrEvidenceInvalidFileURL,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			evidence, err := dispute.NewEvidence(
				ctx,
				disputeID,
				"Valid comment",
				"user@test.com",
				&tt.fileURL,
			)

			if tt.wantErr != nil {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErr)
				require.Nil(t, evidence)
			} else {
				require.NoError(t, err)
				require.NotNil(t, evidence)
				require.NotNil(t, evidence.FileURL)
				require.Equal(t, tt.fileURL, *evidence.FileURL)
			}
		})
	}
}
