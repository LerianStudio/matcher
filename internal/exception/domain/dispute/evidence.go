package dispute

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-uncommons/v2/uncommons/assert"

	"github.com/LerianStudio/matcher/internal/shared/constants"
	"github.com/LerianStudio/matcher/internal/shared/utils"
)

// Sentinel errors for Evidence operations.
var (
	ErrEvidenceCommentRequired   = errors.New("evidence comment is required")
	ErrEvidenceSubmitterRequired = errors.New("evidence submitter is required")
	ErrEvidenceDisputeIDRequired = errors.New("evidence dispute id is required")
	ErrEvidenceInvalidFileURL    = errors.New("evidence file URL is invalid")
)

// Evidence represents supporting evidence for a dispute.
type Evidence struct {
	ID          uuid.UUID
	DisputeID   uuid.UUID
	FileURL     *string
	Comment     string
	SubmittedBy string
	SubmittedAt time.Time
}

// NewEvidence creates a new Evidence for a dispute.
func NewEvidence(
	ctx context.Context,
	disputeID uuid.UUID,
	comment, submittedBy string,
	fileURL *string,
) (*Evidence, error) {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "dispute.evidence.new")

	if err := asserter.That(ctx, disputeID != uuid.Nil, "dispute id is required"); err != nil {
		return nil, ErrEvidenceDisputeIDRequired
	}

	trimmedComment := strings.TrimSpace(comment)
	if err := asserter.NotEmpty(ctx, trimmedComment, "comment is required"); err != nil {
		return nil, ErrEvidenceCommentRequired
	}

	trimmedSubmitter := strings.TrimSpace(submittedBy)
	if err := asserter.NotEmpty(ctx, trimmedSubmitter, "submitter is required"); err != nil {
		return nil, ErrEvidenceSubmitterRequired
	}

	validatedURL, err := validateFileURL(fileURL)
	if err != nil {
		return nil, err
	}

	return &Evidence{
		ID:          uuid.New(),
		DisputeID:   disputeID,
		FileURL:     validatedURL,
		Comment:     trimmedComment,
		SubmittedBy: trimmedSubmitter,
		SubmittedAt: time.Now().UTC(),
	}, nil
}

func validateFileURL(fileURL *string) (*string, error) {
	normalized := utils.NormalizeOptionalText(fileURL)
	if normalized == nil {
		return nil, nil
	}

	parsed, err := url.Parse(*normalized)
	if err != nil {
		return nil, ErrEvidenceInvalidFileURL
	}

	if parsed.Scheme != "https" {
		return nil, ErrEvidenceInvalidFileURL
	}

	if parsed.Host == "" {
		return nil, ErrEvidenceInvalidFileURL
	}

	return normalized, nil
}
