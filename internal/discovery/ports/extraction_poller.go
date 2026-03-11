package ports

import (
	"context"

	"github.com/google/uuid"
)

// ExtractionJobPoller polls for extraction job completion asynchronously.
type ExtractionJobPoller interface {
	PollUntilComplete(
		ctx context.Context,
		extractionID uuid.UUID,
		onComplete func(ctx context.Context, resultPath string) error,
		onFailed func(ctx context.Context, errMsg string),
	)
}
