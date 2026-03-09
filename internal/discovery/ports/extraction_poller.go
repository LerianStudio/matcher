package ports

import (
	"context"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
)

// ExtractionJobPoller polls for extraction job completion asynchronously.
type ExtractionJobPoller interface {
	PollUntilComplete(
		ctx context.Context,
		extraction *entities.ExtractionRequest,
		onComplete func(ctx context.Context, resultPath string) error,
		onFailed func(ctx context.Context, errMsg string),
	)
}
