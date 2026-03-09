//go:build unit

package ports_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/ports"
)

// mockExtractionJobPoller is a test double for the ExtractionJobPoller interface.
type mockExtractionJobPoller struct {
	pollUntilCompleteFunc func(
		ctx context.Context,
		extraction *entities.ExtractionRequest,
		onComplete func(ctx context.Context, resultPath string) error,
		onFailed func(ctx context.Context, errMsg string),
	)
}

func (m *mockExtractionJobPoller) PollUntilComplete(
	ctx context.Context,
	extraction *entities.ExtractionRequest,
	onComplete func(ctx context.Context, resultPath string) error,
	onFailed func(ctx context.Context, errMsg string),
) {
	if m.pollUntilCompleteFunc != nil {
		m.pollUntilCompleteFunc(ctx, extraction, onComplete, onFailed)
	}
}

// Compile-time interface compliance check.
var _ ports.ExtractionJobPoller = (*mockExtractionJobPoller)(nil)

func TestExtractionJobPoller_InterfaceCompliance(t *testing.T) {
	t.Parallel()

	// Verify that the mock implements the interface correctly.
	var poller ports.ExtractionJobPoller = &mockExtractionJobPoller{}
	assert.NotNil(t, poller)
}

func TestExtractionJobPoller_PollUntilComplete(t *testing.T) {
	t.Parallel()

	var capturedExtraction *entities.ExtractionRequest

	var onCompleteCalled bool

	poller := &mockExtractionJobPoller{
		pollUntilCompleteFunc: func(
			_ context.Context,
			extraction *entities.ExtractionRequest,
			onComplete func(ctx context.Context, resultPath string) error,
			_ func(ctx context.Context, errMsg string),
		) {
			capturedExtraction = extraction
			if onComplete != nil {
				_ = onComplete(context.Background(), "/path/to/results")
				onCompleteCalled = true
			}
		},
	}

	extraction := &entities.ExtractionRequest{
		ID:             uuid.New(),
		IngestionJobID: uuid.New(),
		FetcherConnID:  "test-conn",
		FetcherJobID:   "job-123",
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}

	poller.PollUntilComplete(
		context.Background(),
		extraction,
		func(_ context.Context, _ string) error { return nil },
		nil,
	)

	assert.Equal(t, extraction, capturedExtraction)
	assert.True(t, onCompleteCalled)
}
