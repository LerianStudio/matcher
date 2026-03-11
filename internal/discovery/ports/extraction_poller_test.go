//go:build unit

package ports_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/LerianStudio/matcher/internal/discovery/ports"
)

// mockExtractionJobPoller is a test double for the ExtractionJobPoller interface.
type mockExtractionJobPoller struct {
	pollUntilCompleteFunc func(
		ctx context.Context,
		extractionID uuid.UUID,
		onComplete func(ctx context.Context, resultPath string) error,
		onFailed func(ctx context.Context, errMsg string),
	)
}

func (m *mockExtractionJobPoller) PollUntilComplete(
	ctx context.Context,
	extractionID uuid.UUID,
	onComplete func(ctx context.Context, resultPath string) error,
	onFailed func(ctx context.Context, errMsg string),
) {
	if m.pollUntilCompleteFunc != nil {
		m.pollUntilCompleteFunc(ctx, extractionID, onComplete, onFailed)
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

	var capturedExtractionID uuid.UUID

	var onCompleteCalled bool

	poller := &mockExtractionJobPoller{
		pollUntilCompleteFunc: func(
			_ context.Context,
			extractionID uuid.UUID,
			onComplete func(ctx context.Context, resultPath string) error,
			_ func(ctx context.Context, errMsg string),
		) {
			capturedExtractionID = extractionID
			if onComplete != nil {
				_ = onComplete(context.Background(), "/path/to/results")
				onCompleteCalled = true
			}
		},
	}

	extractionID := uuid.New()

	poller.PollUntilComplete(
		context.Background(),
		extractionID,
		func(_ context.Context, _ string) error { return nil },
		nil,
	)

	assert.Equal(t, extractionID, capturedExtractionID)
	assert.True(t, onCompleteCalled)
}
