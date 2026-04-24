//go:build e2e

package journeys

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/client"
	"github.com/LerianStudio/matcher/tests/e2e/factories"
)

// TestErrorHandling_NotFoundResponses verifies 404 responses for non-existent resources.
func TestErrorHandling_NotFoundResponses(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()
		nonExistentID := uuid.New().String()

		// Create a shared context for subtests that need one
		f := factories.New(tc, apiClient)
		sharedContext := f.Context.NewContext().MustCreate(ctx)

		t.Run("get non-existent context returns 404", func(t *testing.T) {
			_, err := apiClient.Configuration.GetContext(ctx, nonExistentID)
			require.Error(t, err)

			var apiErr *client.APIError
			require.True(t, errors.As(err, &apiErr))
			require.True(t, apiErr.IsNotFound(), "expected 404, got %d", apiErr.StatusCode)
		})

		t.Run("get non-existent source returns 404", func(t *testing.T) {
			_, err := apiClient.Configuration.GetSource(ctx, sharedContext.ID, nonExistentID)
			require.Error(t, err)

			var apiErr *client.APIError
			require.True(t, errors.As(err, &apiErr))
			require.True(t, apiErr.IsNotFound(), "expected 404, got %d", apiErr.StatusCode)
		})

		t.Run("get non-existent match run returns 404", func(t *testing.T) {
			// Use the shared context - context must exist for the run lookup,
			// otherwise 403 is returned for auth
			_, err := apiClient.Matching.GetMatchRun(ctx, sharedContext.ID, nonExistentID)
			require.Error(t, err)

			var apiErr *client.APIError
			require.True(t, errors.As(err, &apiErr))
			require.True(t, apiErr.IsNotFound(), "expected 404, got %d", apiErr.StatusCode)
		})
	})
}

// TestErrorHandling_BadRequestResponses verifies 400 responses for invalid inputs.
func TestErrorHandling_BadRequestResponses(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()
		f := factories.New(tc, apiClient)

		t.Run("invalid UUID in path returns 400", func(t *testing.T) {
			_, err := apiClient.Configuration.GetContext(ctx, "not-a-uuid")
			require.Error(t, err)

			var apiErr *client.APIError
			require.True(t, errors.As(err, &apiErr))
			require.True(t, apiErr.IsBadRequest(), "expected 400, got %d", apiErr.StatusCode)
		})

		t.Run("invalid context type returns 400", func(t *testing.T) {
			_, err := apiClient.Configuration.CreateContext(ctx, client.CreateContextRequest{
				Name:     tc.UniqueName("bad-type"),
				Type:     "invalid_type",
				Interval: "0 0 * * *",
			})
			require.Error(t, err)

			var apiErr *client.APIError
			require.True(t, errors.As(err, &apiErr))
			require.True(t, apiErr.IsBadRequest(), "expected 400, got %d", apiErr.StatusCode)
		})

		t.Run("invalid match mode returns 400", func(t *testing.T) {
			reconciliationContext := f.Context.NewContext().MustCreate(ctx)

			_, err := apiClient.Matching.RunMatch(ctx, reconciliationContext.ID, "INVALID_MODE")
			require.Error(t, err)

			var apiErr *client.APIError
			require.True(t, errors.As(err, &apiErr))
			require.True(t, apiErr.IsBadRequest(), "expected 400, got %d", apiErr.StatusCode)
		})
	})
}

// TestErrorHandling_EmptyFileUpload verifies rejection of empty files.
func TestErrorHandling_EmptyFileUpload(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()
		f := factories.New(tc, apiClient)

		reconciliationContext := f.Context.NewContext().MustCreate(ctx)
		source := f.Source.NewSource(reconciliationContext.ID).MustCreate(ctx)
		f.Source.NewFieldMap(reconciliationContext.ID, source.ID).MustCreate(ctx)

		_, err := apiClient.Ingestion.UploadCSV(
			ctx,
			reconciliationContext.ID,
			source.ID,
			"empty.csv",
			[]byte(""),
		)
		require.Error(t, err)

		var apiErr *client.APIError
		require.True(t, errors.As(err, &apiErr))
		require.True(t, apiErr.IsBadRequest(), "expected 400, got %d", apiErr.StatusCode)
	})
}
