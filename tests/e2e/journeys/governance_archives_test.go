//go:build e2e

package journeys

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/client"
)

// TestGovernanceArchives_ListArchives tests listing governance archives.
func TestGovernanceArchives_ListArchives(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()

			archives, err := apiClient.Governance.ListArchives(ctx)
			if err != nil {
				var apiErr *client.APIError
				if errors.As(err, &apiErr) && apiErr.IsNotFound() {
					tc.Logf("✓ No governance archives exist (404)")
					return
				}
				require.NoError(t, err, "unexpected error while listing governance archives")
			}
			tc.Logf("✓ Listed %d governance archives", len(archives))

			for _, arch := range archives {
				require.NotEmpty(t, arch.ID)
			}
		},
	)
}

// TestGovernanceArchives_DownloadNonExistent tests downloading a non-existent archive.
func TestGovernanceArchives_DownloadNonExistent(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()

		_, err := apiClient.Governance.DownloadArchive(
			ctx,
			"00000000-0000-0000-0000-000000000000",
		)
		require.Error(t, err, "downloading non-existent archive should fail")
		tc.Logf("✓ Non-existent archive download correctly rejected")
	})
}
