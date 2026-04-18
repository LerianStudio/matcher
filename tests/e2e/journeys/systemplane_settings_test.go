//go:build e2e

package journeys

import (
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/tests/e2e"
)

const enableSettingsRuntimeJourneyEnv = "E2E_ENABLE_SETTINGS_RUNTIME_JOURNEY"

func TestSystemplaneSettings_TenantRateLimitAffectsProtectedRoute(t *testing.T) {
	if os.Getenv(enableSettingsRuntimeJourneyEnv) == "" {
		t.Skip("settings runtime journey disabled; set E2E_ENABLE_SETTINGS_RUNTIME_JOURNEY=1 to enable")
	}

	cfg := e2e.GetConfig()
	require.NotNil(t, cfg)

	// In v5, systemplane has no per-tenant settings scope. Rate-limit config
	// keys live in the flat "matcher" namespace alongside all other keys.
	// This test sets rate_limit.max=1, verifies the second request is throttled,
	// then restores the original value.

	// Snapshot current values for restore.
	origMax, maxFound, err := readSystemplaneKeyValue(cfg.AppBaseURL, "rate_limit.max")
	require.NoError(t, err)

	origExpiry, expiryFound, err := readSystemplaneKeyValue(cfg.AppBaseURL, "rate_limit.expiry_sec")
	require.NoError(t, err)

	t.Cleanup(func() {
		if maxFound {
			_ = putSystemplaneValues(cfg.AppBaseURL, map[string]any{"rate_limit.max": origMax})
		}

		if expiryFound {
			_ = putSystemplaneValues(cfg.AppBaseURL, map[string]any{"rate_limit.expiry_sec": origExpiry})
		}
	})

	require.NoError(t, putSystemplaneValues(cfg.AppBaseURL, map[string]any{
		"rate_limit.max":        1,
		"rate_limit.expiry_sec": 60,
	}))

	// Verify the key was set.
	val, found, err := readSystemplaneKeyValue(cfg.AppBaseURL, "rate_limit.max")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, float64(1), val)

	// Hit a protected route twice — second should be throttled.
	headers := map[string]string{
		"Accept": "application/json",
	}

	listURL := cfg.AppBaseURL + "/system/" + systemplaneNamespace

	firstResp, firstBody, err := doSystemplaneRequest(http.MethodGet, listURL, nil, headers)
	require.NoError(t, err)
	defer firstResp.Body.Close() //nolint:errcheck // test helper
	assert.Equal(t, http.StatusOK, firstResp.StatusCode, string(firstBody))

	secondResp, secondBody, err := doSystemplaneRequest(http.MethodGet, listURL, nil, headers)
	require.NoError(t, err)
	defer secondResp.Body.Close() //nolint:errcheck // test helper
	assert.Equal(t, http.StatusTooManyRequests, secondResp.StatusCode, string(secondBody))
}
