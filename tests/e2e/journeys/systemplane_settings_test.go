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

	const (
		scope       = "tenant"
		adminUser   = "settings-admin@example.com"
		probeUser   = "settings-probe@example.com"
		restoreUser = "settings-restore@example.com"
	)

	type restoreSetting struct {
		Value  any
		Source string
	}

	original, err := readSystemplaneSettings(cfg.AppBaseURL, scope, adminUser)
	require.NoError(t, err)

	restoreValues := map[string]restoreSetting{}
	for _, key := range []string{"rate_limit.max", "rate_limit.expiry_sec"} {
		if entry, ok := original.Values[key]; ok {
			restoreValues[key] = restoreSetting{Value: entry.Value, Source: entry.Source}
		}
	}

	t.Cleanup(func() {
		if len(restoreValues) == 0 {
			return
		}

		resetPatch := make(map[string]any, len(restoreValues))
		for key, restore := range restoreValues {
			if restore.Source == "tenant-override" {
				resetPatch[key] = restore.Value
				continue
			}

			resetPatch[key] = nil
		}

		require.NoError(t, patchSystemplaneSettingValues(cfg.AppBaseURL, scope, resetPatch, restoreUser))
	})

	require.NoError(t, patchSystemplaneSettingValues(cfg.AppBaseURL, scope, map[string]any{
		"rate_limit.max":        1,
		"rate_limit.expiry_sec": 60,
	}, adminUser))

	settings, err := readSystemplaneSettings(cfg.AppBaseURL, scope, probeUser)
	require.NoError(t, err)
	require.Contains(t, settings.Values, "rate_limit.max")
	assert.Equal(t, float64(1), settings.Values["rate_limit.max"].Value)
	assert.Equal(t, "tenant-override", settings.Values["rate_limit.max"].Source)

	headers := map[string]string{
		"Accept":    "application/json",
		"X-User-ID": probeUser,
	}

	firstResp, firstBody, err := doSystemplaneRequest(http.MethodGet, cfg.AppBaseURL+"/v1/system/settings", nil, headers)
	require.NoError(t, err)
	defer firstResp.Body.Close() //nolint:errcheck // test helper
	assert.Equal(t, http.StatusOK, firstResp.StatusCode, string(firstBody))

	secondResp, secondBody, err := doSystemplaneRequest(http.MethodGet, cfg.AppBaseURL+"/v1/system/settings", nil, headers)
	require.NoError(t, err)
	defer secondResp.Body.Close() //nolint:errcheck // test helper
	assert.Equal(t, http.StatusTooManyRequests, secondResp.StatusCode, string(secondBody))
}
