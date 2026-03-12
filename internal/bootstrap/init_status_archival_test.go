//go:build unit

package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShouldRedactInfraDetails_ProductionReturnsTrue(t *testing.T) {
	t.Parallel()

	assert.True(t, shouldRedactInfraDetails("production"))
}

func TestShouldRedactInfraDetails_DevelopmentReturnsFalse(t *testing.T) {
	t.Parallel()

	assert.False(t, shouldRedactInfraDetails("development"))
}

func TestSafeInfraTarget_ProductionRedacts(t *testing.T) {
	t.Parallel()

	result := safeInfraTarget("production", "db.internal:5432")

	assert.Equal(t, "configured", result)
}

func TestSafeInfraTarget_DevelopmentShowsValue(t *testing.T) {
	t.Parallel()

	result := safeInfraTarget("development", "localhost:5432")

	assert.Equal(t, "localhost:5432", result)
}

func TestInfraStatus_HasAllWorkerFields(t *testing.T) {
	t.Parallel()

	status := InfraStatus{
		ExportWorkerEnabled:    true,
		CleanupWorkerEnabled:   true,
		ArchivalWorkerEnabled:  true,
		SchedulerWorkerEnabled: true,
		DiscoveryWorkerEnabled: true,
	}

	assert.True(t, status.ExportWorkerEnabled)
	assert.True(t, status.CleanupWorkerEnabled)
	assert.True(t, status.ArchivalWorkerEnabled)
	assert.True(t, status.SchedulerWorkerEnabled)
	assert.True(t, status.DiscoveryWorkerEnabled)
}

func TestInitArchivalComponents_DisabledArchival_ReturnsNil(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Archival.Enabled = false

	var cleanups []func()

	worker, err := initArchivalComponents(nil, cfg, nil, nil, &cleanups)

	assert.NoError(t, err)
	assert.Nil(t, worker)
}
