//go:build chaos

package chaos

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSeedData_Fields(t *testing.T) {
	t.Parallel()

	seed := SeedData{}
	assert.True(t, seed.TenantID.String() == "00000000-0000-0000-0000-000000000000")
	assert.True(t, seed.ContextID.String() == "00000000-0000-0000-0000-000000000000")
	assert.True(t, seed.SourceID.String() == "00000000-0000-0000-0000-000000000000")
}

func TestCleanupSharedChaos_NilHarness(t *testing.T) {
	t.Parallel()

	// Save and restore the package-level var.
	orig := sharedChaos
	sharedChaos = nil

	defer func() { sharedChaos = orig }()

	err := CleanupSharedChaos(context.Background())
	assert.NoError(t, err)
}

func TestGetSharedChaos_ReturnsPackageVar(t *testing.T) {
	t.Parallel()

	// GetSharedChaos returns whatever is stored at package level.
	result := GetSharedChaos()
	// We can't assert a specific value, but it must not panic.
	_ = result
}

func TestChaosHarness_Cleanup_NilContainers(t *testing.T) {
	t.Parallel()

	h := &ChaosHarness{}
	err := h.Cleanup(context.Background())
	assert.NoError(t, err)
}
