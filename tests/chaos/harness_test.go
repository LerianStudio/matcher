//go:build unit

package chaos

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSeedData_Fields(t *testing.T) {
	t.Parallel()

	seed := SeedData{}
	assert.Equal(t, uuid.Nil, seed.TenantID)
	assert.Equal(t, uuid.Nil, seed.ContextID)
	assert.Equal(t, uuid.Nil, seed.SourceID)
}

func TestCleanupSharedChaos_NilHarness(t *testing.T) {
	// Save and restore the package-level var.
	orig := sharedChaos
	sharedChaos = nil

	defer func() { sharedChaos = orig }()

	err := CleanupSharedChaos(context.Background())
	assert.NoError(t, err)
}

func TestGetSharedChaos_ReturnsPackageVar(t *testing.T) {
	orig := sharedChaos
	h := &ChaosHarness{}
	sharedChaos = h
	defer func() { sharedChaos = orig }()

	require.Same(t, h, GetSharedChaos())
}

func TestChaosHarness_Cleanup_NilContainers(t *testing.T) {
	t.Parallel()

	h := &ChaosHarness{}
	err := h.Cleanup(context.Background())
	assert.NoError(t, err)
}
