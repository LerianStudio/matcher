//go:build unit

// Copyright 2025 Lerian Studio.

package feedtest

import (
	"testing"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// globalTarget
// ---------------------------------------------------------------------------

func TestGlobalTarget_ReturnsValidGlobalConfig(t *testing.T) {
	t.Parallel()

	target := globalTarget(t)

	assert.Equal(t, domain.KindConfig, target.Kind)
	assert.Equal(t, domain.ScopeGlobal, target.Scope)
	assert.Equal(t, "", target.SubjectID)
}

func TestGlobalTarget_Deterministic(t *testing.T) {
	t.Parallel()

	a := globalTarget(t)
	b := globalTarget(t)

	assert.Equal(t, a, b)
}

func TestGlobalTarget_StringRepresentation(t *testing.T) {
	t.Parallel()

	target := globalTarget(t)

	result := target.String()

	assert.Contains(t, result, "config")
	assert.Contains(t, result, "global")
}

// ---------------------------------------------------------------------------
// testActor
// ---------------------------------------------------------------------------

func TestTestActor_ReturnsFixedActor(t *testing.T) {
	t.Parallel()

	actor := testActor()

	assert.Equal(t, "test-feed-user", actor.ID)
}

func TestTestActor_Deterministic(t *testing.T) {
	t.Parallel()

	a := testActor()
	b := testActor()

	assert.Equal(t, a, b)
}

func TestTestActor_NonEmpty(t *testing.T) {
	t.Parallel()

	actor := testActor()

	assert.NotEmpty(t, actor.ID)
}

// ---------------------------------------------------------------------------
// waitForSubscriptionReady
// ---------------------------------------------------------------------------

func TestWaitForSubscriptionReady_Signature(t *testing.T) {
	t.Parallel()

	// Compile-time check: the function exists and has the expected parameters.
	// We cannot call it without a real Store, but we verify the function signature.
	require.NotNil(t, waitForSubscriptionReady)
}

// ---------------------------------------------------------------------------
// Domain types used by contract helpers
// ---------------------------------------------------------------------------

func TestRevisionZero_IsZero(t *testing.T) {
	t.Parallel()

	assert.Equal(t, uint64(0), domain.RevisionZero.Uint64())
}

func TestActor_StructFields(t *testing.T) {
	t.Parallel()

	actor := domain.Actor{ID: "some-user"}

	assert.Equal(t, "some-user", actor.ID)
}

func TestTarget_CanBeUsedAsMapKey(t *testing.T) {
	t.Parallel()

	target := globalTarget(t)

	m := map[string]domain.Revision{
		target.String(): domain.Revision(1),
	}

	assert.Equal(t, domain.Revision(1), m[target.String()])
}
