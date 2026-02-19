//go:build e2e

package factories

import (
	"testing"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew_CreatesAllFactories(t *testing.T) {
	t.Parallel()

	cfg := &e2e.E2EConfig{DefaultTenantID: "test-tenant"}
	tc := e2e.NewTestContext(t, cfg)
	factories := New(tc, nil)

	require.NotNil(t, factories)
	assert.NotNil(t, factories.Context)
	assert.NotNil(t, factories.Source)
	assert.NotNil(t, factories.Rule)
}

func TestFactories_StructureIsValid(t *testing.T) {
	factories := &Factories{}

	assert.Nil(t, factories.Context)
	assert.Nil(t, factories.Source)
	assert.Nil(t, factories.Rule)
}
