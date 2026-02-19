//go:build e2e

package e2e

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateAllDeterministicScenarios_ReturnsNonEmpty(t *testing.T) {
	t.Parallel()

	scenarios := GenerateAllDeterministicScenarios()
	assert.NotEmpty(t, scenarios, "must generate at least one scenario")
}

func TestGenerateAllDeterministicScenarios_AllFieldsPopulated(t *testing.T) {
	t.Parallel()

	scenarios := GenerateAllDeterministicScenarios()

	for _, scenario := range scenarios {
		assert.NotEmpty(t, scenario.ID, "scenario ID must not be empty")
		assert.NotEmpty(t, scenario.Source, "scenario Source must not be empty")
		assert.NotEmpty(t, scenario.Category, "scenario Category must not be empty")
		assert.NotEmpty(t, scenario.AttackVector, "scenario AttackVector must not be empty")
		assert.NotEmpty(t, scenario.GrossAmount, "scenario GrossAmount must not be empty")
	}
}

func TestGenerateAllDeterministicScenarios_ValidScheduleSpecs(t *testing.T) {
	t.Parallel()

	scenarios := GenerateAllDeterministicScenarios()

	validOrders := map[string]bool{"PARALLEL": true, "CASCADING": true}

	for _, scenario := range scenarios {
		spec := scenario.Schedule

		require.NotEmpty(t, spec.Items, "scenario %s must have at least one item", scenario.ID)

		order := strings.ToUpper(spec.ApplicationOrder)
		assert.True(t, validOrders[order],
			"scenario %s has invalid applicationOrder %q", scenario.ID, spec.ApplicationOrder)

		for _, item := range spec.Items {
			assert.NotEmpty(t, item.Name, "item in scenario %s must have a name", scenario.ID)
			assert.NotEmpty(t, item.StructureType, "item in scenario %s must have a structureType", scenario.ID)
			assert.NotNil(t, item.Structure, "item in scenario %s must have a structure", scenario.ID)
			assert.Positive(t, item.Priority, "item %q in scenario %s must have positive priority", item.Name, scenario.ID)
		}
	}
}

func TestGenerateAllDeterministicScenarios_NoDuplicateIDs(t *testing.T) {
	t.Parallel()

	scenarios := GenerateAllDeterministicScenarios()

	seen := make(map[string]struct{}, len(scenarios))
	for _, scenario := range scenarios {
		_, exists := seen[scenario.ID]
		assert.False(t, exists, "duplicate scenario ID: %s", scenario.ID)

		seen[scenario.ID] = struct{}{}
	}
}

func TestGenerateRoundingTortureScenarios_ReturnsNonEmpty(t *testing.T) {
	t.Parallel()

	scenarios := GenerateRoundingTortureScenarios()
	assert.NotEmpty(t, scenarios)

	for _, scenario := range scenarios {
		assert.Equal(t, "rounding", scenario.Category)
		assert.True(t, strings.HasPrefix(scenario.ID, "rounding_"),
			"rounding scenario ID %q should start with 'rounding_'", scenario.ID)
	}
}

func TestGenerateTieredBoundaryScenarios_ReturnsNonEmpty(t *testing.T) {
	t.Parallel()

	scenarios := GenerateTieredBoundaryScenarios()
	assert.NotEmpty(t, scenarios)

	for _, scenario := range scenarios {
		assert.Equal(t, "tiered_boundary", scenario.Category)
		assert.True(t, strings.HasPrefix(scenario.ID, "tiered_"),
			"tiered scenario ID %q should start with 'tiered_'", scenario.ID)
	}
}

func TestGenerateCascadingStressScenarios_ReturnsNonEmpty(t *testing.T) {
	t.Parallel()

	scenarios := GenerateCascadingStressScenarios()
	assert.NotEmpty(t, scenarios)

	for _, scenario := range scenarios {
		assert.Equal(t, "cascading_stress", scenario.Category)
		assert.True(t, strings.HasPrefix(scenario.ID, "cascading_"),
			"cascading scenario ID %q should start with 'cascading_'", scenario.ID)
	}
}

func TestGenerateConvergenceScenarios_ReturnsNonEmpty(t *testing.T) {
	t.Parallel()

	scenarios := GenerateConvergenceScenarios()
	assert.NotEmpty(t, scenarios)

	for _, scenario := range scenarios {
		assert.Equal(t, "convergence", scenario.Category)
		assert.True(t, strings.HasPrefix(scenario.ID, "convergence_"),
			"convergence scenario ID %q should start with 'convergence_'", scenario.ID)
	}
}

func TestGenerateAllDeterministicScenarios_ContainsAllCategories(t *testing.T) {
	t.Parallel()

	scenarios := GenerateAllDeterministicScenarios()

	categories := make(map[string]int)
	for _, scenario := range scenarios {
		categories[scenario.Category]++
	}

	expectedCategories := []string{"rounding", "tiered_boundary", "cascading_stress", "convergence"}
	for _, cat := range expectedCategories {
		assert.Positive(t, categories[cat], "expected at least one scenario in category %q", cat)
	}
}

func TestGenerateAllDeterministicScenarios_AllSourceDeterministic(t *testing.T) {
	t.Parallel()

	scenarios := GenerateAllDeterministicScenarios()

	for _, scenario := range scenarios {
		assert.Equal(t, "deterministic", scenario.Source,
			"scenario %s should have source 'deterministic', got %q", scenario.ID, scenario.Source)
	}
}

func TestGenerateAllDeterministicScenarios_GoOracleCanProcessAll(t *testing.T) {
	t.Parallel()

	scenarios := GenerateAllDeterministicScenarios()

	for _, scenario := range scenarios {
		result, err := GoOracleCalculate(scenario.Schedule, scenario.GrossAmount)
		require.NoError(t, err, "GoOracleCalculate failed for scenario %s: %v", scenario.ID, err)
		assert.NotEmpty(t, result.TotalFee, "scenario %s should have non-empty totalFee", scenario.ID)
		assert.NotEmpty(t, result.NetAmount, "scenario %s should have non-empty netAmount", scenario.ID)
	}
}
