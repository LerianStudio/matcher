//go:build unit

package ports

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractionParams_ZeroValue(t *testing.T) {
	t.Parallel()

	var params ExtractionParams

	assert.Empty(t, params.StartDate)
	assert.Empty(t, params.EndDate)
	assert.Nil(t, params.Filters)
}

func TestExtractionParams_WithValues(t *testing.T) {
	t.Parallel()

	params := ExtractionParams{
		StartDate: "2024-01-01",
		EndDate:   "2024-12-31",
		Filters:   map[string]any{"key": "value"},
	}

	assert.Equal(t, "2024-01-01", params.StartDate)
	assert.Equal(t, "2024-12-31", params.EndDate)
	assert.Contains(t, params.Filters, "key")
	assert.Equal(t, "value", params.Filters["key"])
}

func TestExtractionParams_EmptyFilters(t *testing.T) {
	t.Parallel()

	params := ExtractionParams{
		StartDate: "2024-06-01",
		EndDate:   "2024-06-30",
		Filters:   map[string]any{},
	}

	assert.NotNil(t, params.Filters)
	assert.Empty(t, params.Filters)
}
