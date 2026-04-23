//go:build unit

package command

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

func TestExtractionSupport_BuildExtractionJobInput_ConfigNameFallback(t *testing.T) {
	t.Parallel()

	conn := testConnectionEntity()
	conn.ConfigName = ""
	conn.ProductName = "matcher"

	input, err := buildExtractionJobInput(conn, map[string]any{
		"transactions": map[string]any{"columns": []string{"id", "amount"}},
	}, sharedPorts.ExtractionParams{
		Filters: &sharedPorts.ExtractionFilters{Equals: map[string]string{"currency": "USD"}},
	})

	require.NoError(t, err)
	require.Contains(t, input.MappedFields, conn.FetcherConnID)
	assert.Equal(t, conn.ProductName, input.Metadata["source"])
	require.Contains(t, input.Filters, conn.FetcherConnID)
}

func TestExtractionSupport_FlattenReturnedTableColumns(t *testing.T) {
	t.Parallel()

	flattened := flattenReturnedTableColumns(map[string]map[string][]string{
		"prod-db": {
			"transactions": {"id", "amount"},
		},
	})

	require.NotNil(t, flattened)
	assert.Equal(t, []string{"id", "amount"}, flattened["transactions"])
	assert.Nil(t, flattenReturnedTableColumns(nil))
}

func TestExtractionSupport_LogMappedFieldsDivergence_BoolContract(t *testing.T) {
	t.Parallel()

	assert.False(t, logMappedFieldsDivergence(context.Background(), map[string][]string{"transactions": {"id"}}, map[string]map[string][]string{
		"prod-db": {"transactions": {"id"}},
	}, "job-1"))

	assert.True(t, logMappedFieldsDivergence(context.Background(), map[string][]string{"transactions": {"id"}}, map[string]map[string][]string{
		"prod-db": {"public.transactions": {"id"}},
	}, "job-1"))
}
