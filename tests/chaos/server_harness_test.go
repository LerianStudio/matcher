//go:build chaos

package chaos

import (
	"encoding/csv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildCSVContent_ZeroRows(t *testing.T) {
	t.Parallel()

	content := BuildCSVContent(0)
	assert.Equal(t, "external_id,date,amount,currency\n", content)
}

func TestBuildCSVContent_SingleRow(t *testing.T) {
	t.Parallel()

	content := BuildCSVContent(1)
	records, err := csv.NewReader(strings.NewReader(content)).ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 2)
	assert.Equal(t, []string{"external_id", "date", "amount", "currency"}, records[0])
	assert.Equal(t, []string{"CHAOS-00000", "2025-01-15", "100.00", "USD"}, records[1])
}

func TestBuildCSVContent_MultipleRows(t *testing.T) {
	t.Parallel()

	content := BuildCSVContent(3)
	records, err := csv.NewReader(strings.NewReader(content)).ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 4)
	assert.Equal(t, []string{"CHAOS-00000", "2025-01-15", "100.00", "USD"}, records[1])
	assert.Equal(t, []string{"CHAOS-00001", "2025-01-15", "200.00", "USD"}, records[2])
	assert.Equal(t, []string{"CHAOS-00002", "2025-01-15", "300.00", "USD"}, records[3])
}

func TestChaosServer_DispatchOutbox_NilDispatcher(t *testing.T) {
	t.Parallel()

	cs := &ChaosServer{
		Dispatcher: nil,
	}

	result := cs.DispatchOutbox(t)
	assert.Equal(t, 0, result)
}

func TestChaosServer_DispatchOutboxUntilEmpty_NilDispatcher(t *testing.T) {
	t.Parallel()

	cs := &ChaosServer{
		Dispatcher: nil,
	}

	total := cs.DispatchOutboxUntilEmpty(t, 5)
	assert.Equal(t, 0, total)
}
