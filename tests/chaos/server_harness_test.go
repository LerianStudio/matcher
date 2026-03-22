//go:build unit

package chaos

import (
	"encoding/csv"
	"strings"
	"testing"
	"time"

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

func TestDispatchOutboxUntilEmpty_StopsAtZero(t *testing.T) {
	t.Parallel()

	sequence := []int{3, 2, 0, 99}
	index := 0
	total := dispatchOutboxUntilEmpty(5, func(time.Duration) {}, func() int {
		value := sequence[index]
		index++
		return value
	})

	assert.Equal(t, 5, total)
	assert.Equal(t, 3, index)
}

func TestDispatchOutboxUntilEmpty_NilEquivalentDispatcher(t *testing.T) {
	t.Parallel()

	total := dispatchOutboxUntilEmpty(5, nil, func() int { return 0 })
	assert.Equal(t, 0, total)
}
