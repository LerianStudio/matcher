//go:build chaos

package chaos

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildCSVContent_ZeroRows(t *testing.T) {
	t.Parallel()

	csv := BuildCSVContent(0)
	assert.Equal(t, "external_id,date,amount,currency\n", csv)
}

func TestBuildCSVContent_SingleRow(t *testing.T) {
	t.Parallel()

	csv := BuildCSVContent(1)
	assert.Contains(t, csv, "external_id,date,amount,currency\n")
	assert.Contains(t, csv, "CHAOS-00000,2025-01-15,100.00,USD\n")
}

func TestBuildCSVContent_MultipleRows(t *testing.T) {
	t.Parallel()

	csv := BuildCSVContent(3)
	assert.Contains(t, csv, "CHAOS-00000,2025-01-15,100.00,USD\n")
	assert.Contains(t, csv, "CHAOS-00001,2025-01-15,200.00,USD\n")
	assert.Contains(t, csv, "CHAOS-00002,2025-01-15,300.00,USD\n")
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
