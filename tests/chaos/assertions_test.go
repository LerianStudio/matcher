//go:build chaos

package chaos

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOutboxStats_String(t *testing.T) {
	t.Parallel()

	stats := OutboxStats{
		Pending:    5,
		Processing: 2,
		Published:  10,
		Failed:     1,
		Invalid:    0,
		Total:      18,
	}

	s := stats.String()
	assert.Contains(t, s, "total=18")
	assert.Contains(t, s, "pending=5")
	assert.Contains(t, s, "processing=2")
	assert.Contains(t, s, "published=10")
	assert.Contains(t, s, "failed=1")
	assert.Contains(t, s, "invalid=0")
}

func TestOutboxStats_String_ZeroValues(t *testing.T) {
	t.Parallel()

	stats := OutboxStats{}

	s := stats.String()
	assert.Contains(t, s, "total=0")
	assert.Contains(t, s, "pending=0")
}

func TestHealthStatus_Fields(t *testing.T) {
	t.Parallel()

	hs := HealthStatus{
		Status: "healthy",
		Checks: map[string]string{
			"postgres": "ok",
			"redis":    "ok",
		},
	}

	assert.Equal(t, "healthy", hs.Status)
	assert.Len(t, hs.Checks, 2)
	assert.Equal(t, "ok", hs.Checks["postgres"])
}
