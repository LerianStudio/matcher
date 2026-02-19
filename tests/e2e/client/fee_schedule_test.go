//go:build e2e

package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewFeeScheduleClient(t *testing.T) {
	t.Parallel()

	base := NewClient("http://localhost:4018", "tenant-1", 0)
	fc := NewFeeScheduleClient(base)
	assert.NotNil(t, fc, "fee schedule client should not be nil")
}
