//go:build unit

package fee_schedule

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

func TestSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "ErrRepoNotInitialized",
			err:      ErrRepoNotInitialized,
			expected: "fee schedule repository not initialized",
		},
		{
			name:     "ErrFeeScheduleModelNeeded",
			err:      ErrFeeScheduleModelNeeded,
			expected: "fee schedule model is required",
		},
		{
			name:     "ErrFeeScheduleNotFound",
			err:      fee.ErrFeeScheduleNotFound,
			expected: "fee schedule not found",
		},
		{
			name:     "ErrUnknownStructureType",
			err:      ErrUnknownStructureType,
			expected: "unknown fee structure type",
		},
		{
			name:     "ErrInvalidTx",
			err:      ErrInvalidTx,
			expected: "fee schedule repository invalid transaction",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.EqualError(t, tt.err, tt.expected)
		})
	}
}
