//go:build unit

package fee

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNormalizationMode_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		mode  NormalizationMode
		valid bool
	}{
		{
			name:  "None (empty string) is valid",
			mode:  NormalizationModeNone,
			valid: true,
		},
		{
			name:  "NET is valid",
			mode:  NormalizationModeNet,
			valid: true,
		},
		{
			name:  "GROSS is valid",
			mode:  NormalizationModeGross,
			valid: true,
		},
		{
			name:  "INVALID is not valid",
			mode:  NormalizationMode("INVALID"),
			valid: false,
		},
		{
			name:  "lowercase net is not valid (case-sensitive)",
			mode:  NormalizationMode("net"),
			valid: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.valid, tc.mode.IsValid())
		})
	}
}
