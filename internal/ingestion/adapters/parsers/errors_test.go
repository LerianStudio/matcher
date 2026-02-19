//go:build unit

package parsers

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSanitizeErrorMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		msg            string
		forProduction  bool
		wantContains   string
		wantNotContain string
	}{
		{
			name:          "dev mode returns original",
			msg:           "error reading /Users/test/file.csv: invalid format",
			forProduction: false,
			wantContains:  "/Users/test/file.csv",
		},
		{
			name:           "production removes file paths",
			msg:            "error reading /Users/test/file.csv: invalid format",
			forProduction:  true,
			wantContains:   "[path]",
			wantNotContain: "/Users/test",
		},
		{
			name:           "production removes unix paths",
			msg:            "failed to open /var/log/app/error.log",
			forProduction:  true,
			wantContains:   "[path]",
			wantNotContain: "/var/log",
		},
		{
			name:          "production truncates long messages",
			msg:           strings.Repeat("a", 300),
			forProduction: true,
			wantContains:  "...",
		},
		{
			name:          "safe message unchanged in production",
			msg:           "parse error at row 5",
			forProduction: true,
			wantContains:  "parse error at row 5",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := SanitizeErrorMessage(tt.msg, tt.forProduction)
			assert.Contains(t, result, tt.wantContains)

			if tt.wantNotContain != "" {
				assert.NotContains(t, result, tt.wantNotContain)
			}
		})
	}
}

func TestGenericParseError(t *testing.T) {
	t.Parallel()

	result := GenericParseError(42)
	assert.Equal(t, "parse error at row 42", result)
}
