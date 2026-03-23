//go:build unit

package parsers

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/ingestion/ports"
)

func TestParseTimeFormats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected time.Time
		wantErr  bool
	}{
		{
			name:     "RFC3339",
			input:    "2024-01-15T10:30:00Z",
			expected: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			name:     "RFC3339 with timezone",
			input:    "2024-01-15T10:30:00+05:00",
			expected: time.Date(2024, 1, 15, 10, 30, 0, 0, time.FixedZone("", 5*3600)),
		},
		{
			name:     "RFC3339Nano",
			input:    "2024-01-15T10:30:00.123456789Z",
			expected: time.Date(2024, 1, 15, 10, 30, 0, 123456789, time.UTC),
		},
		{
			name:     "ISO date with T separator",
			input:    "2024-01-15T10:30:00",
			expected: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			name:     "ISO datetime with space",
			input:    "2024-01-15 10:30:00",
			expected: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			name:     "ISO date only",
			input:    "2024-01-15",
			expected: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "slash datetime",
			input:    "2024/01/15 10:30:00",
			expected: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			name:     "slash date only",
			input:    "2024/01/15",
			expected: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "compact datetime",
			input:    "20240115103000",
			expected: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			name:     "compact date only",
			input:    "20240115",
			expected: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "DD-Mon-YYYY datetime",
			input:    "15-Jan-2024 10:30:00",
			expected: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			name:     "DD-Mon-YYYY date only",
			input:    "15-Jan-2024",
			expected: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "Mon D, YYYY datetime",
			input:    "Jan 15, 2024 10:30:00",
			expected: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			name:     "Mon D, YYYY date only",
			input:    "Jan 15, 2024",
			expected: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "full month name datetime",
			input:    "January 15, 2024 10:30:00",
			expected: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			name:     "full month name date only",
			input:    "January 15, 2024",
			expected: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "D Mon YYYY datetime",
			input:    "15 Jan 2024 10:30:00",
			expected: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			name:     "D Mon YYYY date only",
			input:    "15 Jan 2024",
			expected: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "Unix timestamp seconds",
			input:    "1705312200",
			expected: time.Date(2024, 1, 15, 9, 50, 0, 0, time.UTC),
		},
		{
			name:     "Unix timestamp milliseconds",
			input:    "1705312200000",
			expected: time.Date(2024, 1, 15, 9, 50, 0, 0, time.UTC),
		},
		{
			name:     "whitespace trimmed",
			input:    "  2024-01-15  ",
			expected: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
		{
			name:    "invalid format",
			input:   "not-a-date",
			wantErr: true,
		},
		{
			name:    "ambiguous US format rejected",
			input:   "01/15/2024",
			wantErr: true,
		},
		{
			name:    "ambiguous EU format rejected",
			input:   "15/01/2024",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := parseTime(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.True(t, result.Equal(tt.expected), "expected %v, got %v", tt.expected, result)
		})
	}
}

func TestParseUnixTimestamp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected time.Time
		ok       bool
	}{
		{
			name:     "valid seconds",
			input:    "1705312200",
			expected: time.Date(2024, 1, 15, 9, 50, 0, 0, time.UTC),
			ok:       true,
		},
		{
			name:     "valid milliseconds",
			input:    "1705312200000",
			expected: time.Date(2024, 1, 15, 9, 50, 0, 0, time.UTC),
			ok:       true,
		},
		{
			name:  "too short",
			input: "123456789",
			ok:    false,
		},
		{
			name:  "too long",
			input: "12345678901234",
			ok:    false,
		},
		{
			name:  "contains non-digits",
			input: "170531220a",
			ok:    false,
		},
		{
			name:  "11 digits rejected",
			input: "17053122000",
			ok:    false,
		},
		{
			name:  "12 digits rejected",
			input: "170531220000",
			ok:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, ok := parseUnixTimestamp(tt.input)
			require.Equal(t, tt.ok, ok)

			if tt.ok {
				require.True(
					t,
					result.Equal(tt.expected),
					"expected %v, got %v",
					tt.expected,
					result,
				)
			}
		})
	}
}

func TestUpdateDateRange(t *testing.T) {
	t.Parallel()

	t.Run("nil range initializes from single date", func(t *testing.T) {
		t.Parallel()

		date := time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)
		result := updateDateRange(nil, date)

		require.NotNil(t, result)
		require.Equal(t, date, result.Start)
		require.Equal(t, date, result.End)
	})

	t.Run("expands start when earlier", func(t *testing.T) {
		t.Parallel()

		start := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
		end := time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC)
		dateRange := &ports.DateRange{Start: start, End: end}

		newStart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		dateRange = updateDateRange(dateRange, newStart)
		require.Equal(t, newStart, dateRange.Start)
		require.Equal(t, end, dateRange.End)
	})

	t.Run("expands end when later", func(t *testing.T) {
		t.Parallel()

		start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		end := time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC)
		dateRange := &ports.DateRange{Start: start, End: end}

		newEnd := time.Date(2024, 1, 6, 0, 0, 0, 0, time.UTC)
		dateRange = updateDateRange(dateRange, newEnd)
		require.Equal(t, start, dateRange.Start)
		require.Equal(t, newEnd, dateRange.End)
	})
}
