//go:build unit

package common

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStringToNullString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected sql.NullString
	}{
		{
			name:     "non-empty string returns valid NullString",
			input:    "test",
			expected: sql.NullString{String: "test", Valid: true},
		},
		{
			name:     "empty string returns invalid NullString",
			input:    "",
			expected: sql.NullString{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := StringToNullString(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestStringPtrToNullString(t *testing.T) {
	t.Parallel()

	testValue := "test"

	tests := []struct {
		name     string
		input    *string
		expected sql.NullString
	}{
		{
			name:     "non-nil pointer returns valid NullString",
			input:    &testValue,
			expected: sql.NullString{String: "test", Valid: true},
		},
		{
			name:     "nil pointer returns invalid NullString",
			input:    nil,
			expected: sql.NullString{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := StringPtrToNullString(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNullStringToStringPtr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    sql.NullString
		expected *string
	}{
		{
			name:     "valid NullString returns pointer",
			input:    sql.NullString{String: "test", Valid: true},
			expected: ptrString("test"),
		},
		{
			name:     "invalid NullString returns nil",
			input:    sql.NullString{},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := NullStringToStringPtr(tt.input)
			if tt.expected == nil {
				assert.Nil(t, result)
			} else {
				assert.Equal(t, *tt.expected, *result)
			}
		})
	}
}

func TestTimePtrToNullTime(t *testing.T) {
	t.Parallel()

	testTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	tests := []struct {
		name     string
		input    *time.Time
		expected sql.NullTime
	}{
		{
			name:     "non-nil pointer returns valid NullTime",
			input:    &testTime,
			expected: sql.NullTime{Time: testTime, Valid: true},
		},
		{
			name:     "nil pointer returns invalid NullTime",
			input:    nil,
			expected: sql.NullTime{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := TimePtrToNullTime(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNullTimeToTimePtr(t *testing.T) {
	t.Parallel()

	testTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	tests := []struct {
		name     string
		input    sql.NullTime
		expected *time.Time
	}{
		{
			name:     "valid NullTime returns pointer",
			input:    sql.NullTime{Time: testTime, Valid: true},
			expected: &testTime,
		},
		{
			name:     "invalid NullTime returns nil",
			input:    sql.NullTime{},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := NullTimeToTimePtr(tt.input)
			if tt.expected == nil {
				assert.Nil(t, result)
			} else {
				assert.Equal(t, *tt.expected, *result)
			}
		})
	}
}

func ptrString(s string) *string {
	return &s
}
