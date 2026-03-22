//go:build unit

// Copyright 2025 Lerian Studio.

package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQualify(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		schema   string
		table    string
		expected string
	}{
		{
			name:     "standard schema and table",
			schema:   "system",
			table:    "runtime_entries",
			expected: "system.runtime_entries",
		},
		{
			name:     "custom schema and table",
			schema:   "myschema",
			table:    "my_entries",
			expected: "myschema.my_entries",
		},
		{
			name:     "single character names",
			schema:   "s",
			table:    "t",
			expected: "s.t",
		},
		{
			name:     "underscore prefixed names",
			schema:   "_private",
			table:    "_data",
			expected: "_private._data",
		},
		{
			name:     "empty strings still concatenate",
			schema:   "",
			table:    "",
			expected: ".",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := qualify(tt.schema, tt.table)
			assert.Equal(t, tt.expected, result)
		})
	}
}
