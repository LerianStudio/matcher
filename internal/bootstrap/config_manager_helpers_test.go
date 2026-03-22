// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// --- valuesEquivalent and toFloat64 tests ---

func TestValuesEquivalent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		left  any
		right any
		want  bool
	}{
		{"int_vs_float64", 100, float64(100), true},
		{"int64_vs_float64", int64(42), float64(42), true},
		{"string_equal", "hello", "hello", true},
		{"int_not_equal", 100, 200, false},
		{"string_not_equal", "a", "b", false},
		{"nil_vs_nil", nil, nil, true},
		{"nil_vs_zero", nil, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := valuesEquivalent(tt.left, tt.right)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestToFloat64(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		input     any
		wantValue float64
		wantOK    bool
	}{
		{"int", int(42), 42.0, true},
		{"int64", int64(100), 100.0, true},
		{"float64", float64(3.14), 3.14, true},
		{"uint", uint(10), 10.0, true},
		{"string", "hello", 0, false},
		{"nil", nil, 0, false},
		{"bool", true, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gotValue, gotOK := toFloat64(tt.input)
			assert.Equal(t, tt.wantOK, gotOK)
			assert.InDelta(t, tt.wantValue, gotValue, floatEqualityTolerance)
		})
	}
}
