// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package entityconstructor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAnalyzer(t *testing.T) {
	t.Parallel()

	assert.NotNil(t, Analyzer)
	assert.Equal(t, "entityconstructor", Analyzer.Name)
	assert.NotEmpty(t, Analyzer.Doc)
	assert.NotNil(t, Analyzer.Run)
}

func TestIsEntityPackage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pkgPath  string
		expected bool
	}{
		{"entities package", "github.com/example/internal/domain/entities", true},
		{"dispute package", "github.com/example/internal/domain/dispute", true},
		{"fee package", "github.com/example/internal/domain/fee", true},
		{"services package", "github.com/example/internal/services", false},
		{"adapters package", "github.com/example/internal/adapters/http", false},
		{"root package", "github.com/example", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := isEntityPackage(tt.pkgPath)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsVariantConstructor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		expectedTypeName string
		actualTypeName   string
		expected         bool
	}{
		{"exact match", "MatchItem", "MatchItem", true},
		{"variant with suffix", "MatchItemWithPolicy", "MatchItem", true},
		{"completely different", "User", "MatchItem", false},
		{"partial overlap", "Match", "MatchItem", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := isVariantConstructor(tt.expectedTypeName, tt.actualTypeName)
			assert.Equal(t, tt.expected, result)
		})
	}
}
