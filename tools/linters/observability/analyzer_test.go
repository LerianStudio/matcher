//go:build unit

package observability

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAnalyzer(t *testing.T) {
	t.Parallel()

	assert.NotNil(t, Analyzer)
	assert.Equal(t, "observability", Analyzer.Name)
	assert.NotEmpty(t, Analyzer.Doc)
	assert.NotNil(t, Analyzer.Run)
}

func TestIsServicePackage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pkgPath  string
		expected bool
	}{
		{"command service", "github.com/example/internal/services/command", true},
		{"query service", "github.com/example/internal/services/query", true},
		{"adapters", "github.com/example/internal/adapters/http", false},
		{"domain", "github.com/example/internal/domain", false},
		{"services root", "github.com/example/internal/services", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := isServicePackage(tt.pkgPath)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsServiceMethod(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		method   string
		expected bool
	}{
		{"Execute method", "Execute", true},
		{"Run method", "Run", true},
		{"Handle method", "Handle", true},
		{"Get method", "Get", false},
		{"Create method", "Create", false},
		{"helper method", "helper", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := isServiceMethod(tt.method)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestObservabilityChecks(t *testing.T) {
	t.Parallel()

	checks := &observabilityChecks{}

	assert.False(t, checks.hasTrackingExtraction)
	assert.False(t, checks.hasSpanStart)
	assert.False(t, checks.hasDeferSpanEnd)

	checks.hasTrackingExtraction = true
	checks.hasSpanStart = true
	checks.hasDeferSpanEnd = true

	assert.True(t, checks.hasTrackingExtraction)
	assert.True(t, checks.hasSpanStart)
	assert.True(t, checks.hasDeferSpanEnd)
}
