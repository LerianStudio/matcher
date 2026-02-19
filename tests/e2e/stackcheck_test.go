//go:build e2e

package e2e

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewStackChecker(t *testing.T) {
	cfg := &E2EConfig{
		AppBaseURL:        "http://localhost:4018",
		PostgresHost:      "localhost",
		StackCheckTimeout: 5 * time.Second,
	}

	sc := NewStackChecker(cfg)

	require.NotNil(t, sc)
	assert.Equal(t, cfg, sc.cfg)
}

func TestStackCheckResult_Structure(t *testing.T) {
	result := StackCheckResult{
		Service: "test-service",
		OK:      true,
		Error:   nil,
		Latency: 100 * time.Millisecond,
	}

	assert.Equal(t, "test-service", result.Service)
	assert.True(t, result.OK)
	assert.Nil(t, result.Error)
	assert.Equal(t, 100*time.Millisecond, result.Latency)
}

func TestStackCheckResult_WithError(t *testing.T) {
	err := assert.AnError
	result := StackCheckResult{
		Service: "failing-service",
		OK:      false,
		Error:   err,
		Latency: 0,
	}

	assert.Equal(t, "failing-service", result.Service)
	assert.False(t, result.OK)
	assert.Equal(t, err, result.Error)
	assert.Equal(t, 0*time.Millisecond, result.Latency)
}

func TestFormatResults_AllSuccessful(t *testing.T) {
	results := []StackCheckResult{
		{Service: "app", OK: true, Latency: 50 * time.Millisecond},
		{Service: "postgres", OK: true, Latency: 10 * time.Millisecond},
		{Service: "redis", OK: true, Latency: 5 * time.Millisecond},
		{Service: "rabbitmq", OK: true, Latency: 15 * time.Millisecond},
	}

	output := FormatResults(results)

	assert.Contains(t, output, "Stack Health Check:")
	assert.Contains(t, output, "✓ app")
	assert.Contains(t, output, "✓ postgres")
	assert.Contains(t, output, "✓ redis")
	assert.Contains(t, output, "✓ rabbitmq")
	assert.Contains(t, output, "50ms")
	assert.Contains(t, output, "10ms")
	assert.Contains(t, output, "5ms")
	assert.Contains(t, output, "15ms")
}

func TestFormatResults_WithFailures(t *testing.T) {
	results := []StackCheckResult{
		{Service: "app", OK: true, Latency: 50 * time.Millisecond},
		{Service: "postgres", OK: false, Error: assert.AnError},
		{Service: "redis", OK: true, Latency: 5 * time.Millisecond},
	}

	output := FormatResults(results)

	assert.Contains(t, output, "✓ app")
	assert.Contains(t, output, "✗ postgres")
	assert.Contains(t, output, "✓ redis")
	assert.Contains(t, output, assert.AnError.Error())
}

func TestFormatResults_AllFailed(t *testing.T) {
	results := []StackCheckResult{
		{Service: "app", OK: false, Error: assert.AnError},
		{Service: "postgres", OK: false, Error: assert.AnError},
	}

	output := FormatResults(results)

	assert.Contains(t, output, "✗ app")
	assert.Contains(t, output, "✗ postgres")
	failedCount := strings.Count(output, "✗")
	assert.Equal(t, 2, failedCount)
}

func TestFormatResults_EmptyResults(t *testing.T) {
	results := []StackCheckResult{}

	output := FormatResults(results)

	assert.Contains(t, output, "Stack Health Check:")
	assert.NotContains(t, output, "✓")
	assert.NotContains(t, output, "✗")
}

func TestFormatResults_LatencyRounding(t *testing.T) {
	results := []StackCheckResult{
		{Service: "app", OK: true, Latency: 1234567 * time.Nanosecond},
	}

	output := FormatResults(results)

	assert.Contains(t, output, "1ms")
}
