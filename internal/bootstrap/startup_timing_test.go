//go:build unit

package bootstrap

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
)

func TestNewStartupTimer(t *testing.T) {
	t.Parallel()

	timer := newStartupTimer()

	require.NotNil(t, timer)
	assert.False(t, timer.start.IsZero())
	assert.Empty(t, timer.phases)
}

func TestStartupTimer_Track(t *testing.T) {
	t.Parallel()

	t.Run("records phase duration", func(t *testing.T) {
		t.Parallel()

		timer := newStartupTimer()
		done := timer.track("test-phase")
		time.Sleep(5 * time.Millisecond)
		done()

		timer.mu.Lock()
		defer timer.mu.Unlock()

		assert.Len(t, timer.phases, 1)
		assert.Equal(t, "test-phase", timer.phases[0].name)
		assert.GreaterOrEqual(t, timer.phases[0].duration, 4*time.Millisecond)
	})

	t.Run("tracks multiple phases in order", func(t *testing.T) {
		t.Parallel()

		timer := newStartupTimer()

		done := timer.track("phase-1")
		done()

		done = timer.track("phase-2")
		done()

		done = timer.track("phase-3")
		done()

		timer.mu.Lock()
		defer timer.mu.Unlock()

		assert.Len(t, timer.phases, 3)

		assert.Equal(t, "phase-1", timer.phases[0].name)
		assert.Equal(t, "phase-2", timer.phases[1].name)
		assert.Equal(t, "phase-3", timer.phases[2].name)
	})

	t.Run("concurrent tracks are safe", func(t *testing.T) {
		t.Parallel()

		timer := newStartupTimer()

		var wg sync.WaitGroup

		for i := range 10 {
			wg.Add(1)

			go func(idx int) {
				defer wg.Done()

				done := timer.track("concurrent")
				time.Sleep(time.Duration(idx) * time.Millisecond)
				done()
			}(i)
		}

		wg.Wait()

		timer.mu.Lock()
		defer timer.mu.Unlock()

		assert.Len(t, timer.phases, 10)
	})
}

func TestStartupTimer_Total(t *testing.T) {
	t.Parallel()

	timer := newStartupTimer()
	time.Sleep(5 * time.Millisecond)

	total := timer.total()
	assert.GreaterOrEqual(t, total, 4*time.Millisecond)
}

func TestStartupTimer_TrackNilReceiver(t *testing.T) {
	t.Parallel()

	var timer *startupTimer

	done := timer.track("ignored")

	assert.NotNil(t, done)
	assert.NotPanics(t, done)
}

func TestStartupTimer_PhaseCount(t *testing.T) {
	t.Parallel()

	timer := newStartupTimer()

	timer.mu.Lock()
	assert.Empty(t, timer.phases)
	timer.mu.Unlock()

	done := timer.track("a")
	done()

	timer.mu.Lock()
	defer timer.mu.Unlock()

	assert.Len(t, timer.phases, 1)
}

func TestFormatDuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    time.Duration
		expected string
	}{
		{
			name:     "nanoseconds",
			input:    500 * time.Nanosecond,
			expected: "500ns",
		},
		{
			name:     "microseconds",
			input:    500 * time.Microsecond,
			expected: "500us",
		},
		{
			name:     "milliseconds",
			input:    150 * time.Millisecond,
			expected: "150ms",
		},
		{
			name:     "seconds",
			input:    2500 * time.Millisecond,
			expected: "2.50s",
		},
		{
			name:     "zero",
			input:    0,
			expected: "0ns",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := formatDuration(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestLogStartupTiming_NilInputs(t *testing.T) {
	t.Parallel()

	t.Run("nil logger does not panic", func(t *testing.T) {
		t.Parallel()

		timer := newStartupTimer()
		assert.NotPanics(t, func() {
			logStartupTiming(nil, timer)
		})
	})

	t.Run("nil timer does not panic", func(t *testing.T) {
		t.Parallel()

		logger := &mockTimingLogger{}
		assert.NotPanics(t, func() {
			logStartupTiming(logger, nil)
		})
	})
}

func TestLogStartupTiming_LogsPhases(t *testing.T) {
	t.Parallel()

	logger := &mockTimingLogger{}
	timer := newStartupTimer()

	done := timer.track("config")
	done()

	done = timer.track("postgres")
	done()

	logStartupTiming(logger, timer)

	// Should contain the phase names in logged messages
	messages := logger.messages()
	foundConfig := false
	foundPostgres := false
	foundTotal := false

	for _, msg := range messages {
		if strings.Contains(msg, "config") {
			foundConfig = true
		}

		if strings.Contains(msg, "postgres") {
			foundPostgres = true
		}

		if strings.Contains(msg, "TOTAL") {
			foundTotal = true
		}
	}

	assert.True(t, foundConfig, "should log config phase")
	assert.True(t, foundPostgres, "should log postgres phase")
	assert.True(t, foundTotal, "should log total time")
}

// mockTimingLogger captures log messages for assertion.
type mockTimingLogger struct {
	mu   sync.Mutex
	msgs []string
}

func (m *mockTimingLogger) Log(_ context.Context, _ libLog.Level, msg string, _ ...libLog.Field) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.msgs = append(m.msgs, msg)
}

func (m *mockTimingLogger) messages() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]string, len(m.msgs))
	copy(result, m.msgs)

	return result
}

//nolint:ireturn
func (m *mockTimingLogger) With(_ ...libLog.Field) libLog.Logger { return m }

//nolint:ireturn
func (m *mockTimingLogger) WithGroup(_ string) libLog.Logger { return m }
func (m *mockTimingLogger) Enabled(_ libLog.Level) bool      { return true }
func (m *mockTimingLogger) Sync(_ context.Context) error     { return nil }
