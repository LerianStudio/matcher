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
	"go.opentelemetry.io/otel"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"
)

func TestInitTelemetry(t *testing.T) {
	t.Run(
		"with telemetry disabled returns telemetry struct with EnableTelemetry false",
		func(t *testing.T) {
			t.Parallel()

			cfg := &Config{
				Telemetry: TelemetryConfig{
					Enabled:        false,
					ServiceName:    "matcher-test",
					ServiceVersion: "1.0.0",
				},
			}
			logger := &mockLoggerForTelemetry{}

			telemetry := InitTelemetry(cfg, logger)

			require.NotNil(t, telemetry)
			assert.False(t, telemetry.EnableTelemetry)
		},
	)

	t.Run("with telemetry config sets service name", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			Telemetry: TelemetryConfig{
				Enabled:        false,
				ServiceName:    "test-matcher",
				ServiceVersion: "2.0.0",
				DeploymentEnv:  "test",
			},
		}
		logger := &mockLoggerForTelemetry{}

		telemetry := InitTelemetry(cfg, logger)

		require.NotNil(t, telemetry)
		assert.Equal(t, "test-matcher", telemetry.ServiceName)
		assert.Equal(t, "2.0.0", telemetry.ServiceVersion)
	})

	//nolint:paralleltest // Runs serially because InitTelemetry sets global OpenTelemetry propagator
	t.Run("sets global text map propagator for distributed tracing", func(t *testing.T) {
		cfg := &Config{
			Telemetry: TelemetryConfig{
				Enabled:        false,
				ServiceName:    "propagator-test",
				ServiceVersion: "1.0.0",
			},
		}
		logger := &mockLoggerForTelemetry{}

		_ = InitTelemetry(cfg, logger)

		propagator := otel.GetTextMapPropagator()
		require.NotNil(t, propagator)

		fields := propagator.Fields()
		assert.Contains(t, fields, "traceparent", "propagator should support W3C Trace Context")
	})
}

type mockLoggerForTelemetry struct{}

func (mockLoggerForTelemetry) Log(_ context.Context, _ libLog.Level, _ string, _ ...libLog.Field) {}

//nolint:ireturn
func (mockLoggerForTelemetry) With(_ ...libLog.Field) libLog.Logger { return mockLoggerForTelemetry{} }

//nolint:ireturn
func (mockLoggerForTelemetry) WithGroup(_ string) libLog.Logger { return mockLoggerForTelemetry{} }
func (mockLoggerForTelemetry) Enabled(_ libLog.Level) bool      { return true }
func (mockLoggerForTelemetry) Sync(_ context.Context) error     { return nil }

func TestInitTelemetry_NilConfig(t *testing.T) {
	telemetry := InitTelemetry(nil, &mockLoggerForTelemetry{})

	require.NotNil(t, telemetry)
	assert.False(t, telemetry.EnableTelemetry)
}

func TestInitTelemetryWithTimeout(t *testing.T) {
	t.Run("disabled telemetry returns immediately", func(t *testing.T) {
		cfg := &Config{
			Telemetry: TelemetryConfig{
				Enabled:        false,
				ServiceName:    "timeout-test",
				ServiceVersion: "1.0.0",
			},
		}
		logger := &mockLoggerForTelemetry{}

		telemetry := InitTelemetryWithTimeout(context.Background(), cfg, logger)

		require.NotNil(t, telemetry)
		assert.False(t, telemetry.EnableTelemetry)
	})

	t.Run("nil config defaults to disabled telemetry", func(t *testing.T) {
		logger := &mockLoggerForTelemetry{}

		telemetry := InitTelemetryWithTimeout(context.Background(), nil, logger)

		require.NotNil(t, telemetry)
		assert.False(t, telemetry.EnableTelemetry)
	})

	t.Run("enabled telemetry with unreachable collector returns valid instance", func(t *testing.T) {
		cfg := &Config{
			Telemetry: TelemetryConfig{
				Enabled:           true,
				ServiceName:       "timeout-test",
				ServiceVersion:    "1.0.0",
				CollectorEndpoint: "unreachable-host:4317",
			},
		}
		logger := &mockLoggerForTelemetry{}

		// gRPC uses lazy connect, so NewTelemetry returns immediately even with
		// unreachable hosts. The timeout protection is a safety net for edge cases
		// (DNS hangs, slow network) where the dial actually blocks.
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		start := time.Now()

		telemetry := InitTelemetryWithTimeout(ctx, cfg, logger)
		elapsed := time.Since(start)

		// Must always return a non-nil telemetry — either the real one
		// (gRPC lazy connect) or the disabled fallback (on timeout).
		require.NotNil(t, telemetry)
		assert.Equal(t, "timeout-test", telemetry.ServiceName)
		assert.Less(t, elapsed, 3*time.Second)
	})

	t.Run("timeout fallback logs warning and returns disabled telemetry", func(t *testing.T) {
		originalInitTelemetryFn := loadInitTelemetryFn()

		cfg := &Config{
			Telemetry: TelemetryConfig{
				Enabled:           true,
				ServiceName:       "timeout-fallback-test",
				ServiceVersion:    "1.0.0",
				CollectorEndpoint: "localhost:4317",
			},
		}
		logger := &capturingTelemetryLogger{}

		block := make(chan struct{})

		restore := setInitTelemetryFnForTest(func(*Config, libLog.Logger) *libOpentelemetry.Telemetry {
			<-block

			return originalInitTelemetryFn(&Config{
				Telemetry: TelemetryConfig{Enabled: false},
			}, logger)
		})
		t.Cleanup(restore)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		start := time.Now()

		telemetry := InitTelemetryWithTimeout(ctx, cfg, logger)
		elapsed := time.Since(start)
		close(block)

		require.NotNil(t, telemetry)
		assert.False(t, telemetry.EnableTelemetry)
		assert.True(t, logger.contains("telemetry initialization timed out"))
		assert.Less(t, elapsed, 200*time.Millisecond)
	})

	t.Run("expired parent context returns disabled telemetry quickly", func(t *testing.T) {
		block := make(chan struct{})

		restore := setInitTelemetryFnForTest(func(*Config, libLog.Logger) *libOpentelemetry.Telemetry {
			<-block

			return &libOpentelemetry.Telemetry{}
		})
		t.Cleanup(restore)

		cfg := &Config{
			Telemetry: TelemetryConfig{
				Enabled:        true,
				ServiceName:    "expired-parent-context",
				ServiceVersion: "1.0.0",
			},
		}

		expiredCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-1*time.Millisecond))
		defer cancel()

		start := time.Now()
		telemetry := InitTelemetryWithTimeout(expiredCtx, cfg, &mockLoggerForTelemetry{})
		elapsed := time.Since(start)
		close(block)

		require.NotNil(t, telemetry)
		assert.False(t, telemetry.EnableTelemetry)
		assert.Less(t, elapsed, 200*time.Millisecond)
	})

	t.Run("nil injected telemetry initializer falls back to InitTelemetry", func(t *testing.T) {
		restore := setInitTelemetryFnForTest(nil)
		t.Cleanup(restore)

		cfg := &Config{Telemetry: TelemetryConfig{Enabled: false}}

		telemetry := InitTelemetryWithTimeout(context.Background(), cfg, &mockLoggerForTelemetry{})

		require.NotNil(t, telemetry)
		assert.False(t, telemetry.EnableTelemetry)
	})
}

type capturingTelemetryLogger struct {
	mu       sync.Mutex
	messages []string
}

func (logger *capturingTelemetryLogger) Log(_ context.Context, _ libLog.Level, msg string, _ ...libLog.Field) {
	logger.mu.Lock()
	defer logger.mu.Unlock()

	logger.messages = append(logger.messages, msg)
}

//nolint:ireturn
func (logger *capturingTelemetryLogger) With(_ ...libLog.Field) libLog.Logger { return logger }

//nolint:ireturn
func (logger *capturingTelemetryLogger) WithGroup(_ string) libLog.Logger { return logger }

func (*capturingTelemetryLogger) Enabled(_ libLog.Level) bool { return true }

func (*capturingTelemetryLogger) Sync(_ context.Context) error { return nil }

func (logger *capturingTelemetryLogger) contains(target string) bool {
	logger.mu.Lock()
	defer logger.mu.Unlock()

	for _, msg := range logger.messages {
		if strings.Contains(msg, target) {
			return true
		}
	}

	return false
}
