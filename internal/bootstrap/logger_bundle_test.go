//go:build unit

package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
)

func TestSyncRuntimeLogger_UsesBundleLoggerWhenLevelMatches(t *testing.T) {
	t.Parallel()

	first := &spyLogger{}
	second := &spyLogger{}
	logger := NewSwappableLogger(first)
	cfg := &Config{App: AppConfig{EnvName: "development", LogLevel: "debug"}}
	bundle := &MatcherBundle{Logger: &LoggerBundle{Logger: second, Level: "debug"}}

	require.NoError(t, syncRuntimeLogger(logger, cfg, bundle))
	assert.Same(t, second, logger.Current())
}

func TestSyncRuntimeLogger_RebuildsLoggerWhenBundleLevelIsStale(t *testing.T) {
	t.Parallel()

	first := &spyLogger{}
	stale := &spyLogger{}
	logger := NewSwappableLogger(first)
	cfg := &Config{App: AppConfig{EnvName: "development", LogLevel: "debug"}}
	bundle := &MatcherBundle{Logger: &LoggerBundle{Logger: stale, Level: "info"}}

	require.NoError(t, syncRuntimeLogger(logger, cfg, bundle))
	assert.NotSame(t, stale, logger.Current())
	assert.True(t, logger.Current().Enabled(libLog.LevelDebug))
}
