//go:build unit

package main

import (
	"context"
	"errors"
	"testing"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libZap "github.com/LerianStudio/lib-commons/v5/commons/zap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/bootstrap"
)

var errTestMainExpected = errors.New("expected main test error")

type fakeMainLogger struct {
	syncCalls  int
	syncErr    error
	syncCtxErr error
}

func (l *fakeMainLogger) Log(context.Context, libLog.Level, string, ...libLog.Field) {}

func (l *fakeMainLogger) With(...libLog.Field) libLog.Logger { return l }

func (l *fakeMainLogger) WithGroup(string) libLog.Logger { return l }

func (l *fakeMainLogger) Enabled(libLog.Level) bool { return true }

func (l *fakeMainLogger) Sync(ctx context.Context) error {
	l.syncCalls++
	l.syncCtxErr = ctx.Err()

	return l.syncErr
}

type fakeMainService struct {
	runErr      error
	shutdownErr error
}

func (s *fakeMainService) Run() error { return s.runErr }

func (s *fakeMainService) Shutdown(context.Context) error { return s.shutdownErr }

func TestRun_LoggerInitFailure_ReturnsExitCodeOne(t *testing.T) {
	originalNewLogger := newLogger
	originalInitService := initMatcherService
	originalNotify := notifySignalContext
	originalInitLocalEnvConfig := initLocalEnvConfig

	t.Cleanup(func() {
		newLogger = originalNewLogger
		initMatcherService = originalInitService
		notifySignalContext = originalNotify
		initLocalEnvConfig = originalInitLocalEnvConfig
	})

	initLocalEnvConfig = func() {}

	newLogger = func(libZap.Config) (libLog.Logger, error) {
		return nil, errTestMainExpected
	}

	exitCode := run()
	require.Equal(t, 1, exitCode)
}

func TestRun_ServiceInitFailure_SyncsLoggerWithFreshContext(t *testing.T) {
	originalNewLogger := newLogger
	originalInitService := initMatcherService
	originalNotify := notifySignalContext
	originalInitLocalEnvConfig := initLocalEnvConfig

	fakeLogger := &fakeMainLogger{}

	t.Cleanup(func() {
		newLogger = originalNewLogger
		initMatcherService = originalInitService
		notifySignalContext = originalNotify
		initLocalEnvConfig = originalInitLocalEnvConfig
	})

	initLocalEnvConfig = func() {}

	newLogger = func(libZap.Config) (libLog.Logger, error) {
		return fakeLogger, nil
	}

	initMatcherService = func(*bootstrap.Options) (matcherService, error) {
		return nil, errTestMainExpected
	}

	exitCode := run()
	require.Equal(t, 1, exitCode)
	require.Equal(t, 1, fakeLogger.syncCalls)
	assert.NoError(t, fakeLogger.syncCtxErr)
}

func TestRun_ShutdownFailure_ReturnsExitCodeOne(t *testing.T) {
	originalNewLogger := newLogger
	originalInitService := initMatcherService
	originalNotify := notifySignalContext
	originalInitLocalEnvConfig := initLocalEnvConfig

	fakeLogger := &fakeMainLogger{}

	t.Cleanup(func() {
		newLogger = originalNewLogger
		initMatcherService = originalInitService
		notifySignalContext = originalNotify
		initLocalEnvConfig = originalInitLocalEnvConfig
	})

	initLocalEnvConfig = func() {}

	newLogger = func(libZap.Config) (libLog.Logger, error) {
		return fakeLogger, nil
	}

	initMatcherService = func(*bootstrap.Options) (matcherService, error) {
		return &fakeMainService{shutdownErr: errTestMainExpected}, nil
	}

	exitCode := run()
	require.Equal(t, 1, exitCode)
	require.Equal(t, 1, fakeLogger.syncCalls)
	assert.NoError(t, fakeLogger.syncCtxErr)
}

func TestRun_Success_ReturnsExitCodeZero(t *testing.T) {
	originalNewLogger := newLogger
	originalInitService := initMatcherService
	originalNotify := notifySignalContext
	originalInitLocalEnvConfig := initLocalEnvConfig

	fakeLogger := &fakeMainLogger{}

	t.Cleanup(func() {
		newLogger = originalNewLogger
		initMatcherService = originalInitService
		notifySignalContext = originalNotify
		initLocalEnvConfig = originalInitLocalEnvConfig
	})

	initLocalEnvConfig = func() {}

	newLogger = func(libZap.Config) (libLog.Logger, error) {
		return fakeLogger, nil
	}

	initMatcherService = func(*bootstrap.Options) (matcherService, error) {
		return &fakeMainService{}, nil
	}

	exitCode := run()
	require.Equal(t, 0, exitCode)
	require.Equal(t, 1, fakeLogger.syncCalls)
	assert.NoError(t, fakeLogger.syncCtxErr)
}
