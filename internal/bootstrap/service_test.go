// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/bxcodec/dbresolver/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

// mockApp satisfies libCommons.App for testing.
type mockApp struct{}

func (m *mockApp) Run(_ *libCommons.Launcher) error { return nil }

func TestServiceRun(t *testing.T) {
	t.Parallel()

	t.Run("with nil service does not panic", func(t *testing.T) {
		t.Parallel()

		var svc *Service

		assert.NotPanics(t, func() {
			_ = svc.Run()
		})
	})

	t.Run("with nil service returns nil error", func(t *testing.T) {
		t.Parallel()

		var svc *Service

		err := svc.Run()

		assert.NoError(t, err)
	})
}

func TestServiceStruct(t *testing.T) {
	t.Parallel()

	t.Run("can be instantiated with all fields", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			App: AppConfig{EnvName: "test"},
			Server: ServerConfig{
				Address: ":4018",
			},
		}
		routes := &Routes{}

		svc := &Service{
			Server: nil,
			Config: cfg,
			Routes: routes,
		}

		assert.NotNil(t, svc)
		assert.Equal(t, cfg, svc.Config)
		assert.Equal(t, routes, svc.Routes)
	})
}

func TestServiceShutdown(t *testing.T) {
	t.Parallel()

	t.Run("with nil service returns nil", func(t *testing.T) {
		t.Parallel()

		var svc *Service

		err := svc.Shutdown(context.Background())

		assert.NoError(t, err)
	})

	t.Run("with valid server shuts down successfully", func(t *testing.T) {
		t.Parallel()

		fiberApp := fiber.New()
		svc := &Service{
			Server: &Server{
				app:    fiberApp,
				cfg:    &Config{},
				logger: &libLog.NopLogger{},
			},
			Logger: &libLog.NopLogger{},
		}

		err := svc.Shutdown(context.Background())

		require.NoError(t, err)
	})

	t.Run("with nil server but valid service returns nil", func(t *testing.T) {
		t.Parallel()

		svc := &Service{
			Server: nil,
			Logger: &libLog.NopLogger{},
		}

		err := svc.Shutdown(context.Background())

		assert.NoError(t, err)
	})
}

func TestServiceShutdownWithWorkers(t *testing.T) {
	t.Parallel()

	t.Run("with dbMetricsCollector stops collector", func(t *testing.T) {
		t.Parallel()

		fiberApp := fiber.New()
		collector := &DBMetricsCollector{
			stopCh: make(chan struct{}),
		}

		svc := &Service{
			Server: &Server{
				app:    fiberApp,
				cfg:    &Config{},
				logger: &libLog.NopLogger{},
			},
			Logger:             &libLog.NopLogger{},
			dbMetricsCollector: collector,
		}

		err := svc.Shutdown(context.Background())

		require.NoError(t, err)
	})
}

func TestServiceRun_NilService(t *testing.T) {
	t.Parallel()

	t.Run("returns nil error for nil service", func(t *testing.T) {
		t.Parallel()

		var svc *Service

		err := svc.Run()

		assert.NoError(t, err, "nil service should return nil error from Run()")
	})
}

func TestServiceShutdown_ClosesConnectionsAndStopsDispatcher(t *testing.T) {
	t.Parallel()

	fiberApp := fiber.New()
	sqlDB, _, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, sqlDB.Close())
	})

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(sqlDB))
	postgres := testutil.NewClientWithResolver(resolver)
	redis := testutil.NewRedisClientConnected()

	rabbitmq := createRabbitMQConnection(&Config{
		RabbitMQ: RabbitMQConfig{
			Host:     "localhost",
			Port:     "5672",
			User:     "guest",
			Password: "guest",
			VHost:    "/",
		},
	}, &libLog.NopLogger{})

	server := &Server{
		app:      fiberApp,
		cfg:      &Config{},
		logger:   &libLog.NopLogger{},
		postgres: postgres,
		redis:    redis,
		rabbitmq: rabbitmq,
	}

	stopper := &mockStopper{}
	closer := &mockCloser{}

	svc := &Service{
		Server:            server,
		Logger:            &libLog.NopLogger{},
		outboxRunner:      stopper,
		connectionManager: closer,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err = svc.Shutdown(ctx)

	require.NoError(t, err)
	assert.True(t, stopper.stopped)
	assert.True(t, closer.closed)
}

type mockStopper struct {
	stopped bool
}

func (m *mockStopper) Run(_ *libCommons.Launcher) error {
	return nil
}

func (m *mockStopper) Stop() {
	m.stopped = true
}

type mockCloser struct {
	closed bool
}

type recordingLifecycleWorker struct {
	startErr     error
	stopObserved bool
	stopFn       func()
}

func (worker *recordingLifecycleWorker) Start(_ context.Context) error {
	return worker.startErr
}

func (worker *recordingLifecycleWorker) Stop() error {
	if worker.stopFn != nil {
		worker.stopFn()
	}

	return nil
}

func (m *mockCloser) Close() error {
	m.closed = true

	return nil
}

func TestServiceResolveActiveConfig_UsesConfigManagerSnapshot(t *testing.T) {
	t.Parallel()

	initialCfg := defaultConfig()
	initialCfg.App.LogLevel = "info"
	managedCfg := defaultConfig()
	managedCfg.App.LogLevel = "debug"

	cm, err := NewConfigManager(managedCfg, "", &libLog.NopLogger{})
	require.NoError(t, err)
	t.Cleanup(cm.Stop)

	svc := &Service{
		Config:        initialCfg,
		ConfigManager: cm,
	}

	resolved := svc.resolveActiveConfig()
	require.NotNil(t, resolved)
	assert.Equal(t, "debug", resolved.App.LogLevel)
	assert.Equal(t, resolved, svc.Config)
}

func TestServiceRun_PropagatesWorkerManagerStartFailure(t *testing.T) {
	t.Parallel()

	worker := &recordingLifecycleWorker{startErr: errors.New("worker start failed")}
	wm := NewWorkerManager(&libLog.NopLogger{}, nil)
	wm.Register("critical", func(_ *Config) (WorkerLifecycle, error) {
		return worker, nil
	}, alwaysEnabled, alwaysCritical)

	svc := &Service{
		Logger:        &libLog.NopLogger{},
		Config:        defaultConfig(),
		workerManager: wm,
	}

	err := svc.Run()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "critical worker \"critical\" failed to start")
}

func TestServiceStopBackgroundWorkers_StopsConfigManagerBeforeWorkers(t *testing.T) {
	t.Parallel()

	cm, err := NewConfigManager(defaultConfig(), "", &libLog.NopLogger{})
	require.NoError(t, err)
	t.Cleanup(cm.Stop)

	worker := &recordingLifecycleWorker{}
	worker.stopFn = func() {
		select {
		case <-cm.stopCh:
			worker.stopObserved = true
		default:
			worker.stopObserved = false
		}
	}

	wm := NewWorkerManager(&libLog.NopLogger{}, nil)
	wm.Register("worker", func(_ *Config) (WorkerLifecycle, error) {
		return worker, nil
	}, alwaysEnabled, neverCritical)
	require.NoError(t, wm.Start(context.Background(), defaultConfig()))

	svc := &Service{
		ConfigManager: cm,
		workerManager: wm,
	}

	svc.stopBackgroundWorkers(context.Background(), &libLog.NopLogger{})
	assert.True(t, worker.stopObserved)
}

func TestServerRun_StartsAndShutdowns(t *testing.T) {
	t.Parallel()

	address, err := reserveLoopbackAddress()
	require.NoError(t, err)

	app := fiber.New()
	cfg := &Config{
		Server: ServerConfig{
			Address: address,
		},
	}
	srv := &Server{
		app:    app,
		cfg:    cfg,
		logger: &libLog.NopLogger{},
	}

	// Start server in background
	errCh := make(chan error, 1)

	go func() {
		errCh <- srv.Run(nil)
	}()

	waitForServerToListen(t, address, time.Second)

	// Shutdown immediately
	shutdownErr := srv.Shutdown(context.Background())
	assert.NoError(t, shutdownErr)

	// Wait for run to complete
	select {
	case err := <-errCh:
		// The server should shut down cleanly - no error expected
		assert.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("server did not shut down in time")
	}
}

func reserveLoopbackAddress() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}

	address := listener.Addr().String()

	if err := listener.Close(); err != nil {
		return "", err
	}

	return address, nil
}

func waitForServerToListen(t *testing.T, address string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 100 * time.Millisecond}
	url := "http://" + address + "/ready"

	for time.Now().Before(deadline) {
		req, reqErr := http.NewRequestWithContext(context.Background(), http.MethodGet, url, http.NoBody)
		if reqErr != nil {
			t.Fatalf("failed to build readiness probe request: %v", reqErr)
		}

		req.Close = true

		resp, err := client.Do(req)
		if err == nil {
			if closeErr := resp.Body.Close(); closeErr != nil {
				t.Logf("failed to close probe connection: %v", closeErr)
			}

			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("server did not start listening on %s within %v", address, timeout)
}

func TestServerRun_WithTLSMissingFiles(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	cfg := &Config{
		Server: ServerConfig{
			Address:     ":0",
			TLSCertFile: "/nonexistent/cert.pem",
			TLSKeyFile:  "/nonexistent/key.pem",
		},
	}
	srv := &Server{
		app:    app,
		cfg:    cfg,
		logger: &libLog.NopLogger{},
	}

	// Run should fail immediately since TLS files don't exist
	err := srv.Run(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "server listen tls")
}

func TestStopBackgroundWorkers_AllNilWorkers(t *testing.T) {
	t.Parallel()

	svc := &Service{
		Logger:             &libLog.NopLogger{},
		dbMetricsCollector: nil,
		outboxRunner:       nil,
	}

	assert.NotPanics(t, func() {
		svc.stopBackgroundWorkers(context.Background(), &libLog.NopLogger{})
	})
}

func TestStopBackgroundWorkers_WithOutboxRunnerNonStoppable(t *testing.T) {
	t.Parallel()

	// An outbox runner that does NOT implement the Stop() method
	nonStoppable := &mockNonStoppable{}

	svc := &Service{
		Logger:       &libLog.NopLogger{},
		outboxRunner: nonStoppable,
	}

	assert.NotPanics(t, func() {
		svc.stopBackgroundWorkers(context.Background(), &libLog.NopLogger{})
	})
}

type mockNonStoppable struct{}

func (m *mockNonStoppable) Run(_ *libCommons.Launcher) error {
	return nil
}

func TestStopBackgroundWorkers_WithStoppableOutboxRunner(t *testing.T) {
	t.Parallel()

	stoppable := &mockStopper{}

	svc := &Service{
		Logger:       &libLog.NopLogger{},
		outboxRunner: stoppable,
	}

	svc.stopBackgroundWorkers(context.Background(), &libLog.NopLogger{})

	assert.True(t, stoppable.stopped)
}

func TestStopBackgroundWorkers_WithDBMetricsCollector(t *testing.T) {
	t.Parallel()

	collector := &DBMetricsCollector{
		stopCh: make(chan struct{}),
	}

	svc := &Service{
		Logger:             &libLog.NopLogger{},
		dbMetricsCollector: collector,
	}

	assert.NotPanics(t, func() {
		svc.stopBackgroundWorkers(context.Background(), &libLog.NopLogger{})
	})
}

func TestShutdownServerAndConnections_NilServer(t *testing.T) {
	t.Parallel()

	svc := &Service{
		Server: nil,
		Logger: &libLog.NopLogger{},
	}

	err := svc.shutdownServerAndConnections(context.Background(), &libLog.NopLogger{}, nil)
	assert.NoError(t, err)
}

func TestShutdownServerAndConnections_WithConnectionManager(t *testing.T) {
	t.Parallel()

	fiberApp := fiber.New()
	closer := &mockCloser{}

	svc := &Service{
		Server: &Server{
			app:    fiberApp,
			cfg:    &Config{},
			logger: &libLog.NopLogger{},
		},
		Logger:            &libLog.NopLogger{},
		connectionManager: closer,
	}

	err := svc.shutdownServerAndConnections(context.Background(), &libLog.NopLogger{}, nil)

	assert.NoError(t, err)
	assert.True(t, closer.closed)
}

func TestShutdownServerAndConnections_RunsRegisteredCleanupFuncs(t *testing.T) {
	t.Parallel()

	fiberApp := fiber.New()
	cleanupCalls := 0

	svc := &Service{
		Server: &Server{
			app:    fiberApp,
			cfg:    &Config{},
			logger: &libLog.NopLogger{},
		},
		Logger: &libLog.NopLogger{},
		cleanupFuncs: []func(){
			func() { cleanupCalls++ },
			func() { cleanupCalls++ },
		},
	}

	err := svc.shutdownServerAndConnections(context.Background(), &libLog.NopLogger{}, nil)

	assert.NoError(t, err)
	assert.Equal(t, 2, cleanupCalls)
	assert.Nil(t, svc.cleanupFuncs)

	// Ensure cleanups are not executed twice.
	err = svc.shutdownServerAndConnections(context.Background(), &libLog.NopLogger{}, nil)
	assert.NoError(t, err)
	assert.Equal(t, 2, cleanupCalls)
}

func TestServiceShutdown_WithNilLogger(t *testing.T) {
	t.Parallel()

	fiberApp := fiber.New()
	svc := &Service{
		Server: &Server{
			app:    fiberApp,
			cfg:    &Config{},
			logger: &libLog.NopLogger{},
		},
		Logger: nil,
	}

	err := svc.Shutdown(context.Background())

	assert.NoError(t, err)
}

func TestShutdownServerAndConnections_ConnectionManagerError(t *testing.T) {
	t.Parallel()

	fiberApp := fiber.New()
	failCloser := &mockFailCloser{}

	svc := &Service{
		Server: &Server{
			app:    fiberApp,
			cfg:    &Config{},
			logger: &libLog.NopLogger{},
		},
		Logger:            &libLog.NopLogger{},
		connectionManager: failCloser,
	}

	err := svc.shutdownServerAndConnections(context.Background(), &libLog.NopLogger{}, nil)

	require.Error(t, err)
	assert.True(t, failCloser.closeCalled)
}

type mockFailCloser struct {
	closeCalled bool
}

func (m *mockFailCloser) Close() error {
	m.closeCalled = true

	return errors.New("close failed")
}

func TestShutdownServerAndConnections_ServerShutdownError(t *testing.T) {
	t.Parallel()

	// Create a server with nil app to trigger shutdown error
	svc := &Service{
		Server: &Server{
			app:    nil,
			cfg:    &Config{},
			logger: &libLog.NopLogger{},
		},
		Logger: &libLog.NopLogger{},
	}

	err := svc.shutdownServerAndConnections(context.Background(), &libLog.NopLogger{}, nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "server not initialized")
}

func TestService_GetOutboxRunner(t *testing.T) {
	t.Parallel()

	t.Run("returns nil when service is nil", func(t *testing.T) {
		t.Parallel()

		var svc *Service

		got := svc.GetOutboxRunner()

		assert.Nil(t, got)
	})

	t.Run("returns nil when outbox runner not set", func(t *testing.T) {
		t.Parallel()

		svc := &Service{
			Logger: &libLog.NopLogger{},
		}

		got := svc.GetOutboxRunner()

		assert.Nil(t, got)
	})

	t.Run("returns outbox runner when set", func(t *testing.T) {
		t.Parallel()

		runner := &mockApp{}
		svc := &Service{
			Logger:       &libLog.NopLogger{},
			outboxRunner: runner,
		}

		got := svc.GetOutboxRunner()

		assert.NotNil(t, got)
		assert.Same(t, runner, got)
	})
}
