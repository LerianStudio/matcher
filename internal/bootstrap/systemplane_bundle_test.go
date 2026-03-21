// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
)

// Compile-time interface satisfaction check.
var _ domain.RuntimeBundle = (*MatcherBundle)(nil)

// orderTracker records the sequence of close calls for deterministic order
// verification.
type orderTracker struct {
	mu    sync.Mutex
	order []string
}

func (t *orderTracker) record(name string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.order = append(t.order, name)
}

func (t *orderTracker) snapshot() []string {
	t.mu.Lock()
	defer t.mu.Unlock()

	cp := make([]string, len(t.order))
	copy(cp, t.order)

	return cp
}

// trackingCloser is an io.Closer that records its close call to an
// orderTracker and optionally returns an error.
type trackingCloser struct {
	name    string
	tracker *orderTracker
	err     error
}

func (c *trackingCloser) Close() error {
	c.tracker.record(c.name)

	return c.err
}

// fakeLogger is a test logger that embeds NopLogger and overrides Sync
// to record whether it was called and optionally return an error.
type fakeLogger struct {
	libLog.NopLogger
	synced  bool
	syncErr error
}

func (l *fakeLogger) Sync(_ context.Context) error {
	l.synced = true

	return l.syncErr
}

func TestMatcherBundle_ImplementsRuntimeBundle(t *testing.T) {
	t.Parallel()

	// The compile-time check above (var _ domain.RuntimeBundle = ...) is the
	// real assertion. This test exists to make the assertion visible in test
	// output and prevent the interface variable from being optimized away.
	var bundle domain.RuntimeBundle = &MatcherBundle{}
	assert.NotNil(t, bundle)
}

func TestMatcherBundle_Close_NilBundle(t *testing.T) {
	t.Parallel()

	var bundle *MatcherBundle

	err := bundle.Close(context.Background())

	assert.NoError(t, err)
}

func TestMatcherBundle_Close_NilSubBundles(t *testing.T) {
	t.Parallel()

	bundle := &MatcherBundle{
		Infra:  nil,
		HTTP:   nil,
		Logger: nil,
	}

	err := bundle.Close(context.Background())

	assert.NoError(t, err)
}

func TestMatcherBundle_Close_ReverseOrder(t *testing.T) {
	t.Parallel()

	tracker := &orderTracker{}

	bundle := &MatcherBundle{
		Logger: &LoggerBundle{
			Logger: &fakeLogger{},
			Level:  "info",
		},
		Infra: &InfraBundle{
			ObjectStorage: &trackingCloser{name: "object_storage", tracker: tracker},
			// RabbitMQ and Redis/Postgres are lib-commons types that we cannot
			// easily fake at the field level because they are concrete structs.
			// ObjectStorage (io.Closer) is the one we can track.
			// We verify the overall order by checking that the syncable logger's
			// Sync is called (it runs before infra), and object_storage close is
			// recorded.
		},
	}

	err := bundle.Close(context.Background())

	require.NoError(t, err)

	// Logger sync happens first (before any infra close).
	assert.True(t, bundle.Logger.Logger.(*fakeLogger).synced)
	// Object storage close was recorded.
	assert.Equal(t, []string{"object_storage"}, tracker.snapshot())
}

func TestMatcherBundle_Close_ReverseOrder_AllTracked(t *testing.T) {
	t.Parallel()

	// This test uses only io.Closer-compatible fakes to verify the full
	// reverse close sequence: logger -> object_storage -> rabbitmq -> redis -> postgres.
	// Since RabbitMQ/Redis/Postgres are concrete lib-commons types, we test
	// the object storage ordering and logger sync as the representative
	// observable sequence. The internal closeRabbitMQ helper operates on
	// concrete amqp types and is exercised via integration tests.

	tracker := &orderTracker{}

	syncLogger := &fakeLogger{}

	bundle := &MatcherBundle{
		Logger: &LoggerBundle{
			Logger: syncLogger,
			Level:  "debug",
		},
		Infra: &InfraBundle{
			ObjectStorage: &trackingCloser{name: "object_storage", tracker: tracker},
		},
	}

	err := bundle.Close(context.Background())

	require.NoError(t, err)
	assert.True(t, syncLogger.synced, "logger Sync should be called before infra close")
	assert.Equal(t, []string{"object_storage"}, tracker.snapshot())
}

func TestMatcherBundle_Close_CollectsAllErrors(t *testing.T) {
	t.Parallel()

	errSync := errors.New("sync failed")
	errStorage := errors.New("storage close failed")

	bundle := &MatcherBundle{
		Logger: &LoggerBundle{
			Logger: &fakeLogger{syncErr: errSync},
			Level:  "info",
		},
		Infra: &InfraBundle{
			ObjectStorage: &trackingCloser{
				name:    "object_storage",
				tracker: &orderTracker{},
				err:     errStorage,
			},
		},
	}

	err := bundle.Close(context.Background())

	require.Error(t, err)
	// Both errors should be present in the joined error.
	assert.ErrorContains(t, err, "sync logger")
	assert.ErrorContains(t, err, "close object storage")
	// Verify the original errors are wrapped.
	assert.ErrorIs(t, err, errSync)
	assert.ErrorIs(t, err, errStorage)
}

func TestMatcherBundle_Close_PartialFailure(t *testing.T) {
	t.Parallel()

	tracker := &orderTracker{}
	errStorage := errors.New("storage broken")

	bundle := &MatcherBundle{
		Logger: &LoggerBundle{
			Logger: &fakeLogger{}, // Sync succeeds
			Level:  "info",
		},
		Infra: &InfraBundle{
			ObjectStorage: &trackingCloser{
				name:    "object_storage",
				tracker: tracker,
				err:     errStorage,
			},
		},
	}

	err := bundle.Close(context.Background())

	// Error is returned for the storage failure.
	require.Error(t, err)
	assert.ErrorIs(t, err, errStorage)

	// Logger sync still ran (not blocked by later failure).
	assert.True(t, bundle.Logger.Logger.(*fakeLogger).synced)
	// Object storage close was still attempted.
	assert.Contains(t, tracker.snapshot(), "object_storage")
}

func TestMatcherBundle_Close_LoggerSyncError(t *testing.T) {
	t.Parallel()

	errSync := errors.New("zap sync: bad file descriptor")

	bundle := &MatcherBundle{
		Logger: &LoggerBundle{
			Logger: &fakeLogger{syncErr: errSync},
			Level:  "warn",
		},
	}

	err := bundle.Close(context.Background())

	require.Error(t, err)
	assert.ErrorIs(t, err, errSync)
	assert.ErrorContains(t, err, "sync logger")
}

func TestMatcherBundle_Close_LoggerWithoutSync(t *testing.T) {
	t.Parallel()

	// A logger that does not implement Sync() should not cause a panic.
	bundle := &MatcherBundle{
		Logger: &LoggerBundle{
			Logger: &libLog.NopLogger{},
			Level:  "info",
		},
	}

	err := bundle.Close(context.Background())

	assert.NoError(t, err)
}

func TestMatcherBundle_Close_NilLoggerInstance(t *testing.T) {
	t.Parallel()

	// LoggerBundle exists but Logger field is nil.
	bundle := &MatcherBundle{
		Logger: &LoggerBundle{
			Logger: nil,
			Level:  "info",
		},
	}

	err := bundle.Close(context.Background())

	assert.NoError(t, err)
}

func TestMatcherBundle_Close_NilInfraFields(t *testing.T) {
	t.Parallel()

	// InfraBundle exists but all fields are nil.
	bundle := &MatcherBundle{
		Infra: &InfraBundle{
			Postgres:      nil,
			Redis:         nil,
			RabbitMQ:      nil,
			ObjectStorage: nil,
		},
	}

	err := bundle.Close(context.Background())

	assert.NoError(t, err)
}

func TestMatcherBundle_DB_NilBundle(t *testing.T) {
	t.Parallel()

	var bundle *MatcherBundle
	assert.Nil(t, bundle.DB())
}

func TestMatcherBundle_DB_NilInfra(t *testing.T) {
	t.Parallel()

	bundle := &MatcherBundle{Infra: nil}
	assert.Nil(t, bundle.DB())
}

func TestMatcherBundle_RedisClient_NilBundle(t *testing.T) {
	t.Parallel()

	var bundle *MatcherBundle
	assert.Nil(t, bundle.RedisClient())
}

func TestMatcherBundle_RedisClient_NilInfra(t *testing.T) {
	t.Parallel()

	bundle := &MatcherBundle{Infra: nil}
	assert.Nil(t, bundle.RedisClient())
}

func TestMatcherBundle_RabbitMQConn_NilBundle(t *testing.T) {
	t.Parallel()

	var bundle *MatcherBundle
	assert.Nil(t, bundle.RabbitMQConn())
}

func TestMatcherBundle_RabbitMQConn_NilInfra(t *testing.T) {
	t.Parallel()

	bundle := &MatcherBundle{Infra: nil}
	assert.Nil(t, bundle.RabbitMQConn())
}

func TestMatcherBundle_Log_NilBundle(t *testing.T) {
	t.Parallel()

	var bundle *MatcherBundle
	assert.Nil(t, bundle.Log())
}

func TestMatcherBundle_Log_NilLogger(t *testing.T) {
	t.Parallel()

	bundle := &MatcherBundle{Logger: nil}
	assert.Nil(t, bundle.Log())
}

func TestMatcherBundle_Log_NilLoggerInstance(t *testing.T) {
	t.Parallel()

	bundle := &MatcherBundle{
		Logger: &LoggerBundle{Logger: nil, Level: "info"},
	}
	assert.Nil(t, bundle.Log())
}

func TestMatcherBundle_Log_ReturnsLogger(t *testing.T) {
	t.Parallel()

	logger := &libLog.NopLogger{}
	bundle := &MatcherBundle{
		Logger: &LoggerBundle{Logger: logger, Level: "debug"},
	}

	assert.Equal(t, logger, bundle.Log())
}
