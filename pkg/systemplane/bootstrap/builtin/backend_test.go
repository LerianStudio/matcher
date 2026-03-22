//go:build unit

// Copyright 2025 Lerian Studio.

package builtin

import (
	"context"
	"io"
	"testing"

	"github.com/LerianStudio/matcher/pkg/systemplane/bootstrap"
	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Stubs for BackendResources fields
// ---------------------------------------------------------------------------

type stubStore struct{}

func (stubStore) Get(_ context.Context, _ domain.Target) (ports.ReadResult, error) {
	return ports.ReadResult{}, nil
}

func (stubStore) Put(
	_ context.Context,
	_ domain.Target,
	_ []ports.WriteOp,
	_ domain.Revision,
	_ domain.Actor,
	_ string,
) (domain.Revision, error) {
	return domain.RevisionZero, nil
}

type stubHistoryStore struct{}

func (stubHistoryStore) ListHistory(_ context.Context, _ ports.HistoryFilter) ([]ports.HistoryEntry, error) {
	return nil, nil
}

type stubChangeFeed struct{}

func (stubChangeFeed) Subscribe(_ context.Context, _ func(ports.ChangeSignal)) error {
	return nil
}

type stubCloser struct{}

func (stubCloser) Close() error { return nil }

var _ io.Closer = stubCloser{}

// ---------------------------------------------------------------------------
// Test: init() registers both postgres and mongodb factories
// ---------------------------------------------------------------------------

func TestInit_RegistersPostgresFactory(t *testing.T) {
	t.Parallel()

	// The init() function in backend.go registers a postgres factory.
	// We cannot call it directly, but importing the package triggers it.
	// Verify by attempting to build a config that would succeed if the
	// factory is registered (it will fail at the connection level, not
	// at the "no factory registered" level).
	cfg := &bootstrap.BootstrapConfig{
		Backend: domain.BackendPostgres,
		Postgres: &bootstrap.PostgresBootstrapConfig{
			DSN: "postgres://user:pass@localhost:5432/db",
		},
	}

	// The factory IS registered (by init), so the error should NOT be
	// "no factory registered" — it will fail at the actual connection
	// attempt level instead.
	_, err := NewBackendFromConfig(context.Background(), cfg)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "no factory registered")
}

func TestInit_RegistersMongoDBFactory(t *testing.T) {
	t.Parallel()

	cfg := &bootstrap.BootstrapConfig{
		Backend: domain.BackendMongoDB,
		MongoDB: &bootstrap.MongoBootstrapConfig{
			URI:      "mongodb://localhost:27017",
			Database: "testdb",
		},
	}

	// The factory IS registered (by init), so the error should NOT be
	// "no factory registered" — it will fail at the actual connection
	// attempt level instead.
	_, err := NewBackendFromConfig(context.Background(), cfg)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "no factory registered")
}

// ---------------------------------------------------------------------------
// Test: NewBackendFromConfig delegates to bootstrap.NewBackendFromConfig
// ---------------------------------------------------------------------------

func TestNewBackendFromConfig_NilConfig(t *testing.T) {
	t.Parallel()

	res, err := NewBackendFromConfig(context.Background(), nil)

	require.Error(t, err)
	assert.Nil(t, res)
	assert.Contains(t, err.Error(), "builtin backend")
	assert.Contains(t, err.Error(), "config is nil")
}

func TestNewBackendFromConfig_MissingBackend(t *testing.T) {
	t.Parallel()

	cfg := &bootstrap.BootstrapConfig{
		Backend: domain.BackendKind(""),
	}

	res, err := NewBackendFromConfig(context.Background(), cfg)

	require.Error(t, err)
	assert.Nil(t, res)
	assert.Contains(t, err.Error(), "builtin backend")
}

func TestNewBackendFromConfig_PostgresMissingDSN(t *testing.T) {
	t.Parallel()

	cfg := &bootstrap.BootstrapConfig{
		Backend: domain.BackendPostgres,
		Postgres: &bootstrap.PostgresBootstrapConfig{
			DSN: "",
		},
	}

	res, err := NewBackendFromConfig(context.Background(), cfg)

	require.Error(t, err)
	assert.Nil(t, res)
	assert.Contains(t, err.Error(), "builtin backend")
}

func TestNewBackendFromConfig_MongoMissingURI(t *testing.T) {
	t.Parallel()

	cfg := &bootstrap.BootstrapConfig{
		Backend: domain.BackendMongoDB,
		MongoDB: &bootstrap.MongoBootstrapConfig{
			URI: "",
		},
	}

	res, err := NewBackendFromConfig(context.Background(), cfg)

	require.Error(t, err)
	assert.Nil(t, res)
	assert.Contains(t, err.Error(), "builtin backend")
}

func TestNewBackendFromConfig_PostgresMissingConfigStruct(t *testing.T) {
	t.Parallel()

	cfg := &bootstrap.BootstrapConfig{
		Backend:  domain.BackendPostgres,
		Postgres: nil,
	}

	res, err := NewBackendFromConfig(context.Background(), cfg)

	require.Error(t, err)
	assert.Nil(t, res)
	assert.Contains(t, err.Error(), "builtin backend")
}

func TestNewBackendFromConfig_MongoMissingConfigStruct(t *testing.T) {
	t.Parallel()

	cfg := &bootstrap.BootstrapConfig{
		Backend: domain.BackendMongoDB,
		MongoDB: nil,
	}

	res, err := NewBackendFromConfig(context.Background(), cfg)

	require.Error(t, err)
	assert.Nil(t, res)
	assert.Contains(t, err.Error(), "builtin backend")
}

func TestNewBackendFromConfig_UnsupportedBackend(t *testing.T) {
	t.Parallel()

	cfg := &bootstrap.BootstrapConfig{
		Backend: domain.BackendKind("redis"),
	}

	res, err := NewBackendFromConfig(context.Background(), cfg)

	require.Error(t, err)
	assert.Nil(t, res)
	assert.Contains(t, err.Error(), "builtin backend")
}

func TestNewBackendFromConfig_WrapsErrorWithBuiltinPrefix(t *testing.T) {
	t.Parallel()

	// All validation errors should be wrapped with the "builtin backend:" prefix.
	tests := []struct {
		name string
		cfg  *bootstrap.BootstrapConfig
	}{
		{
			name: "nil config",
			cfg:  nil,
		},
		{
			name: "empty backend",
			cfg:  &bootstrap.BootstrapConfig{Backend: domain.BackendKind("")},
		},
		{
			name: "postgres without config struct",
			cfg:  &bootstrap.BootstrapConfig{Backend: domain.BackendPostgres, Postgres: nil},
		},
		{
			name: "mongo without config struct",
			cfg:  &bootstrap.BootstrapConfig{Backend: domain.BackendMongoDB, MongoDB: nil},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewBackendFromConfig(context.Background(), tc.cfg)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "builtin backend")
		})
	}
}

func TestNewBackendFromConfig_PostgresWhitespaceOnlyDSN(t *testing.T) {
	t.Parallel()

	cfg := &bootstrap.BootstrapConfig{
		Backend: domain.BackendPostgres,
		Postgres: &bootstrap.PostgresBootstrapConfig{
			DSN: "   ",
		},
	}

	res, err := NewBackendFromConfig(context.Background(), cfg)

	require.Error(t, err)
	assert.Nil(t, res)
}

func TestNewBackendFromConfig_MongoWhitespaceOnlyURI(t *testing.T) {
	t.Parallel()

	cfg := &bootstrap.BootstrapConfig{
		Backend: domain.BackendMongoDB,
		MongoDB: &bootstrap.MongoBootstrapConfig{
			URI: "   ",
		},
	}

	res, err := NewBackendFromConfig(context.Background(), cfg)

	require.Error(t, err)
	assert.Nil(t, res)
}

func TestNewBackendFromConfig_CancelledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cfg := &bootstrap.BootstrapConfig{
		Backend: domain.BackendPostgres,
		Postgres: &bootstrap.PostgresBootstrapConfig{
			DSN: "postgres://user:pass@localhost:5432/db",
		},
	}

	// Should still fail (connection attempt with cancelled context), not panic.
	_, err := NewBackendFromConfig(ctx, cfg)
	require.Error(t, err)
}
