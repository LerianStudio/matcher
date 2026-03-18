//go:build unit

// Copyright 2025 Lerian Studio.

package bootstrap

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type noopStore struct{}

func (noopStore) Get(_ context.Context, _ domain.Target) (ports.ReadResult, error) {
	return ports.ReadResult{}, nil
}

func (noopStore) Put(
	_ context.Context,
	_ domain.Target,
	_ []ports.WriteOp,
	_ domain.Revision,
	_ domain.Actor,
	_ string,
) (domain.Revision, error) {
	return domain.RevisionZero, nil
}

type noopHistoryStore struct{}

func (noopHistoryStore) ListHistory(_ context.Context, _ ports.HistoryFilter) ([]ports.HistoryEntry, error) {
	return nil, nil
}

type noopChangeFeed struct{}

func (noopChangeFeed) Subscribe(_ context.Context, _ func(ports.ChangeSignal)) error {
	return nil
}

type noopCloser struct{}

func (noopCloser) Close() error { return nil }

func TestNewBackendFromConfig_NilConfig(t *testing.T) {
	res, err := NewBackendFromConfig(context.Background(), nil)

	require.Error(t, err)
	assert.Nil(t, res)
	assert.ErrorIs(t, err, errNilBackendConfig)
	assert.Contains(t, err.Error(), "config is nil")
}

func TestNewBackendFromConfig_MissingBackend(t *testing.T) {
	cfg := &BootstrapConfig{
		Backend: domain.BackendKind(""),
	}

	res, err := NewBackendFromConfig(context.Background(), cfg)

	require.Error(t, err)
	assert.Nil(t, res)
	assert.ErrorIs(t, err, ErrMissingBackend)
}

func TestNewBackendFromConfig_PostgresMissingConfig(t *testing.T) {
	cfg := &BootstrapConfig{
		Backend:  domain.BackendPostgres,
		Postgres: nil,
	}

	res, err := NewBackendFromConfig(context.Background(), cfg)

	require.Error(t, err)
	assert.Nil(t, res)
	assert.ErrorIs(t, err, ErrMissingPostgresConfig)
}

func TestNewBackendFromConfig_MongoMissingConfig(t *testing.T) {
	cfg := &BootstrapConfig{
		Backend: domain.BackendMongoDB,
		MongoDB: nil,
	}

	res, err := NewBackendFromConfig(context.Background(), cfg)

	require.Error(t, err)
	assert.Nil(t, res)
	assert.ErrorIs(t, err, ErrMissingMongoConfig)
}

func TestNewBackendFromConfig_PostgresMissingDSN(t *testing.T) {
	cfg := &BootstrapConfig{
		Backend: domain.BackendPostgres,
		Postgres: &PostgresBootstrapConfig{
			DSN: "",
		},
	}

	res, err := NewBackendFromConfig(context.Background(), cfg)

	require.Error(t, err)
	assert.Nil(t, res)
	assert.ErrorIs(t, err, ErrMissingPostgresDSN)
}

func TestNewBackendFromConfig_MongoMissingURI(t *testing.T) {
	cfg := &BootstrapConfig{
		Backend: domain.BackendMongoDB,
		MongoDB: &MongoBootstrapConfig{
			URI: "",
		},
	}

	res, err := NewBackendFromConfig(context.Background(), cfg)

	require.Error(t, err)
	assert.Nil(t, res)
	assert.ErrorIs(t, err, ErrMissingMongoURI)
}

func TestNewBackendFromConfig_UnsupportedBackend(t *testing.T) {
	// "redis" is not a valid BackendKind, so Validate rejects it via
	// ErrMissingBackend before the factory lookup is even reached.
	cfg := &BootstrapConfig{
		Backend: domain.BackendKind("redis"),
	}

	res, err := NewBackendFromConfig(context.Background(), cfg)

	require.Error(t, err)
	assert.Nil(t, res)
	assert.Contains(t, err.Error(), "redis")
}

func TestNewBackendFromConfig_NoFactoryRegistered(t *testing.T) {
	// Temporarily remove the postgres factory to simulate an unregistered backend.
	// Save the original and restore after the test.
	original, existed := backendFactories[domain.BackendPostgres]
	delete(backendFactories, domain.BackendPostgres)

	t.Cleanup(func() {
		if existed {
			backendFactories[domain.BackendPostgres] = original
		}
	})

	cfg := &BootstrapConfig{
		Backend: domain.BackendPostgres,
		Postgres: &PostgresBootstrapConfig{
			DSN: "postgres://user:pass@localhost:5432/db",
		},
	}

	res, err := NewBackendFromConfig(context.Background(), cfg)

	require.Error(t, err)
	assert.Nil(t, res)
	assert.Contains(t, err.Error(), "no factory registered")
}

func TestNewBackendFromConfig_FactoryReturnsError(t *testing.T) {
	// Not parallel: mutates global backendFactories for postgres kind.
	// Register a factory that always fails, for a custom test backend kind.
	// We use postgres kind here with a stub factory.
	testKind := domain.BackendPostgres
	original, existed := backendFactories[testKind]

	expectedErr := fmt.Errorf("simulated connection failure")
	backendFactories[testKind] = func(_ context.Context, _ *BootstrapConfig) (*BackendResources, error) {
		return nil, expectedErr
	}

	t.Cleanup(func() {
		if existed {
			backendFactories[testKind] = original
		} else {
			delete(backendFactories, testKind)
		}
	})

	cfg := &BootstrapConfig{
		Backend: testKind,
		Postgres: &PostgresBootstrapConfig{
			DSN: "postgres://user:pass@localhost:5432/db",
		},
	}

	res, err := NewBackendFromConfig(context.Background(), cfg)

	require.Error(t, err)
	assert.Nil(t, res)
	assert.ErrorIs(t, err, expectedErr)
}

func TestNewBackendFromConfig_FactoryReturnsResources(t *testing.T) {
	// Not parallel: mutates global backendFactories for postgres kind.
	testKind := domain.BackendPostgres
	original, existed := backendFactories[testKind]

	expected := &BackendResources{Store: noopStore{}, History: noopHistoryStore{}, ChangeFeed: noopChangeFeed{}, Closer: noopCloser{}}
	backendFactories[testKind] = func(_ context.Context, _ *BootstrapConfig) (*BackendResources, error) {
		return expected, nil
	}

	t.Cleanup(func() {
		if existed {
			backendFactories[testKind] = original
		} else {
			delete(backendFactories, testKind)
		}
	})

	cfg := &BootstrapConfig{
		Backend: testKind,
		Postgres: &PostgresBootstrapConfig{
			DSN: "postgres://user:pass@localhost:5432/db",
		},
	}

	res, err := NewBackendFromConfig(context.Background(), cfg)

	require.NoError(t, err)
	assert.Equal(t, expected, res)
}

func TestNewBackendFromConfig_FactoryReturnsIncompleteResources(t *testing.T) {
	// Not parallel: mutates global backendFactories for postgres kind.
	tests := []struct {
		name        string
		resources   *BackendResources
		expectedErr error
	}{
		{
			name:        "nil resources",
			resources:   nil,
			expectedErr: errNilBackendResources,
		},
		{
			name:        "nil store",
			resources:   &BackendResources{History: noopHistoryStore{}, ChangeFeed: noopChangeFeed{}, Closer: noopCloser{}},
			expectedErr: errNilBackendStore,
		},
		{
			name:        "nil history store",
			resources:   &BackendResources{Store: noopStore{}, ChangeFeed: noopChangeFeed{}, Closer: noopCloser{}},
			expectedErr: errNilBackendHistoryStore,
		},
		{
			name:        "nil change feed",
			resources:   &BackendResources{Store: noopStore{}, History: noopHistoryStore{}, Closer: noopCloser{}},
			expectedErr: errNilBackendChangeFeed,
		},
		{
			name:        "nil closer",
			resources:   &BackendResources{Store: noopStore{}, History: noopHistoryStore{}, ChangeFeed: noopChangeFeed{}},
			expectedErr: errNilBackendCloser,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			testKind := domain.BackendPostgres
			original, existed := backendFactories[testKind]

			backendFactories[testKind] = func(_ context.Context, _ *BootstrapConfig) (*BackendResources, error) {
				return tc.resources, nil
			}

			t.Cleanup(func() {
				if existed {
					backendFactories[testKind] = original
				} else {
					delete(backendFactories, testKind)
				}
			})

			cfg := &BootstrapConfig{
				Backend: testKind,
				Postgres: &PostgresBootstrapConfig{
					DSN: "postgres://user:pass@localhost:5432/db",
				},
			}

			res, err := NewBackendFromConfig(context.Background(), cfg)

			require.Error(t, err)
			assert.Nil(t, res)
			assert.ErrorIs(t, err, tc.expectedErr)
		})
	}
}

func TestNewBackendFromConfig_AppliesDefaults(t *testing.T) {
	// Not parallel: mutates global backendFactories for postgres kind.
	testKind := domain.BackendPostgres
	original, existed := backendFactories[testKind]

	var capturedCfg *BootstrapConfig

	backendFactories[testKind] = func(_ context.Context, cfg *BootstrapConfig) (*BackendResources, error) {
		capturedCfg = cfg
		return &BackendResources{Store: noopStore{}, History: noopHistoryStore{}, ChangeFeed: noopChangeFeed{}, Closer: noopCloser{}}, nil
	}

	t.Cleanup(func() {
		if existed {
			backendFactories[testKind] = original
		} else {
			delete(backendFactories, testKind)
		}
	})

	cfg := &BootstrapConfig{
		Backend: testKind,
		Postgres: &PostgresBootstrapConfig{
			DSN: "postgres://user:pass@localhost:5432/db",
			// Leave Schema, EntriesTable, etc. empty — ApplyDefaults should fill them.
		},
	}

	_, err := NewBackendFromConfig(context.Background(), cfg)

	require.NoError(t, err)
	require.NotNil(t, capturedCfg)
	assert.Equal(t, "system", capturedCfg.Postgres.Schema)
	assert.Equal(t, "runtime_entries", capturedCfg.Postgres.EntriesTable)
	assert.Equal(t, "runtime_history", capturedCfg.Postgres.HistoryTable)
	assert.Equal(t, "runtime_revisions", capturedCfg.Postgres.RevisionTable)
	assert.Equal(t, "systemplane_changes", capturedCfg.Postgres.NotifyChannel)
}

func TestRegisterBackendFactory_RejectsOverwrite(t *testing.T) {
	testKind := domain.BackendPostgres
	original, existed := backendFactories[testKind]

	t.Cleanup(func() {
		if existed {
			backendFactories[testKind] = original
		} else {
			delete(backendFactories, testKind)
		}
	})
	delete(backendFactories, testKind)

	firstCalled := false
	secondCalled := false

	err := RegisterBackendFactory(testKind, func(_ context.Context, _ *BootstrapConfig) (*BackendResources, error) {
		firstCalled = true
		return &BackendResources{Store: noopStore{}, History: noopHistoryStore{}, ChangeFeed: noopChangeFeed{}, Closer: noopCloser{}}, nil
	})
	require.NoError(t, err)

	err = RegisterBackendFactory(testKind, func(_ context.Context, _ *BootstrapConfig) (*BackendResources, error) {
		secondCalled = true
		return &BackendResources{Store: noopStore{}, History: noopHistoryStore{}, ChangeFeed: noopChangeFeed{}, Closer: noopCloser{}}, nil
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, errBackendAlreadyRegistered)

	factory := backendFactories[testKind]
	require.NotNil(t, factory)

	_, _ = factory(context.Background(), nil)

	assert.True(t, firstCalled, "first factory should remain registered")
	assert.False(t, secondCalled, "second factory should not be registered")
}

func TestRegisterBackendFactory_RejectsNilFactory(t *testing.T) {
	kind := domain.BackendMongoDB
	original, existed := backendFactories[kind]
	backendFactories[kind] = func(_ context.Context, _ *BootstrapConfig) (*BackendResources, error) {
		return &BackendResources{Store: noopStore{}, History: noopHistoryStore{}, ChangeFeed: noopChangeFeed{}, Closer: noopCloser{}}, nil
	}

	t.Cleanup(func() {
		if existed {
			backendFactories[kind] = original
		} else {
			delete(backendFactories, kind)
		}
	})

	err := RegisterBackendFactory(kind, nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, errNilBackendFactory)
	_, ok := backendFactories[kind]
	assert.True(t, ok)
}

var _ io.Closer = noopCloser{}
