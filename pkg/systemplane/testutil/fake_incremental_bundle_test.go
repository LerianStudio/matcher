//go:build unit

// Copyright 2025 Lerian Studio.

package testutil

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFakeIncrementalBundleFactory_DefaultBehavior(t *testing.T) {
	t.Parallel()

	factory := NewFakeIncrementalBundleFactory()

	require.NotNil(t, factory)
	assert.NotNil(t, factory.IncrementalBuildFunc)
	assert.Equal(t, 0, factory.IncrementalCallCount())
}

func TestFakeIncrementalBundleFactory_ImplementsBundleFactory(t *testing.T) {
	t.Parallel()

	factory := NewFakeIncrementalBundleFactory()

	// Compile-time check is already in the source, but let's verify at runtime too.
	var bf ports.BundleFactory = factory
	require.NotNil(t, bf)

	var ibf ports.IncrementalBundleFactory = factory
	require.NotNil(t, ibf)
}

func TestFakeIncrementalBundleFactory_BuildIncremental_DefaultFunc(t *testing.T) {
	t.Parallel()

	factory := NewFakeIncrementalBundleFactory()
	ctx := context.Background()
	snap := domain.Snapshot{}
	prevSnap := domain.Snapshot{}
	previous := &FakeBundle{}

	bundle, err := factory.BuildIncremental(ctx, snap, previous, prevSnap)

	require.NoError(t, err)
	require.NotNil(t, bundle)

	_, isFakeBundle := bundle.(*FakeBundle)
	assert.True(t, isFakeBundle)
	assert.Equal(t, 1, factory.IncrementalCallCount())
}

func TestFakeIncrementalBundleFactory_BuildIncremental_CustomFunc(t *testing.T) {
	t.Parallel()

	expectedBundle := &FakeBundle{CloseErr: errors.New("custom")}
	factory := NewFakeIncrementalBundleFactory()
	factory.IncrementalBuildFunc = func(_ context.Context, _ domain.Snapshot,
		_ domain.RuntimeBundle, _ domain.Snapshot,
	) (domain.RuntimeBundle, error) {
		return expectedBundle, nil
	}

	bundle, err := factory.BuildIncremental(context.Background(), domain.Snapshot{}, nil, domain.Snapshot{})

	require.NoError(t, err)
	assert.Same(t, expectedBundle, bundle)
}

func TestFakeIncrementalBundleFactory_BuildIncremental_FuncReturnsError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("build boom")
	factory := NewFakeIncrementalBundleFactory()
	factory.IncrementalBuildFunc = func(_ context.Context, _ domain.Snapshot,
		_ domain.RuntimeBundle, _ domain.Snapshot,
	) (domain.RuntimeBundle, error) {
		return nil, wantErr
	}

	bundle, err := factory.BuildIncremental(context.Background(), domain.Snapshot{}, nil, domain.Snapshot{})

	require.ErrorIs(t, err, wantErr)
	assert.Nil(t, bundle)
}

func TestFakeIncrementalBundleFactory_BuildIncremental_NilFunc(t *testing.T) {
	t.Parallel()

	factory := NewFakeIncrementalBundleFactory()
	factory.IncrementalBuildFunc = nil

	bundle, err := factory.BuildIncremental(context.Background(), domain.Snapshot{}, nil, domain.Snapshot{})

	require.Error(t, err)
	assert.Nil(t, bundle)
	assert.Contains(t, err.Error(), "incremental build not configured")
}

func TestFakeIncrementalBundleFactory_IncrementalCallCount(t *testing.T) {
	t.Parallel()

	factory := NewFakeIncrementalBundleFactory()
	ctx := context.Background()
	snap := domain.Snapshot{}

	assert.Equal(t, 0, factory.IncrementalCallCount())

	_, _ = factory.BuildIncremental(ctx, snap, nil, snap)
	assert.Equal(t, 1, factory.IncrementalCallCount())

	_, _ = factory.BuildIncremental(ctx, snap, nil, snap)
	_, _ = factory.BuildIncremental(ctx, snap, nil, snap)
	assert.Equal(t, 3, factory.IncrementalCallCount())
}

func TestFakeIncrementalBundleFactory_Build_DelegatesToEmbedded(t *testing.T) {
	t.Parallel()

	factory := NewFakeIncrementalBundleFactory()
	ctx := context.Background()
	snap := domain.Snapshot{}

	bundle, err := factory.Build(ctx, snap)

	require.NoError(t, err)
	require.NotNil(t, bundle)
	assert.Equal(t, 1, factory.FakeBundleFactory.CallCount())
}

func TestFakeIncrementalBundleFactory_ConcurrentBuildIncremental(t *testing.T) {
	t.Parallel()

	factory := NewFakeIncrementalBundleFactory()
	ctx := context.Background()
	snap := domain.Snapshot{}

	const goroutines = 50

	var wg sync.WaitGroup

	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()

			bundle, err := factory.BuildIncremental(ctx, snap, nil, snap)
			assert.NoError(t, err)
			assert.NotNil(t, bundle)
		}()
	}

	wg.Wait()

	assert.Equal(t, goroutines, factory.IncrementalCallCount())
}
