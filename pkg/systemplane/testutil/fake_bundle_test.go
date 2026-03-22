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

func TestFakeBundle_Close_MarksClosed(t *testing.T) {
	t.Parallel()

	bundle := &FakeBundle{}

	assert.False(t, bundle.Closed)

	err := bundle.Close(context.Background())

	require.NoError(t, err)
	assert.True(t, bundle.Closed)
}

func TestFakeBundle_Close_ReturnsConfiguredError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("close failed")
	bundle := &FakeBundle{CloseErr: wantErr}

	err := bundle.Close(context.Background())

	require.ErrorIs(t, err, wantErr)
	assert.True(t, bundle.Closed)
}

func TestFakeBundle_Close_NilError(t *testing.T) {
	t.Parallel()

	bundle := &FakeBundle{}

	err := bundle.Close(context.Background())

	require.NoError(t, err)
}

func TestFakeBundle_ImplementsRuntimeBundle(t *testing.T) {
	t.Parallel()

	var _ domain.RuntimeBundle = (*FakeBundle)(nil)
}

func TestNewFakeBundleFactory_ReturnsFactory(t *testing.T) {
	t.Parallel()

	factory := NewFakeBundleFactory()

	require.NotNil(t, factory)
	assert.NotNil(t, factory.BuildFn)
	assert.Equal(t, 0, factory.CallCount())
}

func TestFakeBundleFactory_ImplementsBundleFactory(t *testing.T) {
	t.Parallel()

	var _ ports.BundleFactory = (*FakeBundleFactory)(nil)
}

func TestFakeBundleFactory_Build_ReturnsFakeBundle(t *testing.T) {
	t.Parallel()

	factory := NewFakeBundleFactory()
	ctx := context.Background()
	snap := domain.Snapshot{}

	bundle, err := factory.Build(ctx, snap)

	require.NoError(t, err)
	require.NotNil(t, bundle)

	fb, ok := bundle.(*FakeBundle)
	assert.True(t, ok)
	assert.False(t, fb.Closed)
}

func TestFakeBundleFactory_Build_IncrementsCallCount(t *testing.T) {
	t.Parallel()

	factory := NewFakeBundleFactory()
	ctx := context.Background()
	snap := domain.Snapshot{}

	_, _ = factory.Build(ctx, snap)
	_, _ = factory.Build(ctx, snap)
	_, _ = factory.Build(ctx, snap)

	assert.Equal(t, 3, factory.CallCount())
}

func TestFakeBundleFactory_SetError_MakesBuildReturnError(t *testing.T) {
	t.Parallel()

	factory := NewFakeBundleFactory()
	wantErr := errors.New("build error")
	factory.SetError(wantErr)

	bundle, err := factory.Build(context.Background(), domain.Snapshot{})

	require.ErrorIs(t, err, wantErr)
	assert.Nil(t, bundle)
	assert.Equal(t, 1, factory.CallCount())
}

func TestFakeBundleFactory_SetError_OverridesPreviousFunc(t *testing.T) {
	t.Parallel()

	factory := NewFakeBundleFactory()

	// Default returns a bundle successfully.
	bundle, err := factory.Build(context.Background(), domain.Snapshot{})
	require.NoError(t, err)
	require.NotNil(t, bundle)

	// After SetError, Build returns an error.
	factory.SetError(errors.New("boom"))

	bundle, err = factory.Build(context.Background(), domain.Snapshot{})
	require.Error(t, err)
	assert.Nil(t, bundle)
}

func TestFakeBundleFactory_ConcurrentBuild(t *testing.T) {
	t.Parallel()

	factory := NewFakeBundleFactory()
	ctx := context.Background()
	snap := domain.Snapshot{}

	const goroutines = 50

	var wg sync.WaitGroup

	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()

			bundle, err := factory.Build(ctx, snap)
			assert.NoError(t, err)
			assert.NotNil(t, bundle)
		}()
	}

	wg.Wait()

	assert.Equal(t, goroutines, factory.CallCount())
}

func TestFakeBundleFactory_Build_CustomFunc(t *testing.T) {
	t.Parallel()

	custom := &FakeBundle{CloseErr: errors.New("custom")}
	factory := NewFakeBundleFactory()
	factory.BuildFn = func(_ context.Context, _ domain.Snapshot) (domain.RuntimeBundle, error) {
		return custom, nil
	}

	bundle, err := factory.Build(context.Background(), domain.Snapshot{})

	require.NoError(t, err)
	assert.Same(t, custom, bundle)
}
