//go:build unit

// Copyright 2025 Lerian Studio.

package ports

import (
	"context"
	"testing"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubBundle is a minimal RuntimeBundle for test doubles.
type stubBundle struct {
	closed bool
}

func (b *stubBundle) Close(_ context.Context) error {
	b.closed = true
	return nil
}

// stubBundleFactory is a minimal test double for BundleFactory.
type stubBundleFactory struct {
	bundle domain.RuntimeBundle
	err    error
}

func (f *stubBundleFactory) Build(_ context.Context, _ domain.Snapshot) (domain.RuntimeBundle, error) {
	return f.bundle, f.err
}

// stubIncrementalBundleFactory is a minimal test double for IncrementalBundleFactory.
type stubIncrementalBundleFactory struct {
	stubBundleFactory
	incrBundle domain.RuntimeBundle
	incrErr    error
}

func (f *stubIncrementalBundleFactory) BuildIncremental(
	_ context.Context, _ domain.Snapshot, _ domain.RuntimeBundle, _ domain.Snapshot,
) (domain.RuntimeBundle, error) {
	return f.incrBundle, f.incrErr
}

// Compile-time interface checks.
var (
	_ BundleFactory            = (*stubBundleFactory)(nil)
	_ IncrementalBundleFactory = (*stubIncrementalBundleFactory)(nil)
)

func TestBundleFactory_CompileCheck(t *testing.T) {
	t.Parallel()

	var factory BundleFactory = &stubBundleFactory{}
	require.NotNil(t, factory)
}

func TestBundleFactory_Build(t *testing.T) {
	t.Parallel()

	expected := &stubBundle{}
	factory := &stubBundleFactory{bundle: expected}

	bundle, err := factory.Build(context.Background(), domain.Snapshot{})

	require.NoError(t, err)
	assert.Same(t, expected, bundle)
}

func TestBundleFactory_Build_ReturnsError(t *testing.T) {
	t.Parallel()

	factory := &stubBundleFactory{err: assert.AnError}

	bundle, err := factory.Build(context.Background(), domain.Snapshot{})

	require.ErrorIs(t, err, assert.AnError)
	assert.Nil(t, bundle)
}

func TestIncrementalBundleFactory_CompileCheck(t *testing.T) {
	t.Parallel()

	var factory IncrementalBundleFactory = &stubIncrementalBundleFactory{}
	require.NotNil(t, factory)
}

func TestIncrementalBundleFactory_BuildIncremental(t *testing.T) {
	t.Parallel()

	expected := &stubBundle{}
	factory := &stubIncrementalBundleFactory{incrBundle: expected}

	bundle, err := factory.BuildIncremental(
		context.Background(), domain.Snapshot{}, nil, domain.Snapshot{},
	)

	require.NoError(t, err)
	assert.Same(t, expected, bundle)
}

func TestIncrementalBundleFactory_BuildIncremental_ReturnsError(t *testing.T) {
	t.Parallel()

	factory := &stubIncrementalBundleFactory{incrErr: assert.AnError}

	bundle, err := factory.BuildIncremental(
		context.Background(), domain.Snapshot{}, nil, domain.Snapshot{},
	)

	require.ErrorIs(t, err, assert.AnError)
	assert.Nil(t, bundle)
}

func TestIncrementalBundleFactory_AlsoImplementsBundleFactory(t *testing.T) {
	t.Parallel()

	expected := &stubBundle{}
	factory := &stubIncrementalBundleFactory{
		stubBundleFactory: stubBundleFactory{bundle: expected},
	}

	// Can call Build via the BundleFactory interface.
	bundle, err := factory.Build(context.Background(), domain.Snapshot{})

	require.NoError(t, err)
	assert.Same(t, expected, bundle)
}
