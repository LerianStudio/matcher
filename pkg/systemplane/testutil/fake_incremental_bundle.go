// Copyright 2025 Lerian Studio.

package testutil

import (
	"context"
	"fmt"
	"sync"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

// Compile-time interface checks.
var (
	_ ports.BundleFactory            = (*FakeIncrementalBundleFactory)(nil)
	_ ports.IncrementalBundleFactory = (*FakeIncrementalBundleFactory)(nil)
)

// FakeIncrementalBundleFactory implements both BundleFactory and
// IncrementalBundleFactory. It embeds FakeBundleFactory for full-build
// behavior and adds configurable IncrementalBuildFunc for incremental builds.
type FakeIncrementalBundleFactory struct {
	FakeBundleFactory // embed existing full-build factory

	mu                   sync.Mutex
	IncrementalBuildFunc func(ctx context.Context, snap domain.Snapshot,
		previous domain.RuntimeBundle, prevSnap domain.Snapshot) (domain.RuntimeBundle, error)
	IncrementalCalls int // number of BuildIncremental invocations
}

// NewFakeIncrementalBundleFactory creates a factory that returns a fresh
// FakeBundle for both Build and BuildIncremental calls.
func NewFakeIncrementalBundleFactory() *FakeIncrementalBundleFactory {
	return &FakeIncrementalBundleFactory{
		FakeBundleFactory: *NewFakeBundleFactory(),
		IncrementalBuildFunc: func(_ context.Context, _ domain.Snapshot,
			_ domain.RuntimeBundle, _ domain.Snapshot,
		) (domain.RuntimeBundle, error) {
			return &FakeBundle{}, nil
		},
	}
}

// BuildIncremental delegates to IncrementalBuildFunc if set, otherwise returns
// an error.
func (f *FakeIncrementalBundleFactory) BuildIncremental(
	ctx context.Context,
	snap domain.Snapshot,
	previous domain.RuntimeBundle,
	prevSnap domain.Snapshot,
) (domain.RuntimeBundle, error) {
	f.mu.Lock()
	f.IncrementalCalls++
	fn := f.IncrementalBuildFunc
	f.mu.Unlock()

	if fn != nil {
		return fn(ctx, snap, previous, prevSnap)
	}

	return nil, fmt.Errorf("incremental build not configured")
}

// IncrementalCallCount returns the number of BuildIncremental invocations.
func (f *FakeIncrementalBundleFactory) IncrementalCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.IncrementalCalls
}
