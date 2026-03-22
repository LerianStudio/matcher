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

// stubChangeFeed is a minimal test double for ChangeFeed.
type stubChangeFeed struct {
	signals []ChangeSignal
	err     error
}

func (f *stubChangeFeed) Subscribe(ctx context.Context, handler func(ChangeSignal)) error {
	for _, sig := range f.signals {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			handler(sig)
		}
	}

	return f.err
}

// Compile-time interface check.
var _ ChangeFeed = (*stubChangeFeed)(nil)

func TestChangeSignal_ZeroValue(t *testing.T) {
	t.Parallel()

	var sig ChangeSignal

	assert.Equal(t, domain.Target{}, sig.Target)
	assert.Equal(t, domain.RevisionZero, sig.Revision)
	assert.Empty(t, string(sig.ApplyBehavior))
}

func TestChangeSignal_FieldAssignment(t *testing.T) {
	t.Parallel()

	sig := ChangeSignal{
		Target: domain.Target{
			Kind:      domain.KindConfig,
			Scope:     domain.ScopeGlobal,
			SubjectID: "",
		},
		Revision:      domain.Revision(7),
		ApplyBehavior: domain.ApplyBundleRebuild,
	}

	assert.Equal(t, domain.KindConfig, sig.Target.Kind)
	assert.Equal(t, domain.ScopeGlobal, sig.Target.Scope)
	assert.Equal(t, domain.Revision(7), sig.Revision)
	assert.Equal(t, domain.ApplyBundleRebuild, sig.ApplyBehavior)
}

func TestChangeFeed_CompileCheck(t *testing.T) {
	t.Parallel()

	var feed ChangeFeed = &stubChangeFeed{}
	require.NotNil(t, feed)
}

func TestChangeFeed_Subscribe_DeliversSignals(t *testing.T) {
	t.Parallel()

	signals := []ChangeSignal{
		{Revision: domain.Revision(1)},
		{Revision: domain.Revision(2)},
	}
	feed := &stubChangeFeed{signals: signals}

	var received []ChangeSignal

	err := feed.Subscribe(context.Background(), func(sig ChangeSignal) {
		received = append(received, sig)
	})

	require.NoError(t, err)
	assert.Len(t, received, 2)
	assert.Equal(t, domain.Revision(1), received[0].Revision)
	assert.Equal(t, domain.Revision(2), received[1].Revision)
}

func TestChangeFeed_Subscribe_ContextCancellation(t *testing.T) {
	t.Parallel()

	// Create many signals so that cancellation is hit.
	signals := make([]ChangeSignal, 100)
	for i := range signals {
		signals[i] = ChangeSignal{Revision: domain.Revision(i)}
	}

	feed := &stubChangeFeed{signals: signals}
	ctx, cancel := context.WithCancel(context.Background())

	var count int

	err := feed.Subscribe(ctx, func(_ ChangeSignal) {
		count++
		if count >= 3 {
			cancel()
		}
	})

	// Should have stopped early due to cancellation.
	// The exact count depends on timing, but it should be <= total signals.
	assert.True(t, count >= 3)
	// Error is either nil (all delivered before cancel checked) or context.Canceled.
	if err != nil {
		assert.ErrorIs(t, err, context.Canceled)
	}
}

func TestChangeFeed_Subscribe_ReturnsError(t *testing.T) {
	t.Parallel()

	feed := &stubChangeFeed{err: assert.AnError}

	err := feed.Subscribe(context.Background(), func(_ ChangeSignal) {})

	require.ErrorIs(t, err, assert.AnError)
}
