// Copyright 2025 Lerian Studio.

//go:build unit

package changefeed

import (
	"context"
	"testing"

	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/stretchr/testify/require"
)

func TestSubscribe_NilReceiver_ReturnsError(t *testing.T) {
	t.Parallel()

	var df *DebouncedFeed

	err := df.Subscribe(context.Background(), func(_ ports.ChangeSignal) {})

	require.ErrorIs(t, err, ErrNilDebouncedFeed)
}

func TestSubscribe_NilInnerFeed_ReturnsError(t *testing.T) {
	t.Parallel()

	df := &DebouncedFeed{window: defaultWindow}

	err := df.Subscribe(context.Background(), func(_ ports.ChangeSignal) {})

	require.ErrorIs(t, err, ErrNilInnerFeed)
}

func TestSubscribe_TypedNilInnerFeed_ReturnsError(t *testing.T) {
	t.Parallel()

	var inner ports.ChangeFeed = (*fakeFeed)(nil)
	df := NewDebouncedFeed(inner)

	err := df.Subscribe(context.Background(), func(_ ports.ChangeSignal) {})

	require.ErrorIs(t, err, ErrNilInnerFeed)
}

func TestSubscribe_NilHandler_ReturnsError(t *testing.T) {
	t.Parallel()

	df := NewDebouncedFeed(&fakeFeed{})

	err := df.Subscribe(context.Background(), nil)

	require.ErrorIs(t, err, ErrNilHandler)
}
