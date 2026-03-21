// Copyright 2025 Lerian Studio.

//go:build unit

package changefeed

import (
	"testing"
	"time"

	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/stretchr/testify/assert"
)

func TestDebouncedFeedImplementsChangeFeedInterface(t *testing.T) {
	t.Parallel()

	var _ ports.ChangeFeed = (*DebouncedFeed)(nil)
}

func TestNewDebouncedFeed_Defaults(t *testing.T) {
	t.Parallel()

	inner := &fakeFeed{}
	df := NewDebouncedFeed(inner)

	assert.Equal(t, defaultWindow, df.window)
	assert.Equal(t, defaultJitter, df.jitter)
	assert.Equal(t, inner, df.inner)
}

func TestNewDebouncedFeed_NilOptionIgnored(t *testing.T) {
	t.Parallel()

	df := NewDebouncedFeed(&fakeFeed{}, nil, WithWindow(250*time.Millisecond))

	assert.Equal(t, 250*time.Millisecond, df.window)
	assert.Equal(t, defaultJitter, df.jitter)
}

func TestWithWindow(t *testing.T) {
	t.Parallel()

	inner := &fakeFeed{}
	df := NewDebouncedFeed(inner, WithWindow(500*time.Millisecond))

	assert.Equal(t, 500*time.Millisecond, df.window)
	assert.Equal(t, defaultJitter, df.jitter, "jitter should remain at default")
}

func TestWithWindow_Zero_Ignored(t *testing.T) {
	t.Parallel()

	df := NewDebouncedFeed(&fakeFeed{}, WithWindow(0))

	assert.Equal(t, defaultWindow, df.window, "zero window should be ignored")
}

func TestWithWindow_Negative_Ignored(t *testing.T) {
	t.Parallel()

	df := NewDebouncedFeed(&fakeFeed{}, WithWindow(-10*time.Millisecond))

	assert.Equal(t, defaultWindow, df.window, "negative window should be ignored")
}

func TestWithJitter(t *testing.T) {
	t.Parallel()

	inner := &fakeFeed{}
	df := NewDebouncedFeed(inner, WithJitter(200*time.Millisecond))

	assert.Equal(t, 200*time.Millisecond, df.jitter)
	assert.Equal(t, defaultWindow, df.window, "window should remain at default")
}

func TestWithJitter_Zero_DisablesJitter(t *testing.T) {
	t.Parallel()

	df := NewDebouncedFeed(&fakeFeed{}, WithJitter(0))

	assert.Equal(t, time.Duration(0), df.jitter)
}

func TestWithJitter_Negative_Ignored(t *testing.T) {
	t.Parallel()

	df := NewDebouncedFeed(&fakeFeed{}, WithJitter(-5*time.Millisecond))

	assert.Equal(t, defaultJitter, df.jitter, "negative jitter should be ignored")
}

func TestMultipleOptions(t *testing.T) {
	t.Parallel()

	df := NewDebouncedFeed(&fakeFeed{},
		WithWindow(300*time.Millisecond),
		WithJitter(0),
	)

	assert.Equal(t, 300*time.Millisecond, df.window)
	assert.Equal(t, time.Duration(0), df.jitter)
}

func TestDebounceDuration_NoJitter(t *testing.T) {
	t.Parallel()

	df := &DebouncedFeed{window: 100 * time.Millisecond, jitter: 0}

	for range 10 {
		assert.Equal(t, 100*time.Millisecond, df.debounceDuration())
	}
}

func TestDebounceDuration_WithJitter(t *testing.T) {
	t.Parallel()

	df := &DebouncedFeed{window: 100 * time.Millisecond, jitter: 50 * time.Millisecond}

	for range 50 {
		duration := df.debounceDuration()
		assert.GreaterOrEqual(t, duration, 100*time.Millisecond)
		assert.Less(t, duration, 150*time.Millisecond)
	}
}
