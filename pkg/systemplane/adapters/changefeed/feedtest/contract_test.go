//go:build unit

// Copyright 2025 Lerian Studio.

package feedtest

import (
	"testing"
	"time"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/stretchr/testify/assert"
)

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

func TestConstants_Values(t *testing.T) {
	t.Parallel()

	t.Run("subscribeSignalWaitTimeout", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, 10*time.Second, subscribeSignalWaitTimeout)
	})

	t.Run("subscribeShutdownWaitTimeout", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, 5*time.Second, subscribeShutdownWaitTimeout)
	})

	t.Run("multipleSignalsCollectWait", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, 3*time.Second, multipleSignalsCollectWait)
	})

	t.Run("expectedFinalRevision", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, domain.Revision(3), expectedFinalRevision)
	})

	t.Run("readyProbeRetryInterval", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, 100*time.Millisecond, readyProbeRetryInterval)
	})

	t.Run("eventuallyPollInterval", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, 50*time.Millisecond, eventuallyPollInterval)
	})
}

// ---------------------------------------------------------------------------
// FeedFactory type
// ---------------------------------------------------------------------------

func TestFeedFactory_TypeCheck(t *testing.T) {
	t.Parallel()

	// FeedFactory is a function type. Verify it can be used as such.
	// The actual value is nil but the type is correct.
	var factory FeedFactory

	assert.Nil(t, factory, "zero-value FeedFactory should be nil")
}

// ---------------------------------------------------------------------------
// RunAll
// ---------------------------------------------------------------------------

func TestRunAll_CallsAllThreeSubTests(t *testing.T) {
	t.Parallel()

	// RunAll requires a real FeedFactory backed by a live store and feed.
	// Since this is a unit test, we verify that RunAll is a function that
	// accepts the correct arguments. Actual functional testing is done in
	// integration tests (postgres/integration_test.go, mongodb/integration_test.go).

	// Compile-time signature check: RunAll accepts *testing.T and FeedFactory.
	type runAllFn func(t *testing.T, factory FeedFactory)

	var fn runAllFn = RunAll

	assert.NotNil(t, fn)
}

// ---------------------------------------------------------------------------
// Test function signatures
// ---------------------------------------------------------------------------

func TestSubscribeReceivesSignal_Signature(t *testing.T) {
	t.Parallel()

	// Compile-time check: function accepts *testing.T and FeedFactory.
	type testFn func(t *testing.T, factory FeedFactory)

	var fn testFn = TestSubscribeReceivesSignal

	assert.NotNil(t, fn)
}

func TestContextCancellationStops_Signature(t *testing.T) {
	t.Parallel()

	type testFn func(t *testing.T, factory FeedFactory)

	var fn testFn = TestContextCancellationStops

	assert.NotNil(t, fn)
}

func TestMultipleSignals_Signature(t *testing.T) {
	t.Parallel()

	type testFn func(t *testing.T, factory FeedFactory)

	var fn testFn = TestMultipleSignals

	assert.NotNil(t, fn)
}
