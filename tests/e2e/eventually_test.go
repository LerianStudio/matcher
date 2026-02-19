//go:build e2e

package e2e

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultPollOptions(t *testing.T) {
	cfg := &E2EConfig{
		PollInterval: 100 * time.Millisecond,
		PollTimeout:  5 * time.Second,
	}

	opts := DefaultPollOptions(cfg)

	assert.Equal(t, 100*time.Millisecond, opts.Interval)
	assert.Equal(t, 5*time.Second, opts.Timeout)
}

func TestEventually_SucceedsImmediately(t *testing.T) {
	ctx := context.Background()
	opts := PollOptions{
		Interval: 10 * time.Millisecond,
		Timeout:  1 * time.Second,
	}

	callCount := 0
	err := Eventually(ctx, opts, func() (bool, error) {
		callCount++
		return true, nil
	})

	require.NoError(t, err)
	assert.Equal(t, 1, callCount)
}

func TestEventually_SucceedsAfterRetries(t *testing.T) {
	ctx := context.Background()
	opts := PollOptions{
		Interval: 10 * time.Millisecond,
		Timeout:  1 * time.Second,
	}

	callCount := 0
	err := Eventually(ctx, opts, func() (bool, error) {
		callCount++
		if callCount >= 3 {
			return true, nil
		}
		return false, nil
	})

	require.NoError(t, err)
	assert.GreaterOrEqual(t, callCount, 3)
}

func TestEventually_RetriesOnError(t *testing.T) {
	ctx := context.Background()
	opts := PollOptions{
		Interval: 10 * time.Millisecond,
		Timeout:  1 * time.Second,
	}

	callCount := 0
	err := Eventually(ctx, opts, func() (bool, error) {
		callCount++
		if callCount < 3 {
			return false, errors.New("temporary error")
		}
		return true, nil
	})

	require.NoError(t, err)
	assert.GreaterOrEqual(t, callCount, 3)
}

func TestEventually_TimesOut(t *testing.T) {
	ctx := context.Background()
	opts := PollOptions{
		Interval: 10 * time.Millisecond,
		Timeout:  50 * time.Millisecond,
	}

	err := Eventually(ctx, opts, func() (bool, error) {
		return false, nil
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "timeout")
}

func TestEventually_TimeoutIncludesLastError(t *testing.T) {
	ctx := context.Background()
	opts := PollOptions{
		Interval: 10 * time.Millisecond,
		Timeout:  50 * time.Millisecond,
	}

	err := Eventually(ctx, opts, func() (bool, error) {
		return false, errors.New("persistent error")
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "timeout")
	assert.Contains(t, err.Error(), "persistent error")
}

func TestEventually_RespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	opts := PollOptions{
		Interval: 10 * time.Millisecond,
		Timeout:  5 * time.Second,
	}

	started := make(chan struct{})
	var once sync.Once

	go func() {
		<-started
		cancel()
	}()

	err := Eventually(ctx, opts, func() (bool, error) {
		once.Do(func() { close(started) })
		return false, nil
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "context cancel")
}

func TestEventually_CancelledWithLastError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	opts := PollOptions{
		Interval: 10 * time.Millisecond,
		Timeout:  5 * time.Second,
	}

	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	err := Eventually(ctx, opts, func() (bool, error) {
		return false, errors.New("error before cancel")
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "error before cancel")
}

func TestEventuallyWithResult_SucceedsImmediately(t *testing.T) {
	ctx := context.Background()
	opts := PollOptions{
		Interval: 10 * time.Millisecond,
		Timeout:  1 * time.Second,
	}

	expectedValue := "test-value"
	callCount := 0

	result, err := EventuallyWithResult(ctx, opts, func() (*string, error) {
		callCount++
		return &expectedValue, nil
	})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, expectedValue, *result)
	assert.Equal(t, 1, callCount)
}

func TestEventuallyWithResult_SucceedsAfterRetries(t *testing.T) {
	ctx := context.Background()
	opts := PollOptions{
		Interval: 10 * time.Millisecond,
		Timeout:  1 * time.Second,
	}

	callCount := 0
	expectedValue := 42

	result, err := EventuallyWithResult(ctx, opts, func() (*int, error) {
		callCount++
		if callCount >= 3 {
			return &expectedValue, nil
		}
		return nil, nil
	})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 42, *result)
	assert.GreaterOrEqual(t, callCount, 3)
}

func TestEventuallyWithResult_RetriesOnError(t *testing.T) {
	ctx := context.Background()
	opts := PollOptions{
		Interval: 10 * time.Millisecond,
		Timeout:  1 * time.Second,
	}

	callCount := 0
	expectedValue := "success"

	result, err := EventuallyWithResult(ctx, opts, func() (*string, error) {
		callCount++
		if callCount < 3 {
			return nil, errors.New("temporary error")
		}
		return &expectedValue, nil
	})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "success", *result)
}

func TestEventuallyWithResult_TimesOut(t *testing.T) {
	ctx := context.Background()
	opts := PollOptions{
		Interval: 10 * time.Millisecond,
		Timeout:  50 * time.Millisecond,
	}

	result, err := EventuallyWithResult(ctx, opts, func() (*string, error) {
		return nil, nil
	})

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "timeout")
}

func TestEventuallyWithResult_TimeoutIncludesLastError(t *testing.T) {
	ctx := context.Background()
	opts := PollOptions{
		Interval: 10 * time.Millisecond,
		Timeout:  50 * time.Millisecond,
	}

	result, err := EventuallyWithResult(ctx, opts, func() (*string, error) {
		return nil, errors.New("fetch failed")
	})

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "timeout")
	assert.Contains(t, err.Error(), "fetch failed")
}

func TestEventuallyWithResult_RespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	opts := PollOptions{
		Interval: 10 * time.Millisecond,
		Timeout:  5 * time.Second,
	}

	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	result, err := EventuallyWithResult(ctx, opts, func() (*int, error) {
		return nil, nil
	})

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "context cancel")
}

func TestEventuallyWithResult_WithStructType(t *testing.T) {
	type TestResult struct {
		ID    string
		Value int
	}

	ctx := context.Background()
	opts := PollOptions{
		Interval: 10 * time.Millisecond,
		Timeout:  1 * time.Second,
	}

	expected := TestResult{ID: "test-123", Value: 100}

	result, err := EventuallyWithResult(ctx, opts, func() (*TestResult, error) {
		return &expected, nil
	})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "test-123", result.ID)
	assert.Equal(t, 100, result.Value)
}

func TestPollOptions_ZeroValues(t *testing.T) {
	opts := PollOptions{}

	assert.Equal(t, time.Duration(0), opts.Interval)
	assert.Equal(t, time.Duration(0), opts.Timeout)
}
