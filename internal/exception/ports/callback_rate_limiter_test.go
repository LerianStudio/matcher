//go:build unit

package ports

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// stubCallbackRateLimiter verifies the interface contract.
type stubCallbackRateLimiter struct {
	allowed bool
	err     error
}

func (s *stubCallbackRateLimiter) Allow(_ context.Context, _ string) (bool, error) {
	return s.allowed, s.err
}

func TestCallbackRateLimiter_InterfaceContract(t *testing.T) {
	t.Parallel()

	var limiter CallbackRateLimiter = &stubCallbackRateLimiter{allowed: true}

	allowed, err := limiter.Allow(context.Background(), "test-key")
	require.NoError(t, err)
	require.True(t, allowed)
}

func TestCallbackRateLimiter_Denied(t *testing.T) {
	t.Parallel()

	var limiter CallbackRateLimiter = &stubCallbackRateLimiter{allowed: false}

	allowed, err := limiter.Allow(context.Background(), "test-key")
	require.NoError(t, err)
	require.False(t, allowed)
}

func TestCallbackRateLimiter_Error(t *testing.T) {
	t.Parallel()

	errRateLimiter := errors.New("rate limiter unavailable")

	var limiter CallbackRateLimiter = &stubCallbackRateLimiter{err: errRateLimiter}

	allowed, err := limiter.Allow(context.Background(), "test-key")
	require.ErrorIs(t, err, errRateLimiter)
	require.False(t, allowed)
}
