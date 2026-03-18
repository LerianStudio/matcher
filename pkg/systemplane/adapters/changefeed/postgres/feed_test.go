// Copyright 2025 Lerian Studio.

//go:build unit

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

func TestParsePayload(t *testing.T) {
	t.Parallel()

	t.Run("valid global config payload", func(t *testing.T) {
		t.Parallel()

		data := `{"kind":"config","scope":"global","subject":"","revision":3}`

		signal, err := parsePayload(data)

		require.NoError(t, err)
		assert.Equal(t, domain.KindConfig, signal.Target.Kind)
		assert.Equal(t, domain.ScopeGlobal, signal.Target.Scope)
		assert.Equal(t, "", signal.Target.SubjectID)
		assert.Equal(t, domain.Revision(3), signal.Revision)
	})

	t.Run("valid tenant setting payload", func(t *testing.T) {
		t.Parallel()

		data := `{"kind":"setting","scope":"tenant","subject":"550e8400-e29b-41d4-a716-446655440000","revision":5}`

		signal, err := parsePayload(data)

		require.NoError(t, err)
		assert.Equal(t, domain.KindSetting, signal.Target.Kind)
		assert.Equal(t, domain.ScopeTenant, signal.Target.Scope)
		assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", signal.Target.SubjectID)
		assert.Equal(t, domain.Revision(5), signal.Revision)
	})

	t.Run("revision zero is valid", func(t *testing.T) {
		t.Parallel()

		data := `{"kind":"config","scope":"global","subject":"","revision":0}`

		signal, err := parsePayload(data)

		require.NoError(t, err)
		assert.Equal(t, domain.RevisionZero, signal.Revision)
	})

	t.Run("invalid JSON returns error", func(t *testing.T) {
		t.Parallel()

		_, err := parsePayload("not-json-at-all")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "pg changefeed unmarshal")
	})

	t.Run("empty string returns error", func(t *testing.T) {
		t.Parallel()

		_, err := parsePayload("")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "pg changefeed unmarshal")
	})

	t.Run("invalid kind returns error", func(t *testing.T) {
		t.Parallel()

		data := `{"kind":"bogus","scope":"global","subject":"","revision":1}`

		_, err := parsePayload(data)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "pg changefeed parse kind")
		assert.ErrorIs(t, err, domain.ErrInvalidKind)
	})

	t.Run("invalid scope returns error", func(t *testing.T) {
		t.Parallel()

		data := `{"kind":"config","scope":"bogus","subject":"","revision":1}`

		_, err := parsePayload(data)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "pg changefeed parse scope")
		assert.ErrorIs(t, err, domain.ErrInvalidScope)
	})

	t.Run("tenant scope without subject returns error", func(t *testing.T) {
		t.Parallel()

		data := `{"kind":"setting","scope":"tenant","subject":"","revision":1}`

		_, err := parsePayload(data)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "pg changefeed build target")
		assert.ErrorIs(t, err, domain.ErrScopeInvalid)
	})

	t.Run("global scope with subject returns error", func(t *testing.T) {
		t.Parallel()

		data := `{"kind":"config","scope":"global","subject":"some-id","revision":1}`

		_, err := parsePayload(data)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "pg changefeed build target")
		assert.ErrorIs(t, err, domain.ErrScopeInvalid)
	})

	t.Run("large revision value", func(t *testing.T) {
		t.Parallel()

		data := `{"kind":"config","scope":"global","subject":"","revision":18446744073709551615}`

		signal, err := parsePayload(data)

		require.NoError(t, err)
		assert.Equal(t, domain.Revision(18446744073709551615), signal.Revision)
	})
}

func TestBackoff(t *testing.T) {
	t.Parallel()

	t.Run("exponential growth", func(t *testing.T) {
		t.Parallel()

		feed := New("dsn", "ch")

		prev := time.Duration(0)

		for attempt := range 5 {
			d := feed.backoff(attempt)

			// The base (without jitter) should grow: 1s, 2s, 4s, 8s, 16s.
			// With up to 25% jitter the actual value is in [base, base*1.25].
			// We just check that each attempt is >= the previous base.
			baseNanos := float64(feed.reconnectMin) * float64(uint(1)<<attempt)
			assert.GreaterOrEqual(t, float64(d), baseNanos,
				"attempt %d: duration %v should be >= base %v", attempt, d, time.Duration(int64(baseNanos)))

			if attempt > 0 {
				assert.Greater(t, float64(d), float64(prev)*0.8,
					"attempt %d should generally grow", attempt)
			}

			prev = d
		}
	})

	t.Run("capped at max", func(t *testing.T) {
		t.Parallel()

		maxDelay := 5 * time.Second
		feed := New("dsn", "ch", WithReconnectBounds(1*time.Second, maxDelay))

		// At attempt 10, base would be 1s * 2^10 = 1024s, well above 5s cap.
		for range 20 {
			d := feed.backoff(10)

			// With 25% jitter on a 5s cap, max possible is 6.25s.
			maxWithJitter := time.Duration(float64(maxDelay) * 1.25)
			assert.LessOrEqual(t, d, maxWithJitter,
				"backoff %v should not exceed %v (max + 25%% jitter)", d, maxWithJitter)
			assert.GreaterOrEqual(t, d, maxDelay,
				"backoff %v should be >= max %v (jitter is additive)", d, maxDelay)
		}
	})

	t.Run("includes jitter within range", func(t *testing.T) {
		t.Parallel()

		feed := New("dsn", "ch", WithReconnectBounds(100*time.Millisecond, 10*time.Second))

		// At attempt 0, base = 100ms. Range should be [100ms, 125ms].
		minExpected := 100 * time.Millisecond
		maxExpected := 125 * time.Millisecond

		for range 50 {
			d := feed.backoff(0)
			assert.GreaterOrEqual(t, d, minExpected,
				"backoff %v should be >= %v", d, minExpected)
			assert.LessOrEqual(t, d, maxExpected,
				"backoff %v should be <= %v", d, maxExpected)
		}
	})

	t.Run("attempt zero uses reconnectMin as base", func(t *testing.T) {
		t.Parallel()

		feed := New("dsn", "ch", WithReconnectBounds(500*time.Millisecond, 1*time.Minute))

		d := feed.backoff(0)

		// Base is 500ms, max with jitter is 625ms.
		assert.GreaterOrEqual(t, d, 500*time.Millisecond)
		assert.LessOrEqual(t, d, 625*time.Millisecond)
	})
}

func TestNew(t *testing.T) {
	t.Parallel()

	t.Run("default options", func(t *testing.T) {
		t.Parallel()

		feed := New("postgres://localhost/test", "config_changes")

		assert.Equal(t, "postgres://localhost/test", feed.dsn)
		assert.Equal(t, "config_changes", feed.channel)
		assert.Equal(t, 1*time.Second, feed.reconnectMin)
		assert.Equal(t, 30*time.Second, feed.reconnectMax)
	})

	t.Run("with reconnect bounds", func(t *testing.T) {
		t.Parallel()

		feed := New("dsn", "ch",
			WithReconnectBounds(2*time.Second, 60*time.Second),
		)

		assert.Equal(t, 2*time.Second, feed.reconnectMin)
		assert.Equal(t, 60*time.Second, feed.reconnectMax)
	})

	t.Run("nil option ignored", func(t *testing.T) {
		t.Parallel()

		feed := New("dsn", "ch", nil, WithReconnectBounds(2*time.Second, 60*time.Second))

		assert.Equal(t, 2*time.Second, feed.reconnectMin)
		assert.Equal(t, 60*time.Second, feed.reconnectMax)
	})

	t.Run("zero min ignored", func(t *testing.T) {
		t.Parallel()

		feed := New("dsn", "ch",
			WithReconnectBounds(0, 60*time.Second),
		)

		assert.Equal(t, 1*time.Second, feed.reconnectMin)
		assert.Equal(t, 60*time.Second, feed.reconnectMax)
	})

	t.Run("zero max ignored", func(t *testing.T) {
		t.Parallel()

		feed := New("dsn", "ch",
			WithReconnectBounds(2*time.Second, 0),
		)

		assert.Equal(t, 2*time.Second, feed.reconnectMin)
		assert.Equal(t, 30*time.Second, feed.reconnectMax)
	})

	t.Run("negative values ignored", func(t *testing.T) {
		t.Parallel()

		feed := New("dsn", "ch",
			WithReconnectBounds(-1*time.Second, -5*time.Second),
		)

		assert.Equal(t, 1*time.Second, feed.reconnectMin)
		assert.Equal(t, 30*time.Second, feed.reconnectMax)
	})
}

func TestFeed_InterfaceCompliance(t *testing.T) {
	t.Parallel()

	// Compile-time check is at package level; this test documents intent.
	var _ ports.ChangeFeed = (*Feed)(nil)
}

func TestSubscribe_ValidationErrors(t *testing.T) {
	t.Parallel()

	t.Run("nil receiver", func(t *testing.T) {
		t.Parallel()

		var feed *Feed
		err := feed.Subscribe(context.Background(), func(ports.ChangeSignal) {})
		require.ErrorIs(t, err, ErrNilFeed)
	})

	t.Run("empty dsn", func(t *testing.T) {
		t.Parallel()

		feed := New("", "systemplane_changes")
		err := feed.Subscribe(context.Background(), func(ports.ChangeSignal) {})
		require.ErrorIs(t, err, ErrEmptyDSN)
	})

	t.Run("empty channel", func(t *testing.T) {
		t.Parallel()

		feed := New("postgres://localhost/db", "")
		err := feed.Subscribe(context.Background(), func(ports.ChangeSignal) {})
		require.ErrorIs(t, err, ErrEmptyChannel)
	})

	t.Run("invalid channel", func(t *testing.T) {
		t.Parallel()

		feed := New("postgres://localhost/db", `changes;DROP TABLE users`)
		err := feed.Subscribe(context.Background(), func(ports.ChangeSignal) {})
		require.ErrorIs(t, err, ErrInvalidChannel)
	})

	t.Run("nil handler", func(t *testing.T) {
		t.Parallel()

		feed := New("postgres://localhost/db", "systemplane_changes")
		err := feed.Subscribe(context.Background(), nil)
		require.ErrorIs(t, err, ErrNilFeedHandler)
	})
}
