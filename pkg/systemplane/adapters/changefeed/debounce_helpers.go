// Copyright 2025 Lerian Studio.

package changefeed

import (
	"crypto/rand"
	"math/big"
	"sort"
	"time"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

// debounceEntry tracks the latest signal and its pending timer for a single
// target.
type debounceEntry struct {
	signal ports.ChangeSignal
	dueAt  time.Time
}

// debounceState holds the mutable state for a single Subscribe loop,
// allowing the main loop logic to be decomposed into smaller functions.
type debounceState struct {
	pending map[string]*debounceEntry
	timer   *time.Timer
	timerCh <-chan time.Time
}

// stopTimer cancels any running debounce timer and clears the channel.
func (ds *debounceState) stopTimer() {
	if ds.timer == nil {
		return
	}

	if !ds.timer.Stop() {
		select {
		case <-ds.timer.C:
		default:
		}
	}

	ds.timer = nil
	ds.timerCh = nil
}

// resetTimer recalculates the next fire time from pending entries and
// resets (or creates) the timer accordingly.
func (ds *debounceState) resetTimer() {
	if len(ds.pending) == 0 {
		ds.stopTimer()
		return
	}

	var nextDueAt time.Time

	for _, entry := range ds.pending {
		if nextDueAt.IsZero() || entry.dueAt.Before(nextDueAt) {
			nextDueAt = entry.dueAt
		}
	}

	delay := time.Until(nextDueAt)
	if delay < 0 {
		delay = 0
	}

	if ds.timer == nil {
		ds.timer = time.NewTimer(delay)
	} else {
		if !ds.timer.Stop() {
			select {
			case <-ds.timer.C:
			default:
			}
		}

		ds.timer.Reset(delay)
	}

	ds.timerCh = ds.timer.C
}

// collectSignals removes due (or all, if flushAll) entries from pending and
// returns them sorted by key for deterministic delivery.
func (ds *debounceState) collectSignals(now time.Time, flushAll bool) []ports.ChangeSignal {
	keys := make([]string, 0, len(ds.pending))

	for key, entry := range ds.pending {
		if flushAll || !entry.dueAt.After(now) {
			keys = append(keys, key)
		}
	}

	sort.Strings(keys)

	signals := make([]ports.ChangeSignal, 0, len(keys))

	for _, key := range keys {
		signals = append(signals, ds.pending[key].signal)
		delete(ds.pending, key)
	}

	return signals
}

// debounceDuration returns the debounce window plus a random jitter.
func (df *DebouncedFeed) debounceDuration() time.Duration {
	if df.jitter <= 0 {
		return df.window
	}

	jitterValue, err := rand.Int(rand.Reader, big.NewInt(df.jitter.Nanoseconds()))
	if err != nil {
		return df.window
	}

	jitter := time.Duration(jitterValue.Int64())

	return df.window + jitter
}

func isNilChangeFeed(feed ports.ChangeFeed) bool {
	return domain.IsNilValue(feed)
}
