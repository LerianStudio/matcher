// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"
	"sync"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
)

// startupTimer tracks the duration of each bootstrap phase.
// Thread-safe for use across parallel initialization phases.
type startupTimer struct {
	mu     sync.Mutex
	start  time.Time
	phases []phaseRecord
}

// phaseRecord captures a named bootstrap phase and its duration.
type phaseRecord struct {
	name     string
	duration time.Duration
}

// newStartupTimer creates a timer anchored to the current instant.
func newStartupTimer() *startupTimer {
	return &startupTimer{
		start: time.Now().UTC(),
	}
}

// track begins timing a named phase. Call the returned function to record
// the elapsed duration. Safe for concurrent use — multiple goroutines can
// track different phases simultaneously.
//
// Usage:
//
//	done := timer.track("postgres")
//	defer done()
func (st *startupTimer) track(name string) func() {
	if st == nil {
		return func() {}
	}

	phaseStart := time.Now().UTC()

	return func() {
		elapsed := time.Since(phaseStart)

		st.mu.Lock()
		defer st.mu.Unlock()

		st.phases = append(st.phases, phaseRecord{
			name:     name,
			duration: elapsed,
		})
	}
}

// elapsed returns the wall-clock time since the timer was created.
func (st *startupTimer) elapsed() time.Duration {
	return time.Since(st.start)
}

// logStartupTiming outputs a structured timing breakdown of bootstrap phases.
// Appears in the startup banner right before the "ready" line.
func logStartupTiming(logger libLog.Logger, timer *startupTimer) {
	if logger == nil || timer == nil {
		return
	}

	ctx := context.Background()
	level := libLog.LevelDebug

	timer.mu.Lock()
	phases := make([]phaseRecord, len(timer.phases))
	copy(phases, timer.phases)
	timer.mu.Unlock()

	logger.Log(ctx, level, "")
	logger.Log(ctx, level, "  STARTUP TIMING")
	logger.Log(ctx, level, "--------------------------------------------------------------")

	for _, p := range phases {
		label := fmt.Sprintf("  %-22s: %s", p.name, formatDuration(p.duration))
		logger.Log(ctx, level, label)
	}

	logger.Log(ctx, level, "--------------------------------------------------------------")
	logger.Log(ctx, level, fmt.Sprintf("  %-22s: %s", "TOTAL", formatDuration(timer.elapsed())))
	logger.Log(ctx, level, "--------------------------------------------------------------")
}

// formatDuration produces a human-readable duration string.
// Uses milliseconds for sub-second durations, seconds for the rest.
func formatDuration(duration time.Duration) string {
	switch {
	case duration < time.Millisecond:
		if duration < time.Microsecond {
			return fmt.Sprintf("%dns", duration.Nanoseconds())
		}

		return fmt.Sprintf("%dus", duration.Microseconds())
	case duration < time.Second:
		return fmt.Sprintf("%dms", duration.Milliseconds())
	default:
		return fmt.Sprintf("%.2fs", duration.Seconds())
	}
}
