// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package worker

// BridgeRetryBackoff caps the number of attempts before the bridge worker
// upgrades a transient failure to terminal (max_attempts_exceeded).
//
// Backoff strategy: passive. The worker does NOT sleep between retries.
// Instead, every attempt bumps extraction_requests.updated_at and
// FindEligibleForBridge orders by updated_at ASC, so a row that just failed
// migrates to the tail of the eligibility queue and naturally waits for newer
// rows to drain before being re-picked. The tick cadence (Interval) IS the
// retry cadence; MaxAttempts caps total retries before terminal escalation.
//
// Polish Fix 2: the prior exponential-backoff helpers (ComputeDelay,
// ApplyJitter, InitialBackoff/MaxBackoff fields, defaults, systemplane keys)
// were inert — no caller invoked them. Operators tuning
// fetcher.bridge_retry_initial_backoff_sec or
// fetcher.bridge_retry_max_backoff_sec saw audit log churn but no behavioral
// change. Deleting the dead surface area is the honest move; passive backoff
// via updated_at reordering is the real (and elegant) strategy.
type BridgeRetryBackoff struct {
	// MaxAttempts is the absolute ceiling on retries before the worker
	// upgrades the failure to terminal (max_attempts_exceeded). Floors at
	// 1 to prevent silent disabling.
	MaxAttempts int
}

// defaultMaxAttempts matches the systemplane default for
// fetcher.bridge_retry_max_attempts.
const defaultMaxAttempts = 5

// Normalize fills in safe defaults for any zero-valued field. Called by the
// worker before storing the config so downstream callers never have to
// re-validate.
func (b BridgeRetryBackoff) Normalize() BridgeRetryBackoff {
	if b.MaxAttempts < 1 {
		b.MaxAttempts = defaultMaxAttempts
	}

	return b
}

// ShouldEscalate reports whether the given attempt count has reached the
// configured ceiling. Caller compares attempts AFTER recording the current
// attempt — so a config with MaxAttempts=5 escalates on the 5th attempt's
// failure, not the 6th.
func (b BridgeRetryBackoff) ShouldEscalate(attempts int) bool {
	return attempts >= b.MaxAttempts
}
