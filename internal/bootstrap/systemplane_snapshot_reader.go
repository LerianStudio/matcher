// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"errors"
	"fmt"

	"github.com/LerianStudio/matcher/pkg/systemplane/service"
)

var errSnapshotReaderSupervisorRequired = errors.New("new snapshot reader: supervisor is required")

// SnapshotReader provides typed accessors for live-read configuration keys.
// Each method reads the current snapshot from the Supervisor on every call,
// ensuring callers always see the latest effective value without requiring
// a bundle rebuild or worker reconciliation.
//
// Live-read keys are classified as ApplyLiveRead in the phase-0 classification
// matrix. They include rate limit settings, health check timeouts, callback
// rate limits, and presigned URL expiry durations.
type SnapshotReader struct {
	supervisor service.Supervisor
}

// NewSnapshotReader creates a new SnapshotReader backed by the given Supervisor.
// The Supervisor must be non-nil; it is the source of all snapshot reads.
func NewSnapshotReader(supervisor service.Supervisor) (*SnapshotReader, error) {
	if supervisor == nil {
		return nil, fmt.Errorf("%w", errSnapshotReaderSupervisorRequired)
	}

	return &SnapshotReader{supervisor: supervisor}, nil
}

// Rate Limit accessors.

// RateLimitEnabled returns whether global rate limiting is enabled.
func (r *SnapshotReader) RateLimitEnabled() bool {
	return snapBool(r.supervisor.Snapshot(), "rate_limit.enabled", defaultRateLimitEnabled)
}

// RateLimitMax returns the maximum number of requests per window.
func (r *SnapshotReader) RateLimitMax() int {
	return snapInt(r.supervisor.Snapshot(), "rate_limit.max", defaultRateLimitMax)
}

// RateLimitExpirySec returns the rate limit window duration in seconds.
func (r *SnapshotReader) RateLimitExpirySec() int {
	return snapInt(r.supervisor.Snapshot(), "rate_limit.expiry_sec", defaultRateLimitExpirySec)
}

// ExportRateLimitMax returns the max export requests per window.
func (r *SnapshotReader) ExportRateLimitMax() int {
	return snapInt(r.supervisor.Snapshot(), "rate_limit.export_max", defaultRateLimitExportMax)
}

// ExportRateLimitExpirySec returns the export rate limit window in seconds.
func (r *SnapshotReader) ExportRateLimitExpirySec() int {
	return snapInt(r.supervisor.Snapshot(), "rate_limit.export_expiry_sec", defaultRateLimitExportExpiry)
}

// DispatchRateLimitMax returns the max dispatch requests per window.
func (r *SnapshotReader) DispatchRateLimitMax() int {
	return snapInt(r.supervisor.Snapshot(), "rate_limit.dispatch_max", defaultRateLimitDispatchMax)
}

// DispatchRateLimitExpirySec returns the dispatch rate limit window in seconds.
func (r *SnapshotReader) DispatchRateLimitExpirySec() int {
	return snapInt(r.supervisor.Snapshot(), "rate_limit.dispatch_expiry_sec", defaultRateLimitDispatchExp)
}

// Infrastructure accessors.

// HealthCheckTimeoutSec returns the health check timeout in seconds.
func (r *SnapshotReader) HealthCheckTimeoutSec() int {
	return snapInt(r.supervisor.Snapshot(), "infrastructure.health_check_timeout_sec", defaultInfraHealthCheckTimeout)
}

// Callback Rate Limit accessors.

// CallbackRateLimitPerMinute returns the callback rate limit per minute per system.
func (r *SnapshotReader) CallbackRateLimitPerMinute() int {
	return snapInt(r.supervisor.Snapshot(), "callback_rate_limit.per_minute", defaultCallbackPerMinute)
}

// Export and Archive Presign accessors.

// ExportPresignExpirySec returns the presigned URL expiry for export downloads.
func (r *SnapshotReader) ExportPresignExpirySec() int {
	return snapInt(r.supervisor.Snapshot(), "export_worker.presign_expiry_sec", defaultExportPresignExp)
}

// ArchivalPresignExpirySec returns the presigned URL expiry for archive downloads.
func (r *SnapshotReader) ArchivalPresignExpirySec() int {
	return snapInt(r.supervisor.Snapshot(), "archival.presign_expiry_sec", defaultArchivalPresignExpiry)
}
