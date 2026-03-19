// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/service"
)

// Compile-time check that mockSupervisor satisfies the Supervisor interface.
var _ service.Supervisor = (*mockSupervisor)(nil)

// mockSupervisor is a test double that returns a controllable snapshot.
// The snapshot field can be mutated between calls to verify that SnapshotReader
// always reads the latest value (live-read behavior).
type mockSupervisor struct {
	snapshot domain.Snapshot
}

func (m *mockSupervisor) Current() domain.RuntimeBundle { return nil }
func (m *mockSupervisor) Snapshot() domain.Snapshot     { return m.snapshot }
func (m *mockSupervisor) PublishSnapshot(_ context.Context, _ domain.Snapshot, _ string) error {
	return nil
}

func (m *mockSupervisor) ReconcileCurrent(_ context.Context, _ domain.Snapshot, _ string) error {
	return nil
}
func (m *mockSupervisor) Reload(_ context.Context, _ string) error { return nil }
func (m *mockSupervisor) Stop(_ context.Context) error             { return nil }

// newMockSnapshot creates a domain.Snapshot with the given key-value pairs
// as effective configuration entries.
func newMockSnapshot(configs map[string]any) domain.Snapshot {
	effective := make(map[string]domain.EffectiveValue, len(configs))
	for key, val := range configs {
		effective[key] = domain.EffectiveValue{
			Key:   key,
			Value: val,
		}
	}

	return domain.Snapshot{Configs: effective}
}

// ---------------------------------------------------------------------------
// Constructor tests
// ---------------------------------------------------------------------------

func TestNewSnapshotReader_NilSupervisor(t *testing.T) {
	t.Parallel()

	reader, err := NewSnapshotReader(nil)

	require.Error(t, err)
	assert.Nil(t, reader)
	assert.ErrorIs(t, err, errSnapshotReaderSupervisorRequired)
}

func TestNewSnapshotReader_Success(t *testing.T) {
	t.Parallel()

	sup := &mockSupervisor{}

	reader, err := NewSnapshotReader(sup)

	require.NoError(t, err)
	require.NotNil(t, reader)
}

// ---------------------------------------------------------------------------
// Rate Limit accessor tests
// ---------------------------------------------------------------------------

func TestSnapshotReader_RateLimitEnabled_Default(t *testing.T) {
	t.Parallel()

	sup := &mockSupervisor{snapshot: domain.Snapshot{}}

	reader, err := NewSnapshotReader(sup)
	require.NoError(t, err)

	assert.True(t, reader.RateLimitEnabled())
}

func TestSnapshotReader_RateLimitEnabled_Override(t *testing.T) {
	t.Parallel()

	sup := &mockSupervisor{
		snapshot: newMockSnapshot(map[string]any{
			"rate_limit.enabled": false,
		}),
	}

	reader, err := NewSnapshotReader(sup)
	require.NoError(t, err)

	assert.False(t, reader.RateLimitEnabled())
}

func TestSnapshotReader_RateLimitMax_Default(t *testing.T) {
	t.Parallel()

	sup := &mockSupervisor{snapshot: domain.Snapshot{}}

	reader, err := NewSnapshotReader(sup)
	require.NoError(t, err)

	assert.Equal(t, 100, reader.RateLimitMax())
}

func TestSnapshotReader_RateLimitMax_Override(t *testing.T) {
	t.Parallel()

	sup := &mockSupervisor{
		snapshot: newMockSnapshot(map[string]any{
			"rate_limit.max": 500,
		}),
	}

	reader, err := NewSnapshotReader(sup)
	require.NoError(t, err)

	assert.Equal(t, 500, reader.RateLimitMax())
}

func TestSnapshotReader_RateLimitExpirySec_Default(t *testing.T) {
	t.Parallel()

	sup := &mockSupervisor{snapshot: domain.Snapshot{}}

	reader, err := NewSnapshotReader(sup)
	require.NoError(t, err)

	assert.Equal(t, 60, reader.RateLimitExpirySec())
}

func TestSnapshotReader_RateLimitExpirySec_Override(t *testing.T) {
	t.Parallel()

	sup := &mockSupervisor{
		snapshot: newMockSnapshot(map[string]any{
			"rate_limit.expiry_sec": 120,
		}),
	}

	reader, err := NewSnapshotReader(sup)
	require.NoError(t, err)

	assert.Equal(t, 120, reader.RateLimitExpirySec())
}

func TestSnapshotReader_ExportRateLimitMax_Default(t *testing.T) {
	t.Parallel()

	sup := &mockSupervisor{snapshot: domain.Snapshot{}}

	reader, err := NewSnapshotReader(sup)
	require.NoError(t, err)

	assert.Equal(t, 10, reader.ExportRateLimitMax())
}

func TestSnapshotReader_ExportRateLimitMax_Override(t *testing.T) {
	t.Parallel()

	sup := &mockSupervisor{
		snapshot: newMockSnapshot(map[string]any{
			"rate_limit.export_max": 25,
		}),
	}

	reader, err := NewSnapshotReader(sup)
	require.NoError(t, err)

	assert.Equal(t, 25, reader.ExportRateLimitMax())
}

func TestSnapshotReader_ExportRateLimitExpirySec_Default(t *testing.T) {
	t.Parallel()

	sup := &mockSupervisor{snapshot: domain.Snapshot{}}

	reader, err := NewSnapshotReader(sup)
	require.NoError(t, err)

	assert.Equal(t, 60, reader.ExportRateLimitExpirySec())
}

func TestSnapshotReader_DispatchRateLimitMax_Default(t *testing.T) {
	t.Parallel()

	sup := &mockSupervisor{snapshot: domain.Snapshot{}}

	reader, err := NewSnapshotReader(sup)
	require.NoError(t, err)

	assert.Equal(t, 50, reader.DispatchRateLimitMax())
}

func TestSnapshotReader_DispatchRateLimitExpirySec_Default(t *testing.T) {
	t.Parallel()

	sup := &mockSupervisor{snapshot: domain.Snapshot{}}

	reader, err := NewSnapshotReader(sup)
	require.NoError(t, err)

	assert.Equal(t, 60, reader.DispatchRateLimitExpirySec())
}

// ---------------------------------------------------------------------------
// Infrastructure accessor tests
// ---------------------------------------------------------------------------

func TestSnapshotReader_HealthCheckTimeoutSec_Default(t *testing.T) {
	t.Parallel()

	sup := &mockSupervisor{snapshot: domain.Snapshot{}}

	reader, err := NewSnapshotReader(sup)
	require.NoError(t, err)

	assert.Equal(t, 5, reader.HealthCheckTimeoutSec())
}

func TestSnapshotReader_HealthCheckTimeoutSec_Override(t *testing.T) {
	t.Parallel()

	sup := &mockSupervisor{
		snapshot: newMockSnapshot(map[string]any{
			"infrastructure.health_check_timeout_sec": 15,
		}),
	}

	reader, err := NewSnapshotReader(sup)
	require.NoError(t, err)

	assert.Equal(t, 15, reader.HealthCheckTimeoutSec())
}

// ---------------------------------------------------------------------------
// Callback Rate Limit accessor tests
// ---------------------------------------------------------------------------

func TestSnapshotReader_CallbackRateLimitPerMinute_Default(t *testing.T) {
	t.Parallel()

	sup := &mockSupervisor{snapshot: domain.Snapshot{}}

	reader, err := NewSnapshotReader(sup)
	require.NoError(t, err)

	assert.Equal(t, 60, reader.CallbackRateLimitPerMinute())
}

func TestSnapshotReader_CallbackRateLimitPerMinute_Override(t *testing.T) {
	t.Parallel()

	sup := &mockSupervisor{
		snapshot: newMockSnapshot(map[string]any{
			"callback_rate_limit.per_minute": 200,
		}),
	}

	reader, err := NewSnapshotReader(sup)
	require.NoError(t, err)

	assert.Equal(t, 200, reader.CallbackRateLimitPerMinute())
}

// ---------------------------------------------------------------------------
// Export/Archive Presign accessor tests
// ---------------------------------------------------------------------------

func TestSnapshotReader_ExportPresignExpirySec_Default(t *testing.T) {
	t.Parallel()

	sup := &mockSupervisor{snapshot: domain.Snapshot{}}

	reader, err := NewSnapshotReader(sup)
	require.NoError(t, err)

	assert.Equal(t, 3600, reader.ExportPresignExpirySec())
}

func TestSnapshotReader_ExportPresignExpirySec_Override(t *testing.T) {
	t.Parallel()

	sup := &mockSupervisor{
		snapshot: newMockSnapshot(map[string]any{
			"export_worker.presign_expiry_sec": 7200,
		}),
	}

	reader, err := NewSnapshotReader(sup)
	require.NoError(t, err)

	assert.Equal(t, 7200, reader.ExportPresignExpirySec())
}

func TestSnapshotReader_ArchivalPresignExpirySec_Default(t *testing.T) {
	t.Parallel()

	sup := &mockSupervisor{snapshot: domain.Snapshot{}}

	reader, err := NewSnapshotReader(sup)
	require.NoError(t, err)

	assert.Equal(t, 3600, reader.ArchivalPresignExpirySec())
}

func TestSnapshotReader_ArchivalPresignExpirySec_Override(t *testing.T) {
	t.Parallel()

	sup := &mockSupervisor{
		snapshot: newMockSnapshot(map[string]any{
			"archival.presign_expiry_sec": 1800,
		}),
	}

	reader, err := NewSnapshotReader(sup)
	require.NoError(t, err)

	assert.Equal(t, 1800, reader.ArchivalPresignExpirySec())
}

// ---------------------------------------------------------------------------
// Live-read behavior test
// ---------------------------------------------------------------------------

func TestSnapshotReader_ReadsLatestSnapshot(t *testing.T) {
	t.Parallel()

	sup := &mockSupervisor{
		snapshot: newMockSnapshot(map[string]any{
			"rate_limit.max": 100,
		}),
	}

	reader, err := NewSnapshotReader(sup)
	require.NoError(t, err)

	// First read: snapshot has max=100.
	assert.Equal(t, 100, reader.RateLimitMax())

	// Mutate the supervisor's snapshot to simulate a live configuration update.
	sup.snapshot = newMockSnapshot(map[string]any{
		"rate_limit.max": 999,
	})

	// Second read: SnapshotReader must see the new value because it calls
	// supervisor.Snapshot() on every access, not caching any values.
	assert.Equal(t, 999, reader.RateLimitMax())

	// Third mutation: verify with a different accessor to confirm all methods
	// read live, not just RateLimitMax.
	sup.snapshot = newMockSnapshot(map[string]any{
		"rate_limit.max": 999,
		"infrastructure.health_check_timeout_sec": 30,
	})

	assert.Equal(t, 30, reader.HealthCheckTimeoutSec())
}
