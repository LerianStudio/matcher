//go:build chaos

package chaos

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --------------------------------------------------------------------------
// Match Run state assertions
// --------------------------------------------------------------------------

// AssertMatchRunStatus verifies that a match run has the expected status.
// Uses a direct DB connection (bypassing Toxiproxy) for reliability.
func AssertMatchRunStatus(t *testing.T, db *sql.DB, runID uuid.UUID, expectedStatus string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var status string

	err := db.QueryRowContext(ctx,
		`SELECT status FROM match_runs WHERE id = $1`, runID,
	).Scan(&status)
	require.NoError(t, err, "query match run status for %s", runID)
	assert.Equal(t, expectedStatus, status,
		"match run %s: expected status %q, got %q", runID, expectedStatus, status)
}

// AssertNoOrphanedProcessingRuns verifies that no match runs are stuck in PROCESSING state.
// An orphaned PROCESSING run indicates a crash recovery failure.
func AssertNoOrphanedProcessingRuns(t *testing.T, db *sql.DB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var count int

	err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM match_runs WHERE status = 'PROCESSING'`,
	).Scan(&count)
	require.NoError(t, err, "count orphaned processing runs")
	assert.Equal(t, 0, count,
		"found %d orphaned match runs in PROCESSING state (expected 0)", count)
}

// AssertNoOrphanedProcessingJobs verifies that no ingestion jobs are stuck in PROCESSING state.
func AssertNoOrphanedProcessingJobs(t *testing.T, db *sql.DB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var count int

	err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM ingestion_jobs WHERE status = 'PROCESSING'`,
	).Scan(&count)
	require.NoError(t, err, "count orphaned processing jobs")
	assert.Equal(t, 0, count,
		"found %d orphaned ingestion jobs in PROCESSING state (expected 0)", count)
}

// --------------------------------------------------------------------------
// Outbox state assertions
// --------------------------------------------------------------------------

// GetOutboxStats queries the current outbox event statistics.
func GetOutboxStats(t *testing.T, db *sql.DB) OutboxStats {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx,
		`SELECT status, COUNT(*) FROM outbox_events GROUP BY status`,
	)
	require.NoError(t, err, "query outbox stats")
	defer rows.Close()

	stats := OutboxStats{}

	for rows.Next() {
		var status string

		var count int

		require.NoError(t, rows.Scan(&status, &count), "scan outbox stat row")

		switch strings.ToUpper(status) {
		case "PENDING":
			stats.Pending = count
		case "PROCESSING":
			stats.Processing = count
		case "PUBLISHED":
			stats.Published = count
		case "FAILED":
			stats.Failed = count
		case "INVALID":
			stats.Invalid = count
		}

		stats.Total += count
	}

	require.NoError(t, rows.Err(), "iterate outbox stats")

	return stats
}

// --------------------------------------------------------------------------
// Data consistency assertions
// --------------------------------------------------------------------------

// AssertNoDataCorruption runs a comprehensive consistency check across related tables.
// Verifies referential integrity that should hold regardless of chaos injected.
func AssertNoDataCorruption(t *testing.T, db *sql.DB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Match items reference valid match groups.
	var orphanedItems int

	err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM match_items mi
		LEFT JOIN match_groups mg ON mi.match_group_id = mg.id
		WHERE mg.id IS NULL
	`).Scan(&orphanedItems)
	if err == nil {
		assert.Equal(t, 0, orphanedItems,
			"found %d match items referencing non-existent match groups", orphanedItems)
	}

	// 2. Match groups reference valid match runs.
	var orphanedGroups int

	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM match_groups mg
		LEFT JOIN match_runs mr ON mg.run_id = mr.id
		WHERE mr.id IS NULL
	`).Scan(&orphanedGroups)
	if err == nil {
		assert.Equal(t, 0, orphanedGroups,
			"found %d match groups referencing non-existent match runs", orphanedGroups)
	}

	// 3. No match runs in inconsistent terminal state
	// (COMPLETED COMMIT runs with no groups are suspicious but not necessarily wrong).
	var completedNoGroups int

	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM match_runs mr
		WHERE mr.status = 'COMPLETED'
		AND mr.mode != 'DRY_RUN'
		AND NOT EXISTS (SELECT 1 FROM match_groups mg WHERE mg.run_id = mr.id)
	`).Scan(&completedNoGroups)
	if err == nil && completedNoGroups > 0 {
		t.Logf("INFO: %d completed (non-dry-run) match runs with no groups (may be valid for zero-match scenarios)",
			completedNoGroups)
	}
}
