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

// OutboxStats summarizes outbox event states for diagnostic reporting.
type OutboxStats struct {
	Pending    int
	Processing int
	Published  int
	Failed     int
	Invalid    int
	Total      int
}

// String provides a human-readable summary.
func (s OutboxStats) String() string {
	return fmt.Sprintf(
		"outbox[total=%d pending=%d processing=%d published=%d failed=%d invalid=%d]",
		s.Total, s.Pending, s.Processing, s.Published, s.Failed, s.Invalid,
	)
}

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

// AssertOutboxAllPublished verifies all outbox events have been published.
func AssertOutboxAllPublished(t *testing.T, db *sql.DB) {
	t.Helper()

	stats := GetOutboxStats(t, db)
	assert.Equal(t, 0, stats.Pending,
		"expected 0 pending outbox events, got %d (%s)", stats.Pending, stats)
	assert.Equal(t, 0, stats.Processing,
		"expected 0 processing outbox events, got %d (%s)", stats.Processing, stats)
	assert.Equal(t, 0, stats.Failed,
		"expected 0 failed outbox events, got %d (%s)", stats.Failed, stats)
}

// AssertOutboxEventuallyPublished waits for all outbox events to be published.
// Polls the database every interval until timeout or all events are published/failed permanently.
func AssertOutboxEventuallyPublished(t *testing.T, db *sql.DB, timeout, interval time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var lastStats OutboxStats

	for time.Now().Before(deadline) {
		lastStats = GetOutboxStats(t, db)

		remaining := lastStats.Pending + lastStats.Processing + lastStats.Failed
		if remaining == 0 {
			return // All events settled (published or invalid).
		}

		time.Sleep(interval)
	}

	t.Errorf("outbox events not fully published after %v: %s", timeout, lastStats)
}

// AssertOutboxHasPendingEvents verifies that there are pending outbox events.
// Useful after injecting a RabbitMQ failure to confirm events accumulated.
func AssertOutboxHasPendingEvents(t *testing.T, db *sql.DB, minCount int) {
	t.Helper()

	stats := GetOutboxStats(t, db)
	pendingOrFailed := stats.Pending + stats.Failed

	assert.GreaterOrEqual(t, pendingOrFailed, minCount,
		"expected at least %d pending/failed outbox events, got %d (%s)",
		minCount, pendingOrFailed, stats)
}

// --------------------------------------------------------------------------
// Transaction state assertions
// --------------------------------------------------------------------------

// AssertTransactionStatuses verifies that all given transactions have the expected status.
func AssertTransactionStatuses(t *testing.T, db *sql.DB, txIDs []uuid.UUID, expectedStatus string) {
	t.Helper()

	if len(txIDs) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	placeholders := make([]string, len(txIDs))
	args := make([]any, len(txIDs))

	for i, id := range txIDs {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}

	query := fmt.Sprintf(
		`SELECT id, status FROM transactions WHERE id IN (%s)`,
		strings.Join(placeholders, ", "),
	)

	rows, err := db.QueryContext(ctx, query, args...)
	require.NoError(t, err, "query transaction statuses")
	defer rows.Close()

	found := 0

	for rows.Next() {
		var id uuid.UUID

		var status string

		require.NoError(t, rows.Scan(&id, &status), "scan transaction status")
		assert.Equal(t, expectedStatus, status,
			"transaction %s: expected status %q, got %q", id, expectedStatus, status)

		found++
	}

	require.NoError(t, rows.Err(), "iterate transaction statuses")
	assert.Equal(t, len(txIDs), found,
		"expected %d transactions, found %d", len(txIDs), found)
}

// --------------------------------------------------------------------------
// Health endpoint assertions
// --------------------------------------------------------------------------

// HealthStatus represents parsed /ready response.
type HealthStatus struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks,omitempty"`
}

// AssertTableRowCount verifies the row count of a specific table.
func AssertTableRowCount(t *testing.T, db *sql.DB, table string, expected int) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var count int
	//nolint:gosec // table name is test-controlled, not user input.
	err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s`, table)).Scan(&count)
	require.NoError(t, err, "count rows in %s", table)
	assert.Equal(t, expected, count, "table %s: expected %d rows, got %d", table, expected, count)
}

// AssertTableRowCountAtLeast verifies minimum row count.
func AssertTableRowCountAtLeast(t *testing.T, db *sql.DB, table string, minCount int) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var count int
	//nolint:gosec // table name is test-controlled, not user input.
	err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s`, table)).Scan(&count)
	require.NoError(t, err, "count rows in %s", table)
	assert.GreaterOrEqual(t, count, minCount,
		"table %s: expected at least %d rows, got %d", table, minCount, count)
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
