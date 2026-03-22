//go:build unit

package command

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
)

// --- Constructor tests ---

func TestNewPartitionManager_Success(t *testing.T) {
	t.Parallel()

	db, _, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	pm, err := NewPartitionManager(db, nil, nil)
	require.NoError(t, err)
	assert.NotNil(t, pm)
}

func TestNewPartitionManager_NilDB(t *testing.T) {
	t.Parallel()

	pm, err := NewPartitionManager(nil, nil, nil)
	assert.Nil(t, pm)
	assert.ErrorIs(t, err, ErrNilDB)
}

func TestNewPartitionManagerWithClock_NilClock(t *testing.T) {
	t.Parallel()

	db, _, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	pm, err := NewPartitionManagerWithClock(db, nil, nil, nil)
	assert.Nil(t, pm)
	assert.ErrorIs(t, err, ErrNowFuncRequired)
}

// --- Partition name validation tests ---

func TestValidatePartitionName_Valid(t *testing.T) {
	t.Parallel()

	validNames := []string{
		"audit_logs_2026_01",
		"audit_logs_2026_12",
		"audit_logs_2025_06",
		"audit_logs_2030_02",
	}

	for _, name := range validNames {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.NoError(t, validatePartitionName(name))
		})
	}
}

func TestValidatePartitionName_Invalid(t *testing.T) {
	t.Parallel()

	invalidNames := []struct {
		name   string
		reason string
	}{
		{"audit_logs_2026_1", "single digit month"},
		{"audit_logs_202_01", "short year"},
		{"audit_logs_20260_01", "five digit year"},
		{"audit_logs", "no date"},
		{"other_table_2026_01", "wrong prefix"},
		{"audit_logs_2026_01; DROP TABLE users", "SQL injection attempt"},
		{"audit_logs_2026_01--", "SQL comment injection"},
		{"", "empty string"},
		{"audit_logs_2026_01\n", "newline injection"},
		{"AUDIT_LOGS_2026_01", "uppercase"},
	}

	for _, tc := range invalidNames {
		t.Run(tc.reason, func(t *testing.T) {
			t.Parallel()
			err := validatePartitionName(tc.name)
			assert.ErrorIs(t, err, ErrInvalidPartitionName)
		})
	}
}

// --- Partition name generation tests ---

func TestPartitionName_Generation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    time.Time
		expected string
	}{
		{
			name:     "february 2026",
			input:    time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
			expected: "audit_logs_2026_02",
		},
		{
			name:     "december 2025",
			input:    time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC),
			expected: "audit_logs_2025_12",
		},
		{
			name:     "january 2027",
			input:    time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
			expected: "audit_logs_2027_01",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expected, partitionName(tc.input))
		})
	}
}

// --- monthStart edge case tests ---

func TestMonthStart_YearBoundary(t *testing.T) {
	t.Parallel()

	// December 2025 + 1 month = January 2026
	dec := time.Date(2025, 12, 15, 10, 30, 0, 0, time.UTC)
	result := monthStart(dec, 1)
	assert.Equal(t, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), result)
}

func TestMonthStart_DecemberToJanuaryRollover(t *testing.T) {
	t.Parallel()

	// November + 2 months = January of next year
	nov := time.Date(2025, 11, 20, 0, 0, 0, 0, time.UTC)
	result := monthStart(nov, 2)
	assert.Equal(t, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), result)
}

func TestMonthStart_ZeroOffset(t *testing.T) {
	t.Parallel()

	feb := time.Date(2026, 2, 15, 10, 0, 0, 0, time.UTC)
	result := monthStart(feb, 0)
	assert.Equal(t, time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC), result)
}

// --- EnsurePartitionsExist tests ---

func TestEnsurePartitionsExist_InvalidLookahead(t *testing.T) {
	t.Parallel()

	db, _, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	pm, err := NewPartitionManager(db, nil, nil)
	require.NoError(t, err)

	tests := []struct {
		name      string
		lookahead int
	}{
		{"zero", 0},
		{"negative", -1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := pm.EnsurePartitionsExist(context.Background(), tc.lookahead)
			assert.ErrorIs(t, err, ErrInvalidLookahead)
		})
	}
}

func TestEnsurePartitionsExist_CreatesCorrectDDL(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	fixedNow := time.Date(2026, 2, 15, 12, 0, 0, 0, time.UTC)
	pm, err := NewPartitionManagerWithClock(db, nil, nil, func() time.Time {
		return fixedNow
	})
	require.NoError(t, err)

	// Use default tenant context so ApplyTenantSchema is a no-op
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "11111111-1111-1111-1111-111111111111")

	mock.ExpectBegin()
	// ApplyTenantSchema for default tenant is a no-op (no SET LOCAL query)

	// The service creates partitions for current month + lookaheadMonths (2)
	// That's 3 partitions total (i in range(3): 0, 1, 2)
	now := fixedNow
	for i := range 3 {
		start := monthStart(now, i)
		end := monthStart(now, i+1)
		name := partitionName(start)

		expectedDDL := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s PARTITION OF audit_logs FOR VALUES FROM ('%s') TO ('%s')",
			name,
			start.Format("2006-01-02"),
			end.Format("2006-01-02"),
		)

		mock.ExpectExec(regexp.QuoteMeta(expectedDDL)).
			WillReturnResult(sqlmock.NewResult(0, 0))
	}

	mock.ExpectCommit()

	err = pm.EnsurePartitionsExist(ctx, 2)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEnsurePartitionsExist_BeginTxError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	pm, err := NewPartitionManager(db, nil, nil)
	require.NoError(t, err)

	mock.ExpectBegin().WillReturnError(errors.New("connection refused"))

	err = pm.EnsurePartitionsExist(context.Background(), 2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "begin transaction")
}

func TestEnsurePartitionsExist_CreatePartitionError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	pm, err := NewPartitionManager(db, nil, nil)
	require.NoError(t, err)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "11111111-1111-1111-1111-111111111111")

	mock.ExpectBegin()
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS").
		WillReturnError(errors.New("table already exists as non-partition"))
	mock.ExpectRollback()

	err = pm.EnsurePartitionsExist(ctx, 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "create partition")
}

// --- ListPartitions tests ---

func TestListPartitions_Success(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	pm, err := NewPartitionManager(db, nil, nil)
	require.NoError(t, err)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "11111111-1111-1111-1111-111111111111")

	mock.ExpectBegin()

	rows := sqlmock.NewRows([]string{"partition_name", "partition_bound", "approx_row_count"}).
		AddRow("audit_logs_2026_02", "FOR VALUES FROM ('2026-02-01 00:00:00') TO ('2026-03-01 00:00:00')", int64(1500)).
		AddRow("audit_logs_2026_03", "FOR VALUES FROM ('2026-03-01 00:00:00') TO ('2026-04-01 00:00:00')", int64(0))

	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	mock.ExpectCommit()

	partitions, err := pm.ListPartitions(ctx)
	require.NoError(t, err)
	require.Len(t, partitions, 2)

	assert.Equal(t, "audit_logs_2026_02", partitions[0].Name)
	assert.Equal(t, time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC), partitions[0].RangeStart)
	assert.Equal(t, time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC), partitions[0].RangeEnd)
	assert.Equal(t, int64(1500), partitions[0].ApproxRowCount)

	assert.Equal(t, "audit_logs_2026_03", partitions[1].Name)
	assert.Equal(t, int64(0), partitions[1].ApproxRowCount)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestListPartitions_Empty(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	pm, err := NewPartitionManager(db, nil, nil)
	require.NoError(t, err)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "11111111-1111-1111-1111-111111111111")

	mock.ExpectBegin()

	rows := sqlmock.NewRows([]string{"partition_name", "partition_bound", "approx_row_count"})
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	mock.ExpectCommit()

	partitions, err := pm.ListPartitions(ctx)
	assert.NoError(t, err)
	assert.Empty(t, partitions)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestListPartitions_QueryError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	pm, err := NewPartitionManager(db, nil, nil)
	require.NoError(t, err)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "11111111-1111-1111-1111-111111111111")

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT").WillReturnError(errors.New("connection lost"))
	mock.ExpectRollback()

	partitions, err := pm.ListPartitions(ctx)
	assert.Error(t, err)
	assert.Nil(t, partitions)
	assert.Contains(t, err.Error(), "query partitions")
}

func TestListPartitions_BeginTxError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	pm, err := NewPartitionManager(db, nil, nil)
	require.NoError(t, err)

	mock.ExpectBegin().WillReturnError(errors.New("connection refused"))

	partitions, err := pm.ListPartitions(context.Background())
	assert.Error(t, err)
	assert.Nil(t, partitions)
	assert.Contains(t, err.Error(), "begin transaction")
}

// --- DetachPartition tests ---

func TestDetachPartition_Success(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	pm, err := NewPartitionManager(db, nil, nil)
	require.NoError(t, err)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "11111111-1111-1111-1111-111111111111")

	mock.ExpectBegin()
	mock.ExpectExec("ALTER TABLE audit_logs DETACH PARTITION audit_logs_2015_01").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	err = pm.DetachPartition(ctx, "audit_logs_2015_01")
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestDetachPartition_RetentionPeriodActive(t *testing.T) {
	t.Parallel()

	db, _, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	fixedNow := time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC)
	pm, err := NewPartitionManagerWithClock(db, nil, nil, func() time.Time {
		return fixedNow
	})
	require.NoError(t, err)

	err = pm.DetachPartition(context.Background(), "audit_logs_2026_02")
	assert.ErrorIs(t, err, ErrRetentionPeriodActive)
	assert.Contains(t, err.Error(), "retention period")
}

func TestDetachPartition_InvalidName(t *testing.T) {
	t.Parallel()

	db, _, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	pm, err := NewPartitionManager(db, nil, nil)
	require.NoError(t, err)

	err = pm.DetachPartition(context.Background(), "malicious_table; DROP TABLE users")
	assert.ErrorIs(t, err, ErrInvalidPartitionName)
}

func TestDetachPartition_ExecError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	pm, err := NewPartitionManager(db, nil, nil)
	require.NoError(t, err)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "11111111-1111-1111-1111-111111111111")

	mock.ExpectBegin()
	mock.ExpectExec("ALTER TABLE audit_logs DETACH PARTITION").
		WillReturnError(errors.New("partition does not exist"))
	mock.ExpectRollback()

	err = pm.DetachPartition(ctx, "audit_logs_2015_01")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "detach partition")
}

// --- DropPartition tests ---

func TestDropPartition_Success(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	fixedNow := time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC)
	pm, err := NewPartitionManagerWithClock(db, nil, nil, func() time.Time {
		return fixedNow
	})
	require.NoError(t, err)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "11111111-1111-1111-1111-111111111111")

	mock.ExpectBegin()
	mock.ExpectExec("DROP TABLE IF EXISTS audit_logs_2015_01").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	err = pm.DropPartition(ctx, "audit_logs_2015_01")
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestDropPartition_InvalidName(t *testing.T) {
	t.Parallel()

	db, _, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	pm, err := NewPartitionManager(db, nil, nil)
	require.NoError(t, err)

	err = pm.DropPartition(context.Background(), "audit_logs_2026_02; DROP DATABASE")
	assert.ErrorIs(t, err, ErrInvalidPartitionName)
}

func TestDropPartition_ExecError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	fixedNow := time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC)
	pm, err := NewPartitionManagerWithClock(db, nil, nil, func() time.Time {
		return fixedNow
	})
	require.NoError(t, err)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "11111111-1111-1111-1111-111111111111")

	mock.ExpectBegin()
	mock.ExpectExec("DROP TABLE IF EXISTS").
		WillReturnError(errors.New("permission denied"))
	mock.ExpectRollback()

	err = pm.DropPartition(ctx, "audit_logs_2015_01")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "drop partition")
}

func TestDropPartition_RetentionPeriodActive(t *testing.T) {
	t.Parallel()

	db, _, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	fixedNow := time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC)
	pm, err := NewPartitionManagerWithClock(db, nil, nil, func() time.Time {
		return fixedNow
	})
	require.NoError(t, err)

	err = pm.DropPartition(context.Background(), "audit_logs_2026_02")
	assert.ErrorIs(t, err, ErrRetentionPeriodActive)
	assert.Contains(t, err.Error(), "retention period")
}

// --- parsePartitionBound tests ---

func TestParsePartitionBound_Success(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		expr      string
		wantStart time.Time
		wantEnd   time.Time
	}{
		{
			name:      "standard timestamp format",
			expr:      "FOR VALUES FROM ('2026-02-01 00:00:00') TO ('2026-03-01 00:00:00')",
			wantStart: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
			wantEnd:   time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			start, end, err := parsePartitionBound(tc.expr)
			require.NoError(t, err)
			assert.Equal(t, tc.wantStart, start)
			assert.Equal(t, tc.wantEnd, end)
		})
	}
}

func TestParsePartitionBound_InvalidFormat(t *testing.T) {
	t.Parallel()

	invalidExprs := []struct {
		name string
		expr string
	}{
		{"empty string", ""},
		{"garbage", "not a partition bound"},
		{"missing TO", "FOR VALUES FROM ('2026-02-01 00:00:00')"},
	}

	for _, tc := range invalidExprs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, _, err := parsePartitionBound(tc.expr)
			assert.Error(t, err)
		})
	}
}

// --- Sentinel error identity tests ---

func TestSentinelErrors_Distinct(t *testing.T) {
	t.Parallel()

	sentinels := []error{
		ErrNilDB,
		ErrNowFuncRequired,
		ErrPartitionNotFound,
		ErrInvalidLookahead,
		ErrInvalidPartitionName,
		ErrRetentionPeriodActive,
	}

	for i, a := range sentinels {
		for j, b := range sentinels {
			if i != j {
				assert.NotEqual(t, a.Error(), b.Error(),
					"sentinel errors %d and %d should have unique messages", i, j)
			}
		}
	}
}

// --- EnsurePartitionsExist year boundary test ---

func TestEnsurePartitionsExist_DecemberToJanuaryRollover(t *testing.T) {
	t.Parallel()

	// This test verifies that partition naming works correctly across year boundaries.
	// The actual month depends on time.Now(), so we verify the naming function directly.
	dec := time.Date(2025, 12, 15, 0, 0, 0, 0, time.UTC)

	// Current month partition
	start0 := monthStart(dec, 0)
	assert.Equal(t, "audit_logs_2025_12", partitionName(start0))

	// Next month (January 2026)
	start1 := monthStart(dec, 1)
	assert.Equal(t, "audit_logs_2026_01", partitionName(start1))

	// Two months ahead (February 2026)
	start2 := monthStart(dec, 2)
	assert.Equal(t, "audit_logs_2026_02", partitionName(start2))

	// Verify the range boundaries are correct
	end0 := monthStart(dec, 1)
	assert.Equal(t, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), end0)
}

func TestTryParseTime_AllFormats(t *testing.T) {
	t.Parallel()

	formats := []string{
		"2006-01-02 15:04:05+00",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}

	tests := []struct {
		name     string
		input    string
		expected time.Time
	}{
		{
			name:     "timestamp with timezone",
			input:    "2026-02-01 00:00:00+00",
			expected: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "timestamp without timezone",
			input:    "2026-02-01 00:00:00",
			expected: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "date only",
			input:    "2026-02-01",
			expected: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "invalid format",
			input:    "not-a-date",
			expected: time.Time{},
		},
		{
			name:     "empty string",
			input:    "",
			expected: time.Time{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := tryParseTime(tc.input, formats)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestParsePartitionBound_AllTimestampFormats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		expr      string
		wantStart time.Time
		wantEnd   time.Time
	}{
		{
			name:      "timestamp with timezone",
			expr:      "FOR VALUES FROM ('2026-02-01 00:00:00+00') TO ('2026-03-01 00:00:00+00')",
			wantStart: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
			wantEnd:   time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:      "timestamp without timezone",
			expr:      "FOR VALUES FROM ('2026-02-01 00:00:00') TO ('2026-03-01 00:00:00')",
			wantStart: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
			wantEnd:   time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:      "date only",
			expr:      "FOR VALUES FROM ('2026-02-01') TO ('2026-03-01')",
			wantStart: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
			wantEnd:   time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			start, end, err := parsePartitionBound(tc.expr)
			require.NoError(t, err)
			assert.Equal(t, tc.wantStart, start)
			assert.Equal(t, tc.wantEnd, end)
		})
	}
}

func TestExtractBoundValues_ErrorCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		expr        string
		expectedErr error
	}{
		{
			name:        "missing FROM",
			expr:        "garbage",
			expectedErr: ErrMissingFromClause,
		},
		{
			name:        "missing TO",
			expr:        "FOR VALUES FROM ('2026-02-01 00:00:00') garbage",
			expectedErr: ErrMissingToClause,
		},
		{
			name:        "missing end delimiter",
			expr:        "FOR VALUES FROM ('2026-02-01 00:00:00') TO ('2026-03-01 00:00:00",
			expectedErr: ErrMissingEndDelimiter,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, _, err := extractBoundValues(tc.expr)
			require.ErrorIs(t, err, tc.expectedErr)
		})
	}
}
