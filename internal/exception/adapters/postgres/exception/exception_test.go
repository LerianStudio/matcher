//go:build unit

package exception

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	sharedexception "github.com/LerianStudio/matcher/internal/shared/domain/exception"
)

// mockScannerImpl implements the scanner interface for direct scanInto testing.
type mockScannerImpl struct {
	err error
}

func (m *mockScannerImpl) Scan(_ ...any) error {
	return m.err
}

// --- Sentinel Errors ---

func TestSentinelErrors_AreDistinct(t *testing.T) {
	t.Parallel()

	sentinels := []error{
		ErrRepoNotInitialized,
		ErrConcurrentModification,
		ErrTransactionRequired,
	}

	for i := range len(sentinels) {
		for j := i + 1; j < len(sentinels); j++ {
			assert.NotErrorIs(t, sentinels[i], sentinels[j],
				"sentinel errors at index %d and %d must be distinct", i, j)
			assert.NotEqual(t, sentinels[i].Error(), sentinels[j].Error(),
				"sentinel error messages at index %d and %d must differ", i, j)
		}
	}
}

func TestSentinelErrors_CanBeWrappedAndUnwrapped(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"ErrRepoNotInitialized", ErrRepoNotInitialized},
		{"ErrConcurrentModification", ErrConcurrentModification},
		{"ErrTransactionRequired", ErrTransactionRequired},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			wrapped := fmt.Errorf("outer context: %w", tt.err)
			assert.ErrorIs(t, wrapped, tt.err)
			assert.Contains(t, wrapped.Error(), tt.err.Error())
		})
	}
}

func TestSentinelErrors_MessageContent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		contains string
	}{
		{"ErrRepoNotInitialized", ErrRepoNotInitialized, "not initialized"},
		{"ErrConcurrentModification", ErrConcurrentModification, "modified by another"},
		{"ErrTransactionRequired", ErrTransactionRequired, "transaction is required"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Contains(t, tt.err.Error(), tt.contains)
		})
	}
}

// --- columns constant ---

func TestColumns_ContainsAllExpectedFields(t *testing.T) {
	t.Parallel()

	expectedFields := []string{
		"id", "transaction_id", "severity", "status",
		"external_system", "external_issue_id", "assigned_to", "due_at",
		"resolution_notes", "resolution_type", "resolution_reason",
		"reason", "version", "created_at", "updated_at",
	}

	for _, field := range expectedFields {
		assert.Contains(t, columns, field, "columns should contain %q", field)
	}
}

func TestColumns_FieldCount(t *testing.T) {
	t.Parallel()

	parts := strings.Split(columns, ",")
	assert.Len(t, parts, 15, "columns should have exactly 15 comma-separated fields")

	for i, part := range parts {
		trimmed := strings.TrimSpace(part)
		assert.NotEmpty(t, trimmed, "column at position %d should not be empty after trimming", i)
	}
}

func TestColumns_IsNonEmpty(t *testing.T) {
	t.Parallel()

	assert.NotEmpty(t, columns)
}

// --- allowedSortColumns slice ---

func TestAllowedSortColumns_ContainsExpectedKeys(t *testing.T) {
	t.Parallel()

	expected := []string{"id", "created_at", "updated_at", "severity", "status"}

	assert.Len(t, allowedSortColumns, len(expected),
		"allowedSortColumns should have exactly %d entries", len(expected))

	for _, key := range expected {
		assert.Contains(t, allowedSortColumns, key,
			"allowedSortColumns should contain %q", key)
	}
}

func TestAllowedSortColumns_RejectsInvalidKeys(t *testing.T) {
	t.Parallel()

	rejected := []string{
		"transaction_id", "reason", "version",
		"assigned_to", "external_system", "external_issue_id",
		"due_at", "resolution_notes", "resolution_type",
		"nonexistent", "",
	}

	for _, key := range rejected {
		t.Run(fmt.Sprintf("rejects_%s", key), func(t *testing.T) {
			t.Parallel()
			assert.NotContains(t, allowedSortColumns, key,
				"allowedSortColumns should not allow %q", key)
		})
	}
}

// --- scanInto / scanException / scanRows ---

// newMockExceptionRows creates a sqlmock DB with rows using the standard exception column set.
func newMockExceptionRows(t *testing.T, rowValues ...[]driver.Value) (*sql.DB, sqlmock.Sqlmock, *sqlmock.Rows) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	cols := []string{
		"id", "transaction_id", "severity", "status",
		"external_system", "external_issue_id", "assigned_to", "due_at",
		"resolution_notes", "resolution_type", "resolution_reason",
		"reason", "version", "created_at", "updated_at",
	}

	mockRows := sqlmock.NewRows(cols)
	for _, row := range rowValues {
		mockRows.AddRow(row...)
	}

	return db, mock, mockRows
}

func makePopulatedRow(id, txID uuid.UUID, now time.Time) []driver.Value {
	return []driver.Value{
		id.String(),
		txID.String(),
		"HIGH",
		"OPEN",
		sql.NullString{String: "ServiceNow", Valid: true},
		sql.NullString{String: "INC-789", Valid: true},
		sql.NullString{String: "ops@corp.com", Valid: true},
		sql.NullTime{Time: now.Add(48 * time.Hour), Valid: true},
		sql.NullString{String: "Investigated and matched", Valid: true},
		sql.NullString{String: "MANUAL", Valid: true},
		sql.NullString{String: "Duplicate entry", Valid: true},
		sql.NullString{String: "FX rate unavailable", Valid: true},
		int64(5),
		now,
		now,
	}
}

func makeNullableRow(id, txID uuid.UUID, now time.Time) []driver.Value {
	return []driver.Value{
		id.String(),
		txID.String(),
		"LOW",
		"OPEN",
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		sql.NullTime{},
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		sql.NullString{},
		int64(1),
		now,
		now,
	}
}

var (
	fixedPopulatedExceptionID   = uuid.MustParse("11111111-1111-1111-1111-111111111111")
	fixedPopulatedTransactionID = uuid.MustParse("22222222-2222-2222-2222-222222222222")
	fixedPopulatedTimestamp     = time.Date(2026, time.January, 15, 10, 30, 45, 0, time.UTC)
)

// TestScanInto_ScanErrorIsWrapped tests that scanInto wraps the underlying scan error.
func TestScanInto_ScanErrorIsWrapped(t *testing.T) {
	t.Parallel()

	scanErr := errors.New("column count mismatch")
	mock := &mockScannerImpl{err: scanErr}

	result, err := scanInto(mock)

	require.Error(t, err)
	require.Nil(t, result)
	assert.ErrorIs(t, err, scanErr)
	assert.Contains(t, err.Error(), "scan exception row")
}

// TestScanException_AllFieldsPopulated verifies scanException correctly maps
// all populated nullable fields to non-nil pointers.
func TestScanException_AllFieldsPopulated(t *testing.T) {
	t.Parallel()

	exceptionID := fixedPopulatedExceptionID
	transactionID := fixedPopulatedTransactionID
	now := fixedPopulatedTimestamp

	db, mock, mockRows := newMockExceptionRows(t, makePopulatedRow(exceptionID, transactionID, now))
	defer db.Close()

	mock.ExpectQuery("SELECT").WillReturnRows(mockRows)

	row := db.QueryRow("SELECT 1")
	result, err := scanException(row)

	require.NoError(t, err)
	require.NotNil(t, result)

	// Identity fields.
	assert.Equal(t, exceptionID, result.ID)
	assert.Equal(t, transactionID, result.TransactionID)

	// Enum fields.
	assert.Equal(t, sharedexception.ExceptionSeverityHigh, result.Severity)
	assert.Equal(t, value_objects.ExceptionStatusOpen, result.Status)

	// Nullable string pointers.
	require.NotNil(t, result.ExternalSystem)
	assert.Equal(t, "ServiceNow", *result.ExternalSystem)

	require.NotNil(t, result.ExternalIssueID)
	assert.Equal(t, "INC-789", *result.ExternalIssueID)

	require.NotNil(t, result.AssignedTo)
	assert.Equal(t, "ops@corp.com", *result.AssignedTo)

	require.NotNil(t, result.ResolutionNotes)
	assert.Equal(t, "Investigated and matched", *result.ResolutionNotes)

	require.NotNil(t, result.ResolutionType)
	assert.Equal(t, "MANUAL", *result.ResolutionType)

	require.NotNil(t, result.ResolutionReason)
	assert.Equal(t, "Duplicate entry", *result.ResolutionReason)

	require.NotNil(t, result.Reason)
	assert.Equal(t, "FX rate unavailable", *result.Reason)

	// Nullable time pointer.
	require.NotNil(t, result.DueAt)
	assert.WithinDuration(t, now.Add(48*time.Hour), *result.DueAt, time.Second)

	// Version and timestamps.
	assert.Equal(t, int64(5), result.Version)
	assert.Equal(t, now, result.CreatedAt)
	assert.Equal(t, now, result.UpdatedAt)

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestScanException_NullFieldsReturnNilPointers verifies scanException correctly
// maps invalid (NULL) sql.Null* values to nil pointers.
func TestScanException_NullFieldsReturnNilPointers(t *testing.T) {
	t.Parallel()

	exceptionID := uuid.New()
	transactionID := uuid.New()
	now := time.Now().UTC()

	db, mock, mockRows := newMockExceptionRows(t, makeNullableRow(exceptionID, transactionID, now))
	defer db.Close()

	mock.ExpectQuery("SELECT").WillReturnRows(mockRows)

	row := db.QueryRow("SELECT 1")
	result, err := scanException(row)

	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, exceptionID, result.ID)
	assert.Equal(t, transactionID, result.TransactionID)
	assert.Equal(t, sharedexception.ExceptionSeverityLow, result.Severity)
	assert.Equal(t, value_objects.ExceptionStatusOpen, result.Status)

	// All nullable fields must be nil.
	assert.Nil(t, result.ExternalSystem)
	assert.Nil(t, result.ExternalIssueID)
	assert.Nil(t, result.AssignedTo)
	assert.Nil(t, result.DueAt)
	assert.Nil(t, result.ResolutionNotes)
	assert.Nil(t, result.ResolutionType)
	assert.Nil(t, result.ResolutionReason)
	assert.Nil(t, result.Reason)

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestScanException_ScanError verifies scanException propagates underlying scan errors.
func TestScanException_ScanError(t *testing.T) {
	t.Parallel()

	db, mock, _ := sqlmock.New()
	defer db.Close()

	mock.ExpectQuery("SELECT").WillReturnError(sql.ErrNoRows)

	row := db.QueryRow("SELECT 1")
	result, err := scanException(row)

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "scan exception row")

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestScanRows_SingleRow verifies scanRows correctly scans a single row from sql.Rows.
func TestScanRows_SingleRow(t *testing.T) {
	t.Parallel()

	exceptionID := uuid.New()
	transactionID := uuid.New()
	now := time.Now().UTC()

	db, mock, mockRows := newMockExceptionRows(t, makePopulatedRow(exceptionID, transactionID, now))
	defer db.Close()

	mock.ExpectQuery("SELECT").WillReturnRows(mockRows)

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	result, err := scanRows(rows)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, exceptionID, result.ID)
	assert.Equal(t, transactionID, result.TransactionID)
	assert.Equal(t, sharedexception.ExceptionSeverityHigh, result.Severity)
	assert.Equal(t, value_objects.ExceptionStatusOpen, result.Status)

	require.NoError(t, rows.Err())
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestScanRows_MultipleRows verifies scanRows can iterate through multiple rows
// and correctly scan each one with different nullable field states.
func TestScanRows_MultipleRows(t *testing.T) {
	t.Parallel()

	id1 := uuid.New()
	id2 := uuid.New()
	tx1 := uuid.New()
	tx2 := uuid.New()
	now := time.Now().UTC()

	db, mock, mockRows := newMockExceptionRows(t,
		makePopulatedRow(id1, tx1, now),
		makeNullableRow(id2, tx2, now),
	)
	defer db.Close()

	mock.ExpectQuery("SELECT").WillReturnRows(mockRows)

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	defer rows.Close()

	// First row: all fields populated.
	require.True(t, rows.Next())

	r1, err := scanRows(rows)
	require.NoError(t, err)
	assert.Equal(t, id1, r1.ID)
	assert.Equal(t, tx1, r1.TransactionID)
	assert.NotNil(t, r1.ExternalSystem)
	assert.Equal(t, sharedexception.ExceptionSeverityHigh, r1.Severity)

	// Second row: nullable fields null.
	require.True(t, rows.Next())

	r2, err := scanRows(rows)
	require.NoError(t, err)
	assert.Equal(t, id2, r2.ID)
	assert.Equal(t, tx2, r2.TransactionID)
	assert.Nil(t, r2.ExternalSystem)
	assert.Equal(t, sharedexception.ExceptionSeverityLow, r2.Severity)

	// No more rows.
	assert.False(t, rows.Next())
	require.NoError(t, rows.Err())

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestScannerInterfaceContract verifies that the types scanException and scanRows
// are designed to work with satisfy the scanner interface at compile time.
func TestScannerInterfaceContract(t *testing.T) {
	t.Parallel()

	// Compile-time verification: *sql.Row and *sql.Rows both satisfy scanner.
	var _ scanner = (*sql.Row)(nil)
	var _ scanner = (*sql.Rows)(nil)
}
