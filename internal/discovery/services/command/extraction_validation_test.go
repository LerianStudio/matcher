// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// ---------------------------------------------------------------------------
// validateExtractionRequest
// ---------------------------------------------------------------------------

func TestValidateExtractionRequest_EmptyTables_ReturnsError(t *testing.T) {
	t.Parallel()

	err := validateExtractionRequest(map[string]any{}, sharedPorts.ExtractionParams{})

	require.ErrorIs(t, err, ErrInvalidExtractionRequest)
	assert.Contains(t, err.Error(), "at least one table is required")
}

func TestValidateExtractionRequest_InvalidStartDate_ReturnsError(t *testing.T) {
	t.Parallel()

	tables := map[string]any{"t": nil}
	params := sharedPorts.ExtractionParams{StartDate: "not-a-date"}

	err := validateExtractionRequest(tables, params)

	require.ErrorIs(t, err, ErrInvalidExtractionRequest)
	assert.Contains(t, err.Error(), "start date must use YYYY-MM-DD format")
}

func TestValidateExtractionRequest_InvalidEndDate_ReturnsError(t *testing.T) {
	t.Parallel()

	tables := map[string]any{"t": nil}
	params := sharedPorts.ExtractionParams{StartDate: "2026-01-01", EndDate: "13/01/2026"}

	err := validateExtractionRequest(tables, params)

	require.ErrorIs(t, err, ErrInvalidExtractionRequest)
	assert.Contains(t, err.Error(), "end date must use YYYY-MM-DD format")
}

func TestValidateExtractionRequest_EndBeforeStart_ReturnsError(t *testing.T) {
	t.Parallel()

	tables := map[string]any{"t": nil}
	params := sharedPorts.ExtractionParams{StartDate: "2026-03-10", EndDate: "2026-03-01"}

	err := validateExtractionRequest(tables, params)

	require.ErrorIs(t, err, ErrInvalidExtractionRequest)
	assert.Contains(t, err.Error(), "end date must be on or after start date")
}

func TestValidateExtractionRequest_BlankTableName_ReturnsError(t *testing.T) {
	t.Parallel()

	tables := map[string]any{"   ": nil}

	err := validateExtractionRequest(tables, sharedPorts.ExtractionParams{})

	require.ErrorIs(t, err, ErrInvalidExtractionRequest)
	assert.Contains(t, err.Error(), "table name is required")
}

func TestValidateExtractionRequest_InvalidTableConfig_ReturnsError(t *testing.T) {
	t.Parallel()

	tables := map[string]any{"orders": true}

	err := validateExtractionRequest(tables, sharedPorts.ExtractionParams{})

	require.ErrorIs(t, err, ErrInvalidExtractionRequest)
	assert.Contains(t, err.Error(), "table configuration must be an object")
}

func TestValidateExtractionRequest_ValidInput_NoError(t *testing.T) {
	t.Parallel()

	tables := map[string]any{
		"transactions": map[string]any{"columns": []string{"id", "amount"}},
	}
	params := sharedPorts.ExtractionParams{StartDate: "2026-01-01", EndDate: "2026-01-31"}

	err := validateExtractionRequest(tables, params)

	require.NoError(t, err)
}

func TestValidateExtractionRequest_NilTableConfig_NoError(t *testing.T) {
	t.Parallel()

	tables := map[string]any{"transactions": nil}

	err := validateExtractionRequest(tables, sharedPorts.ExtractionParams{})

	require.NoError(t, err)
}

func TestValidateExtractionRequest_EmptyDates_NoError(t *testing.T) {
	t.Parallel()

	tables := map[string]any{"t": nil}

	err := validateExtractionRequest(tables, sharedPorts.ExtractionParams{})

	require.NoError(t, err)
}

func TestValidateExtractionRequest_SameStartAndEndDate_NoError(t *testing.T) {
	t.Parallel()

	tables := map[string]any{"t": nil}
	params := sharedPorts.ExtractionParams{StartDate: "2026-06-15", EndDate: "2026-06-15"}

	err := validateExtractionRequest(tables, params)

	require.NoError(t, err)
}

func TestValidateExtractionRequest_OnlyStartDate_NoError(t *testing.T) {
	t.Parallel()

	tables := map[string]any{"t": nil}
	params := sharedPorts.ExtractionParams{StartDate: "2026-06-15"}

	err := validateExtractionRequest(tables, params)

	require.NoError(t, err)
}

func TestValidateExtractionRequest_OnlyEndDate_NoError(t *testing.T) {
	t.Parallel()

	tables := map[string]any{"t": nil}
	params := sharedPorts.ExtractionParams{EndDate: "2026-06-15"}

	err := validateExtractionRequest(tables, params)

	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// parseExtractionDate
// ---------------------------------------------------------------------------

func TestParseExtractionDate_EmptyString_ReturnsZero(t *testing.T) {
	t.Parallel()

	parsed, err := parseExtractionDate("test", "")
	require.NoError(t, err)
	assert.True(t, parsed.IsZero())
}

func TestParseExtractionDate_WhitespaceOnly_ReturnsZero(t *testing.T) {
	t.Parallel()

	parsed, err := parseExtractionDate("test", "   ")
	require.NoError(t, err)
	assert.True(t, parsed.IsZero())
}

func TestParseExtractionDate_ValidDate_ParsesCorrectly(t *testing.T) {
	t.Parallel()

	parsed, err := parseExtractionDate("start date", "2026-03-15")
	require.NoError(t, err)
	assert.Equal(t, 2026, parsed.Year())
	assert.Equal(t, 3, int(parsed.Month()))
	assert.Equal(t, 15, parsed.Day())
}

func TestParseExtractionDate_InvalidFormat_ReturnsError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		label string
		raw   string
	}{
		{"slash_format", "start date", "15/03/2026"},
		{"datetime", "end date", "2026-03-15T10:00:00Z"},
		{"us_format", "start date", "03-15-2026"},
		{"garbage", "end date", "not-a-date"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := parseExtractionDate(tt.label, tt.raw)
			require.ErrorIs(t, err, ErrInvalidExtractionRequest)
			assert.Contains(t, err.Error(), tt.label)
			assert.Contains(t, err.Error(), "YYYY-MM-DD format")
		})
	}
}

// ---------------------------------------------------------------------------
// validateExtractionScope
// ---------------------------------------------------------------------------

func TestValidateExtractionScope_EmptySchemas_ReturnsError(t *testing.T) {
	t.Parallel()

	err := validateExtractionScope(
		map[string]any{"transactions": nil},
		nil,
	)

	require.ErrorIs(t, err, ErrInvalidExtractionRequest)
	assert.Contains(t, err.Error(), "schema has not been discovered")
}

func TestValidateExtractionScope_UnknownTable_ReturnsError(t *testing.T) {
	t.Parallel()

	schemas := []*entities.DiscoveredSchema{
		{ID: uuid.New(), TableName: "transactions", Columns: []entities.ColumnInfo{{Name: "id"}}},
	}

	err := validateExtractionScope(
		map[string]any{"nonexistent": nil},
		schemas,
	)

	require.ErrorIs(t, err, ErrInvalidExtractionRequest)
	assert.Contains(t, err.Error(), "unknown table")
	assert.Contains(t, err.Error(), "nonexistent")
}

func TestValidateExtractionScope_UnknownColumn_ReturnsError(t *testing.T) {
	t.Parallel()

	schemas := []*entities.DiscoveredSchema{
		{ID: uuid.New(), TableName: "transactions", Columns: []entities.ColumnInfo{{Name: "id"}, {Name: "amount"}}},
	}

	err := validateExtractionScope(
		map[string]any{"transactions": map[string]any{"columns": []string{"id", "missing_col"}}},
		schemas,
	)

	require.ErrorIs(t, err, ErrInvalidExtractionRequest)
	assert.Contains(t, err.Error(), "unknown column")
	assert.Contains(t, err.Error(), "missing_col")
}

func TestValidateExtractionScope_ValidScope_NoError(t *testing.T) {
	t.Parallel()

	schemas := []*entities.DiscoveredSchema{
		{ID: uuid.New(), TableName: "transactions", Columns: []entities.ColumnInfo{{Name: "id"}, {Name: "amount"}}},
	}

	err := validateExtractionScope(
		map[string]any{"transactions": map[string]any{"columns": []string{"id", "amount"}}},
		schemas,
	)

	require.NoError(t, err)
}

func TestValidateExtractionScope_NilTableConfig_NoColumnValidation(t *testing.T) {
	t.Parallel()

	schemas := []*entities.DiscoveredSchema{
		{ID: uuid.New(), TableName: "transactions", Columns: []entities.ColumnInfo{{Name: "id"}}},
	}

	err := validateExtractionScope(
		map[string]any{"transactions": nil},
		schemas,
	)

	require.NoError(t, err)
}

func TestValidateExtractionScope_SkipsNilAndBlankSchemas(t *testing.T) {
	t.Parallel()

	schemas := []*entities.DiscoveredSchema{
		nil,
		{ID: uuid.New(), TableName: "", Columns: []entities.ColumnInfo{{Name: "id"}}},
		{ID: uuid.New(), TableName: "orders", Columns: []entities.ColumnInfo{{Name: "id"}}},
	}

	err := validateExtractionScope(
		map[string]any{"orders": map[string]any{"columns": []string{"id"}}},
		schemas,
	)

	require.NoError(t, err)
}

func TestValidateExtractionScope_SkipsBlankColumnNames(t *testing.T) {
	t.Parallel()

	schemas := []*entities.DiscoveredSchema{
		{
			ID:        uuid.New(),
			TableName: "transactions",
			Columns: []entities.ColumnInfo{
				{Name: "id"},
				{Name: ""},
				{Name: "   "},
			},
		},
	}

	// Only "id" should be recognized as a valid column.
	err := validateExtractionScope(
		map[string]any{"transactions": map[string]any{"columns": []string{"id"}}},
		schemas,
	)
	require.NoError(t, err)

	// Requesting a blank-named column won't exist in the allowed set.
	err = validateExtractionScope(
		map[string]any{"transactions": map[string]any{"columns": []string{""}}},
		schemas,
	)
	// Empty string column names get caught by validateRequestedColumns as "blanks"
	require.ErrorIs(t, err, ErrInvalidExtractionRequest)
}

// ---------------------------------------------------------------------------
// extractRequestedColumns
// ---------------------------------------------------------------------------

func TestExtractRequestedColumns_NilConfig_ReturnsNil(t *testing.T) {
	t.Parallel()

	cols, err := extractRequestedColumns(nil)
	require.NoError(t, err)
	assert.Nil(t, cols)
}

func TestExtractRequestedColumns_NonMapConfig_ReturnsError(t *testing.T) {
	t.Parallel()

	_, err := extractRequestedColumns(42)
	require.ErrorIs(t, err, ErrInvalidExtractionRequest)
	assert.Contains(t, err.Error(), "table configuration must be an object")
}

func TestExtractRequestedColumns_UnsupportedKey_ReturnsError(t *testing.T) {
	t.Parallel()

	_, err := extractRequestedColumns(map[string]any{"filters": "bad"})
	require.ErrorIs(t, err, ErrInvalidExtractionRequest)
	assert.Contains(t, err.Error(), "unsupported table configuration key")
	assert.Contains(t, err.Error(), "filters")
}

func TestExtractRequestedColumns_MissingColumnsKey_ReturnsNil(t *testing.T) {
	t.Parallel()

	cols, err := extractRequestedColumns(map[string]any{})
	require.NoError(t, err)
	assert.Nil(t, cols)
}

func TestExtractRequestedColumns_StringSlice_Preserved(t *testing.T) {
	t.Parallel()

	cols, err := extractRequestedColumns(map[string]any{"columns": []string{"id", "amount"}})
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "amount"}, cols)
}

func TestExtractRequestedColumns_AnySliceOfStrings_Accepted(t *testing.T) {
	t.Parallel()

	cols, err := extractRequestedColumns(map[string]any{"columns": []any{"id", "amount"}})
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "amount"}, cols)
}

func TestExtractRequestedColumns_AnySliceWithNonString_ReturnsError(t *testing.T) {
	t.Parallel()

	_, err := extractRequestedColumns(map[string]any{"columns": []any{"id", 42}})
	require.ErrorIs(t, err, ErrInvalidExtractionRequest)
	assert.Contains(t, err.Error(), "columns must be strings")
}

func TestExtractRequestedColumns_UnsupportedColumnsType_ReturnsError(t *testing.T) {
	t.Parallel()

	_, err := extractRequestedColumns(map[string]any{"columns": "not-an-array"})
	require.ErrorIs(t, err, ErrInvalidExtractionRequest)
	assert.Contains(t, err.Error(), "columns must be an array of strings")
}

// ---------------------------------------------------------------------------
// validateRequestedColumns
// ---------------------------------------------------------------------------

func TestValidateRequestedColumns_EmptySlice_ReturnsError(t *testing.T) {
	t.Parallel()

	_, err := validateRequestedColumns([]string{})
	require.ErrorIs(t, err, ErrInvalidExtractionRequest)
	assert.Contains(t, err.Error(), "columns must not be empty")
}

func TestValidateRequestedColumns_BlankColumn_ReturnsError(t *testing.T) {
	t.Parallel()

	_, err := validateRequestedColumns([]string{"id", "  "})
	require.ErrorIs(t, err, ErrInvalidExtractionRequest)
	assert.Contains(t, err.Error(), "columns must not contain blanks")
}

func TestValidateRequestedColumns_DeduplicatesColumns(t *testing.T) {
	t.Parallel()

	cols, err := validateRequestedColumns([]string{"id", "amount", "id"})
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "amount"}, cols)
}

func TestValidateRequestedColumns_TrimsWhitespace(t *testing.T) {
	t.Parallel()

	cols, err := validateRequestedColumns([]string{"  id  ", "amount "})
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "amount"}, cols)
}

func TestValidateRequestedColumns_DeduplicatesAfterTrimming(t *testing.T) {
	t.Parallel()

	cols, err := validateRequestedColumns([]string{"id", " id "})
	require.NoError(t, err)
	assert.Equal(t, []string{"id"}, cols)
}

func TestValidateRequestedColumns_SingleColumn_Accepted(t *testing.T) {
	t.Parallel()

	cols, err := validateRequestedColumns([]string{"amount"})
	require.NoError(t, err)
	assert.Equal(t, []string{"amount"}, cols)
}
