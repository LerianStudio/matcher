//go:build unit

// Copyright 2025 Lerian Studio.

package bootstrap

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// validatePostgresIdentifier (unexported helper)
// ---------------------------------------------------------------------------

func TestValidatePostgresIdentifier_ValidSimple(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
	}{
		{name: "lowercase letters", value: "schema"},
		{name: "with underscore", value: "my_schema"},
		{name: "starts with underscore", value: "_private"},
		{name: "letters and digits", value: "table42"},
		{name: "underscore and digits", value: "_t3st"},
		{name: "single char lowercase", value: "a"},
		{name: "single underscore", value: "_"},
		{name: "max 63 chars", value: strings.Repeat("a", 63)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validatePostgresIdentifier("test", tc.value)
			assert.NoError(t, err)
		})
	}
}

func TestValidatePostgresIdentifier_Invalid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
	}{
		{name: "empty string", value: ""},
		{name: "starts with digit", value: "1table"},
		{name: "contains hyphen", value: "bad-name"},
		{name: "contains dot", value: "schema.table"},
		{name: "contains space", value: "bad name"},
		{name: "uppercase letters", value: "Schema"},
		{name: "mixed case", value: "myTable"},
		{name: "contains dollar", value: "price$"},
		{name: "contains special char", value: "tbl@home"},
		{name: "too long (64 chars)", value: strings.Repeat("a", 64)},
		{name: "whitespace only", value: "   "},
		{name: "tab character", value: "\t"},
		{name: "newline character", value: "bad\nname"},
		{name: "contains unicode", value: "tëst"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validatePostgresIdentifier("test", tc.value)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrInvalidPostgresIdentifier)
		})
	}
}

func TestValidatePostgresIdentifier_ErrorContainsKindAndValue(t *testing.T) {
	t.Parallel()

	err := validatePostgresIdentifier("my_kind", "BAD-VALUE")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "my_kind")
	assert.Contains(t, err.Error(), "BAD-VALUE")
}

func TestValidatePostgresIdentifier_ErrorContainsPattern(t *testing.T) {
	t.Parallel()

	err := validatePostgresIdentifier("test", "INVALID")

	require.Error(t, err)
	// The error message should include the regex pattern for diagnostics.
	assert.Contains(t, err.Error(), "^[a-z_][a-z0-9_]{0,62}$")
}

// ---------------------------------------------------------------------------
// ValidatePostgresObjectNames (exported, validates the full set)
// ---------------------------------------------------------------------------

func TestValidatePostgresObjectNames_AllValid(t *testing.T) {
	t.Parallel()

	err := ValidatePostgresObjectNames(
		"system",
		"runtime_entries",
		"runtime_history",
		"runtime_revisions",
		"systemplane_changes",
	)
	assert.NoError(t, err)
}

func TestValidatePostgresObjectNames_AllDefaults(t *testing.T) {
	t.Parallel()

	err := ValidatePostgresObjectNames(
		DefaultPostgresSchema,
		DefaultPostgresEntriesTable,
		DefaultPostgresHistoryTable,
		DefaultPostgresRevisionTable,
		DefaultPostgresNotifyChannel,
	)
	assert.NoError(t, err)
}

func TestValidatePostgresObjectNames_InvalidSchema(t *testing.T) {
	t.Parallel()

	err := ValidatePostgresObjectNames(
		"BAD-SCHEMA",
		"runtime_entries",
		"runtime_history",
		"runtime_revisions",
		"systemplane_changes",
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidPostgresIdentifier)
	assert.Contains(t, err.Error(), "schema")
}

func TestValidatePostgresObjectNames_InvalidEntriesTable(t *testing.T) {
	t.Parallel()

	err := ValidatePostgresObjectNames(
		"system",
		"BAD-ENTRIES",
		"runtime_history",
		"runtime_revisions",
		"systemplane_changes",
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidPostgresIdentifier)
	assert.Contains(t, err.Error(), "entries table")
}

func TestValidatePostgresObjectNames_InvalidHistoryTable(t *testing.T) {
	t.Parallel()

	err := ValidatePostgresObjectNames(
		"system",
		"runtime_entries",
		"BAD-HISTORY",
		"runtime_revisions",
		"systemplane_changes",
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidPostgresIdentifier)
	assert.Contains(t, err.Error(), "history table")
}

func TestValidatePostgresObjectNames_InvalidRevisionTable(t *testing.T) {
	t.Parallel()

	err := ValidatePostgresObjectNames(
		"system",
		"runtime_entries",
		"runtime_history",
		"BAD-REVISION",
		"systemplane_changes",
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidPostgresIdentifier)
	assert.Contains(t, err.Error(), "revision table")
}

func TestValidatePostgresObjectNames_InvalidNotifyChannel(t *testing.T) {
	t.Parallel()

	err := ValidatePostgresObjectNames(
		"system",
		"runtime_entries",
		"runtime_history",
		"runtime_revisions",
		"BAD-CHANNEL",
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidPostgresIdentifier)
	assert.Contains(t, err.Error(), "notify channel")
}

func TestValidatePostgresObjectNames_FirstInvalidStopsValidation(t *testing.T) {
	t.Parallel()

	// When multiple identifiers are invalid, the first one encountered
	// should be reported (schema comes first).
	err := ValidatePostgresObjectNames(
		"BAD-SCHEMA",
		"BAD-ENTRIES",
		"BAD-HISTORY",
		"BAD-REVISION",
		"BAD-CHANNEL",
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidPostgresIdentifier)
	assert.Contains(t, err.Error(), "schema")
}

func TestValidatePostgresObjectNames_EmptyValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		schema  string
		entries string
		history string
		rev     string
		channel string
	}{
		{name: "empty schema", schema: "", entries: "e", history: "h", rev: "r", channel: "c"},
		{name: "empty entries", schema: "s", entries: "", history: "h", rev: "r", channel: "c"},
		{name: "empty history", schema: "s", entries: "e", history: "", rev: "r", channel: "c"},
		{name: "empty revision", schema: "s", entries: "e", history: "h", rev: "", channel: "c"},
		{name: "empty channel", schema: "s", entries: "e", history: "h", rev: "r", channel: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValidatePostgresObjectNames(tc.schema, tc.entries, tc.history, tc.rev, tc.channel)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrInvalidPostgresIdentifier)
		})
	}
}

// ---------------------------------------------------------------------------
// Boundary: exact length limits
// ---------------------------------------------------------------------------

func TestValidatePostgresIdentifier_ExactlyMaxLength(t *testing.T) {
	t.Parallel()

	// 63 chars total: 1 (leading) + 62 (trailing) = max allowed by {0,62}.
	valid := "a" + strings.Repeat("b", 62)
	assert.Len(t, valid, 63)

	err := validatePostgresIdentifier("test", valid)
	assert.NoError(t, err)
}

func TestValidatePostgresIdentifier_OneOverMaxLength(t *testing.T) {
	t.Parallel()

	// 64 chars is beyond the {0,62} quantifier.
	tooLong := "a" + strings.Repeat("b", 63)
	assert.Len(t, tooLong, 64)

	err := validatePostgresIdentifier("test", tooLong)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidPostgresIdentifier)
}

func TestValidatePostgresIdentifier_SingleCharValid(t *testing.T) {
	t.Parallel()

	// Single lowercase letter and single underscore are both valid starts.
	for _, ch := range []string{"a", "z", "_"} {
		t.Run(ch, func(t *testing.T) {
			t.Parallel()
			err := validatePostgresIdentifier("test", ch)
			assert.NoError(t, err)
		})
	}
}

// ---------------------------------------------------------------------------
// Allowed character classes in trailing positions
// ---------------------------------------------------------------------------

func TestValidatePostgresIdentifier_TrailingDigits(t *testing.T) {
	t.Parallel()

	err := validatePostgresIdentifier("test", "table_0123456789")
	assert.NoError(t, err)
}

func TestValidatePostgresIdentifier_AllLowercaseLetters(t *testing.T) {
	t.Parallel()

	err := validatePostgresIdentifier("test", "abcdefghijklmnopqrstuvwxyz")
	assert.NoError(t, err)
}

func TestValidatePostgresIdentifier_UnderscoreSeparated(t *testing.T) {
	t.Parallel()

	err := validatePostgresIdentifier("test", "a_b_c_d")
	assert.NoError(t, err)
}
