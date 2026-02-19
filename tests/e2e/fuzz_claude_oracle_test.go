//go:build e2e

package e2e

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractJSONObject_CleanJSON(t *testing.T) {
	t.Parallel()

	input := `{"totalFee": "15.00", "netAmount": "985.00"}`

	result, err := extractJSONObject(input)
	require.NoError(t, err)
	assert.Equal(t, input, result)
}

func TestExtractJSONObject_EmbeddedInText(t *testing.T) {
	t.Parallel()

	input := `Here is the result: {"totalFee": "15.00", "netAmount": "985.00"} and that's it.`

	result, err := extractJSONObject(input)
	require.NoError(t, err)
	assert.JSONEq(t, `{"totalFee": "15.00", "netAmount": "985.00"}`, result)
}

func TestExtractJSONObject_NestedObjects(t *testing.T) {
	t.Parallel()

	input := `{"outer": {"inner": {"deep": true}}, "value": 42}`

	result, err := extractJSONObject(input)
	require.NoError(t, err)
	assert.Equal(t, input, result)
}

func TestExtractJSONObject_WithEscapedBraces(t *testing.T) {
	t.Parallel()

	input := `{"message": "use {braces} like \"this\""}`

	result, err := extractJSONObject(input)
	require.NoError(t, err)
	assert.Equal(t, input, result)
}

func TestExtractJSONObject_NoBraces(t *testing.T) {
	t.Parallel()

	input := `no json here at all`

	_, err := extractJSONObject(input)
	assert.ErrorIs(t, err, errNoOpenBrace)
}

func TestExtractJSONObject_UnmatchedBrace(t *testing.T) {
	t.Parallel()

	input := `{"open": "but never closed`

	_, err := extractJSONObject(input)
	assert.ErrorIs(t, err, errNoMatchingBrace)
}

func TestExtractJSONArray_CleanArray(t *testing.T) {
	t.Parallel()

	input := `[{"id": "1"}, {"id": "2"}]`

	result, err := extractJSONArray(input)
	require.NoError(t, err)
	assert.Equal(t, input, result)
}

func TestExtractJSONArray_EmbeddedInText(t *testing.T) {
	t.Parallel()

	input := "Here are the results:\n[{\"id\": \"1\"}, {\"id\": \"2\"}]\nDone."

	result, err := extractJSONArray(input)
	require.NoError(t, err)
	assert.JSONEq(t, `[{"id": "1"}, {"id": "2"}]`, result)
}

func TestExtractJSONArray_NestedArrays(t *testing.T) {
	t.Parallel()

	input := `[[1, 2], [3, [4, 5]]]`

	result, err := extractJSONArray(input)
	require.NoError(t, err)
	assert.Equal(t, input, result)
}

func TestExtractJSONArray_NoBrackets(t *testing.T) {
	t.Parallel()

	input := `no array here`

	_, err := extractJSONArray(input)
	assert.ErrorIs(t, err, errNoOpenBracket)
}

func TestExtractJSONArray_UnmatchedBracket(t *testing.T) {
	t.Parallel()

	input := `["open but never closed`

	_, err := extractJSONArray(input)
	assert.ErrorIs(t, err, errNoMatchingBracket)
}

func TestExtractJSONObject_StringsWithBrackets(t *testing.T) {
	t.Parallel()

	// Braces inside strings should not confuse the parser.
	input := `prefix {"key": "value with } and { inside"} suffix`

	result, err := extractJSONObject(input)
	require.NoError(t, err)
	assert.JSONEq(t, `{"key": "value with } and { inside"}`, result)
}

func TestExtractJSONArray_StringsWithBrackets(t *testing.T) {
	t.Parallel()

	// Brackets inside strings should not confuse the parser.
	input := `before ["item with ] and [ inside"] after`

	result, err := extractJSONArray(input)
	require.NoError(t, err)
	assert.Equal(t, `["item with ] and [ inside"]`, result)
}

func TestNewClaudeOracle_CreatesNonNil(t *testing.T) {
	t.Parallel()

	oracle := NewClaudeOracle("", "claude-sonnet-4-20250514")
	require.NotNil(t, oracle)
	assert.Equal(t, "claude-sonnet-4-20250514", oracle.model)
	assert.NotNil(t, oracle.client)
}

func TestNewClaudeOracle_WithAPIKey(t *testing.T) {
	t.Parallel()

	oracle := NewClaudeOracle("sk-test-key-1234", "claude-opus-4-20250514")
	require.NotNil(t, oracle)
	assert.Equal(t, "claude-opus-4-20250514", oracle.model)
	assert.NotNil(t, oracle.client)
}
