//go:build unit

package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeOptionalText_NilInput(t *testing.T) {
	t.Parallel()

	result := NormalizeOptionalText(nil)
	require.Nil(t, result)
}

func TestNormalizeOptionalText_EmptyString(t *testing.T) {
	t.Parallel()

	empty := ""
	result := NormalizeOptionalText(&empty)
	require.Nil(t, result)
}

func TestNormalizeOptionalText_WhitespaceOnly(t *testing.T) {
	t.Parallel()

	spaces := "   "
	result := NormalizeOptionalText(&spaces)
	require.Nil(t, result)
}

func TestNormalizeOptionalText_TabsAndNewlines(t *testing.T) {
	t.Parallel()

	whitespace := "\t\n  \r\n"
	result := NormalizeOptionalText(&whitespace)
	require.Nil(t, result)
}

func TestNormalizeOptionalText_ValidText(t *testing.T) {
	t.Parallel()

	text := "hello"
	result := NormalizeOptionalText(&text)
	require.NotNil(t, result)
	require.Equal(t, "hello", *result)
}

func TestNormalizeOptionalText_TrimsLeadingTrailingSpaces(t *testing.T) {
	t.Parallel()

	text := "  hello world  "
	result := NormalizeOptionalText(&text)
	require.NotNil(t, result)
	require.Equal(t, "hello world", *result)
}

func TestNormalizeOptionalText_PreservesInternalSpaces(t *testing.T) {
	t.Parallel()

	text := "hello   world"
	result := NormalizeOptionalText(&text)
	require.NotNil(t, result)
	require.Equal(t, "hello   world", *result)
}

func TestNormalizeOptionalText_DoesNotMutateInput(t *testing.T) {
	t.Parallel()

	original := "  padded  "
	NormalizeOptionalText(&original)
	require.Equal(t, "  padded  ", original, "input string must not be mutated")
}
