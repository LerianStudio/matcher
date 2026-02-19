//go:build unit

package shared

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseRuleType(t *testing.T) {
	t.Parallel()

	ruleType, err := ParseRuleType(" exact ")
	require.NoError(t, err)
	assert.Equal(t, RuleTypeExact, ruleType)

	_, err = ParseRuleType("invalid")
	require.ErrorIs(t, err, ErrInvalidRuleType)
}

func TestParseContextType(t *testing.T) {
	t.Parallel()

	ctxType, err := ParseContextType("1:1")
	require.NoError(t, err)
	assert.Equal(t, ContextTypeOneToOne, ctxType)

	_, err = ParseContextType("invalid")
	require.ErrorIs(t, err, ErrInvalidContextType)
}
