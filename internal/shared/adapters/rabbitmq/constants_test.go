//go:build unit

package rabbitmq

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExchangeName_HasCorrectValue(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "matcher.events", ExchangeName)
}

func TestExchangeType_HasCorrectValue(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "topic", ExchangeType)
}

func TestDLXExchangeName_HasCorrectValue(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "matcher.events.dlx", DLXExchangeName)
}

func TestDLQName_HasCorrectValue(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "matcher.events.dlq", DLQName)
}

func TestExchangeName_NamingConventions(t *testing.T) {
	t.Parallel()

	t.Run("uses dot notation", func(t *testing.T) {
		t.Parallel()
		assert.Contains(t, ExchangeName, ".")
	})

	t.Run("starts with matcher prefix", func(t *testing.T) {
		t.Parallel()
		assert.True(t, strings.HasPrefix(ExchangeName, "matcher."))
	})

	t.Run("is not empty", func(t *testing.T) {
		t.Parallel()
		assert.NotEmpty(t, ExchangeName)
	})

	t.Run("contains no spaces", func(t *testing.T) {
		t.Parallel()
		assert.NotContains(t, ExchangeName, " ")
	})
}

func TestExchangeType_IsValidAMQPType(t *testing.T) {
	t.Parallel()

	validTypes := []string{"direct", "topic", "fanout", "headers"}
	assert.Contains(t, validTypes, ExchangeType)
}

func TestDLXExchangeName_NamingConventions(t *testing.T) {
	t.Parallel()

	t.Run("derives from main exchange", func(t *testing.T) {
		t.Parallel()
		assert.True(t, strings.HasPrefix(DLXExchangeName, ExchangeName))
	})

	t.Run("has dlx suffix", func(t *testing.T) {
		t.Parallel()
		assert.True(t, strings.HasSuffix(DLXExchangeName, ".dlx"))
	})

	t.Run("is not empty", func(t *testing.T) {
		t.Parallel()
		assert.NotEmpty(t, DLXExchangeName)
	})

	t.Run("contains no spaces", func(t *testing.T) {
		t.Parallel()
		assert.NotContains(t, DLXExchangeName, " ")
	})
}

func TestDLQName_NamingConventions(t *testing.T) {
	t.Parallel()

	t.Run("derives from main exchange", func(t *testing.T) {
		t.Parallel()
		assert.True(t, strings.HasPrefix(DLQName, ExchangeName))
	})

	t.Run("has dlq suffix", func(t *testing.T) {
		t.Parallel()
		assert.True(t, strings.HasSuffix(DLQName, ".dlq"))
	})

	t.Run("is not empty", func(t *testing.T) {
		t.Parallel()
		assert.NotEmpty(t, DLQName)
	})

	t.Run("contains no spaces", func(t *testing.T) {
		t.Parallel()
		assert.NotContains(t, DLQName, " ")
	})
}

func TestConstants_Consistency(t *testing.T) {
	t.Parallel()

	t.Run("DLX and DLQ share same base", func(t *testing.T) {
		t.Parallel()

		dlxBase := strings.TrimSuffix(DLXExchangeName, ".dlx")
		dlqBase := strings.TrimSuffix(DLQName, ".dlq")
		assert.Equal(t, dlxBase, dlqBase)
	})

	t.Run("all constants use lowercase", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, ExchangeName, strings.ToLower(ExchangeName))
		assert.Equal(t, ExchangeType, strings.ToLower(ExchangeType))
		assert.Equal(t, DLXExchangeName, strings.ToLower(DLXExchangeName))
		assert.Equal(t, DLQName, strings.ToLower(DLQName))
	})

	t.Run("no duplicate values", func(t *testing.T) {
		t.Parallel()

		values := []string{ExchangeName, DLXExchangeName, DLQName}
		seen := make(map[string]bool)

		for _, v := range values {
			assert.False(t, seen[v], "duplicate constant value: %s", v)
			seen[v] = true
		}
	})
}
