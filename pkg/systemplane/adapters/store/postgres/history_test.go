//go:build unit

// Copyright 2025 Lerian Studio.

package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/pkg/systemplane/adapters/store/secretcodec"
	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

func TestNewHistoryQueryBuilder(t *testing.T) {
	t.Parallel()

	builder := newHistoryQueryBuilder()

	assert.NotNil(t, builder)
	assert.Empty(t, builder.clauses)
	assert.Empty(t, builder.args)
	assert.Equal(t, 0, builder.argIdx)
}

func TestHistoryQueryBuilder_NextArg(t *testing.T) {
	t.Parallel()

	t.Run("increments sequentially", func(t *testing.T) {
		t.Parallel()

		builder := newHistoryQueryBuilder()

		assert.Equal(t, "$1", builder.nextArg())
		assert.Equal(t, "$2", builder.nextArg())
		assert.Equal(t, "$3", builder.nextArg())
		assert.Equal(t, 3, builder.argIdx)
	})
}

func TestHistoryQueryBuilder_AddFilterClause(t *testing.T) {
	t.Parallel()

	t.Run("adds clause for non-empty value", func(t *testing.T) {
		t.Parallel()

		builder := newHistoryQueryBuilder()
		builder.addFilterClause("kind", "config")

		require.Len(t, builder.clauses, 1)
		assert.Equal(t, "kind = $1", builder.clauses[0])
		require.Len(t, builder.args, 1)
		assert.Equal(t, "config", builder.args[0])
	})

	t.Run("skips clause for empty value", func(t *testing.T) {
		t.Parallel()

		builder := newHistoryQueryBuilder()
		builder.addFilterClause("kind", "")

		assert.Empty(t, builder.clauses)
		assert.Empty(t, builder.args)
		assert.Equal(t, 0, builder.argIdx)
	})

	t.Run("adds multiple clauses with incrementing args", func(t *testing.T) {
		t.Parallel()

		builder := newHistoryQueryBuilder()
		builder.addFilterClause("kind", "config")
		builder.addFilterClause("scope", "global")
		builder.addFilterClause("subject", "tenant-abc")

		require.Len(t, builder.clauses, 3)
		assert.Equal(t, "kind = $1", builder.clauses[0])
		assert.Equal(t, "scope = $2", builder.clauses[1])
		assert.Equal(t, "subject = $3", builder.clauses[2])
		assert.Equal(t, []any{"config", "global", "tenant-abc"}, builder.args)
	})

	t.Run("skips empty values in mixed sequence", func(t *testing.T) {
		t.Parallel()

		builder := newHistoryQueryBuilder()
		builder.addFilterClause("kind", "config")
		builder.addFilterClause("scope", "")
		builder.addFilterClause("key", "log_level")

		require.Len(t, builder.clauses, 2)
		assert.Equal(t, "kind = $1", builder.clauses[0])
		assert.Equal(t, "key = $2", builder.clauses[1])
		assert.Equal(t, []any{"config", "log_level"}, builder.args)
	})
}

func TestHistoryQueryBuilder_AddPaginationClause(t *testing.T) {
	t.Parallel()

	t.Run("appends LIMIT clause for positive value", func(t *testing.T) {
		t.Parallel()

		builder := newHistoryQueryBuilder()
		query := "SELECT * FROM table ORDER BY id"
		builder.addPaginationClause(&query, "LIMIT", 10)

		assert.Contains(t, query, "LIMIT $1")
		require.Len(t, builder.args, 1)
		assert.Equal(t, 10, builder.args[0])
	})

	t.Run("appends OFFSET clause for positive value", func(t *testing.T) {
		t.Parallel()

		builder := newHistoryQueryBuilder()
		query := "SELECT * FROM table ORDER BY id"
		builder.addPaginationClause(&query, "OFFSET", 20)

		assert.Contains(t, query, "OFFSET $1")
		require.Len(t, builder.args, 1)
		assert.Equal(t, 20, builder.args[0])
	})

	t.Run("skips clause for zero value", func(t *testing.T) {
		t.Parallel()

		builder := newHistoryQueryBuilder()
		query := "SELECT * FROM table ORDER BY id"
		original := query
		builder.addPaginationClause(&query, "LIMIT", 0)

		assert.Equal(t, original, query)
		assert.Empty(t, builder.args)
	})

	t.Run("skips clause for negative value", func(t *testing.T) {
		t.Parallel()

		builder := newHistoryQueryBuilder()
		query := "SELECT * FROM table ORDER BY id"
		original := query
		builder.addPaginationClause(&query, "LIMIT", -5)

		assert.Equal(t, original, query)
		assert.Empty(t, builder.args)
	})

	t.Run("combines LIMIT and OFFSET with filters", func(t *testing.T) {
		t.Parallel()

		builder := newHistoryQueryBuilder()
		builder.addFilterClause("kind", "config")

		query := "SELECT * FROM table WHERE kind = $1 ORDER BY id"
		builder.addPaginationClause(&query, "LIMIT", 10)
		builder.addPaginationClause(&query, "OFFSET", 5)

		assert.Contains(t, query, "LIMIT $2")
		assert.Contains(t, query, "OFFSET $3")
		assert.Equal(t, []any{"config", 10, 5}, builder.args)
	})
}

func TestDecodeOptionalJSONValue(t *testing.T) {
	t.Parallel()

	t.Run("nil input returns nil and false", func(t *testing.T) {
		t.Parallel()

		value, hasValue, err := decodeOptionalJSONValue(nil, "test_field")

		require.NoError(t, err)
		assert.Nil(t, value)
		assert.False(t, hasValue)
	})

	t.Run("valid JSON string returns decoded value and true", func(t *testing.T) {
		t.Parallel()

		value, hasValue, err := decodeOptionalJSONValue([]byte(`"hello"`), "test_field")

		require.NoError(t, err)
		assert.Equal(t, "hello", value)
		assert.True(t, hasValue)
	})

	t.Run("valid JSON integer returns int and true", func(t *testing.T) {
		t.Parallel()

		value, hasValue, err := decodeOptionalJSONValue([]byte(`42`), "test_field")

		require.NoError(t, err)
		assert.Equal(t, 42, value)
		assert.True(t, hasValue)
	})

	t.Run("invalid JSON returns error with field name", func(t *testing.T) {
		t.Parallel()

		_, _, err := decodeOptionalJSONValue([]byte(`{bad}`), "my_field")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal my_field")
	})

	t.Run("valid JSON object returns map", func(t *testing.T) {
		t.Parallel()

		value, hasValue, err := decodeOptionalJSONValue([]byte(`{"key":"value"}`), "obj_field")

		require.NoError(t, err)
		assert.True(t, hasValue)

		obj, ok := value.(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "value", obj["key"])
	})
}

func TestHistoryStore_DecryptValue(t *testing.T) {
	t.Parallel()

	t.Run("nil store returns value unchanged", func(t *testing.T) {
		t.Parallel()

		var nilStore *HistoryStore
		target := domain.Target{Kind: domain.KindConfig, Scope: domain.ScopeGlobal}

		result, err := nilStore.decryptValue(target, "some_key", "plain-text")

		require.NoError(t, err)
		assert.Equal(t, "plain-text", result)
	})

	t.Run("nil codec returns value unchanged", func(t *testing.T) {
		t.Parallel()

		store := &HistoryStore{secretCodec: nil}
		target := domain.Target{Kind: domain.KindConfig, Scope: domain.ScopeGlobal}

		result, err := store.decryptValue(target, "some_key", "plain-text")

		require.NoError(t, err)
		assert.Equal(t, "plain-text", result)
	})

	t.Run("decrypts encrypted value successfully", func(t *testing.T) {
		t.Parallel()

		codec, err := secretcodec.New("0123456789abcdef0123456789abcdef", []string{"db.password"})
		require.NoError(t, err)

		store := &HistoryStore{secretCodec: codec}
		target := domain.Target{Kind: domain.KindConfig, Scope: domain.ScopeGlobal}

		encrypted, err := codec.Encrypt(target, "db.password", "secret-pass")
		require.NoError(t, err)

		result, err := store.decryptValue(target, "db.password", encrypted)

		require.NoError(t, err)
		assert.Equal(t, "secret-pass", result)
	})

	t.Run("returns non-secret value unchanged when codec present", func(t *testing.T) {
		t.Parallel()

		codec, err := secretcodec.New("0123456789abcdef0123456789abcdef", []string{"db.password"})
		require.NoError(t, err)

		store := &HistoryStore{secretCodec: codec}
		target := domain.Target{Kind: domain.KindConfig, Scope: domain.ScopeGlobal}

		result, err := store.decryptValue(target, "log_level", "debug")

		require.NoError(t, err)
		assert.Equal(t, "debug", result)
	})
}

func TestHistoryStore_ListHistory_NilDB(t *testing.T) {
	t.Parallel()

	t.Run("nil store returns error", func(t *testing.T) {
		t.Parallel()

		var nilStore *HistoryStore

		_, err := nilStore.ListHistory(t.Context(), ports.HistoryFilter{})

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNilDB)
	})

	t.Run("nil db field returns error", func(t *testing.T) {
		t.Parallel()

		store := &HistoryStore{db: nil}

		_, err := store.ListHistory(t.Context(), ports.HistoryFilter{})

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNilDB)
	})
}
