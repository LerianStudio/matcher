//go:build unit

// Copyright 2025 Lerian Studio.

package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFormatSchemaDDL(t *testing.T) {
	t.Parallel()

	t.Run("contains CREATE SCHEMA IF NOT EXISTS", func(t *testing.T) {
		t.Parallel()

		ddl := FormatSchemaDDL("my_schema")

		assert.Contains(t, ddl, "CREATE SCHEMA IF NOT EXISTS")
		assert.Contains(t, ddl, "my_schema")
	})

	t.Run("substitutes schema name", func(t *testing.T) {
		t.Parallel()

		ddl := FormatSchemaDDL("system")

		assert.Equal(t, "CREATE SCHEMA IF NOT EXISTS system", ddl)
	})

	t.Run("works with underscore-prefixed schema", func(t *testing.T) {
		t.Parallel()

		ddl := FormatSchemaDDL("_private")

		assert.Contains(t, ddl, "_private")
	})
}

func TestFormatEntriesDDL(t *testing.T) {
	t.Parallel()

	t.Run("contains CREATE TABLE with schema-qualified name", func(t *testing.T) {
		t.Parallel()

		ddl := FormatEntriesDDL("system", "runtime_entries")

		assert.Contains(t, ddl, "CREATE TABLE IF NOT EXISTS system.runtime_entries")
	})

	t.Run("contains PRIMARY KEY on compound key", func(t *testing.T) {
		t.Parallel()

		ddl := FormatEntriesDDL("system", "runtime_entries")

		assert.Contains(t, ddl, "PRIMARY KEY (kind, scope, subject, key)")
	})

	t.Run("contains target index with table name", func(t *testing.T) {
		t.Parallel()

		ddl := FormatEntriesDDL("system", "runtime_entries")

		assert.Contains(t, ddl, "idx_runtime_entries_target")
		assert.Contains(t, ddl, "ON system.runtime_entries")
	})

	t.Run("contains required columns", func(t *testing.T) {
		t.Parallel()

		ddl := FormatEntriesDDL("system", "runtime_entries")

		assert.Contains(t, ddl, "kind")
		assert.Contains(t, ddl, "scope")
		assert.Contains(t, ddl, "subject")
		assert.Contains(t, ddl, "key")
		assert.Contains(t, ddl, "value")
		assert.Contains(t, ddl, "JSONB")
		assert.Contains(t, ddl, "revision")
		assert.Contains(t, ddl, "updated_at")
		assert.Contains(t, ddl, "updated_by")
		assert.Contains(t, ddl, "source")
	})

	t.Run("custom schema and table names are substituted", func(t *testing.T) {
		t.Parallel()

		ddl := FormatEntriesDDL("myschema", "my_entries")

		assert.Contains(t, ddl, "myschema.my_entries")
		assert.Contains(t, ddl, "idx_my_entries_target")
	})
}

func TestFormatHistoryDDL(t *testing.T) {
	t.Parallel()

	t.Run("contains CREATE TABLE with schema-qualified name", func(t *testing.T) {
		t.Parallel()

		ddl := FormatHistoryDDL("system", "runtime_history")

		assert.Contains(t, ddl, "CREATE TABLE IF NOT EXISTS system.runtime_history")
	})

	t.Run("contains BIGSERIAL primary key", func(t *testing.T) {
		t.Parallel()

		ddl := FormatHistoryDDL("system", "runtime_history")

		assert.Contains(t, ddl, "BIGSERIAL")
		assert.Contains(t, ddl, "PRIMARY KEY")
	})

	t.Run("contains target_key index", func(t *testing.T) {
		t.Parallel()

		ddl := FormatHistoryDDL("system", "runtime_history")

		assert.Contains(t, ddl, "idx_runtime_history_target_key")
		assert.Contains(t, ddl, "ON system.runtime_history (kind, scope, subject, key)")
	})

	t.Run("contains changed_at descending index", func(t *testing.T) {
		t.Parallel()

		ddl := FormatHistoryDDL("system", "runtime_history")

		assert.Contains(t, ddl, "idx_runtime_history_changed_at")
		assert.Contains(t, ddl, "(changed_at DESC)")
	})

	t.Run("contains required columns", func(t *testing.T) {
		t.Parallel()

		ddl := FormatHistoryDDL("system", "runtime_history")

		assert.Contains(t, ddl, "kind")
		assert.Contains(t, ddl, "scope")
		assert.Contains(t, ddl, "subject")
		assert.Contains(t, ddl, "key")
		assert.Contains(t, ddl, "old_value")
		assert.Contains(t, ddl, "new_value")
		assert.Contains(t, ddl, "revision")
		assert.Contains(t, ddl, "actor_id")
		assert.Contains(t, ddl, "changed_at")
		assert.Contains(t, ddl, "source")
	})

	t.Run("custom table name in indexes", func(t *testing.T) {
		t.Parallel()

		ddl := FormatHistoryDDL("custom", "audit_trail")

		assert.Contains(t, ddl, "idx_audit_trail_target_key")
		assert.Contains(t, ddl, "idx_audit_trail_changed_at")
		assert.Contains(t, ddl, "custom.audit_trail")
	})
}

func TestFormatRevisionsDDL(t *testing.T) {
	t.Parallel()

	t.Run("contains CREATE TABLE with schema-qualified name", func(t *testing.T) {
		t.Parallel()

		ddl := FormatRevisionsDDL("system", "runtime_revisions")

		assert.Contains(t, ddl, "CREATE TABLE IF NOT EXISTS system.runtime_revisions")
	})

	t.Run("contains PRIMARY KEY on compound key", func(t *testing.T) {
		t.Parallel()

		ddl := FormatRevisionsDDL("system", "runtime_revisions")

		assert.Contains(t, ddl, "PRIMARY KEY (kind, scope, subject)")
	})

	t.Run("contains required columns", func(t *testing.T) {
		t.Parallel()

		ddl := FormatRevisionsDDL("system", "runtime_revisions")

		assert.Contains(t, ddl, "kind")
		assert.Contains(t, ddl, "scope")
		assert.Contains(t, ddl, "subject")
		assert.Contains(t, ddl, "revision")
		assert.Contains(t, ddl, "apply_behavior")
		assert.Contains(t, ddl, "updated_at")
		assert.Contains(t, ddl, "updated_by")
		assert.Contains(t, ddl, "source")
	})

	t.Run("contains ALTER TABLE for apply_behavior column migration", func(t *testing.T) {
		t.Parallel()

		ddl := FormatRevisionsDDL("system", "runtime_revisions")

		assert.Contains(t, ddl, "ALTER TABLE system.runtime_revisions ADD COLUMN IF NOT EXISTS apply_behavior")
	})

	t.Run("custom schema and table names are substituted", func(t *testing.T) {
		t.Parallel()

		ddl := FormatRevisionsDDL("myschema", "my_revisions")

		assert.Contains(t, ddl, "myschema.my_revisions")
	})
}

func TestDDL_ConsistentSchemaReference(t *testing.T) {
	t.Parallel()

	t.Run("all DDL functions use the same schema", func(t *testing.T) {
		t.Parallel()

		schema := "test_schema"

		schemaDDL := FormatSchemaDDL(schema)
		entriesDDL := FormatEntriesDDL(schema, "entries")
		historyDDL := FormatHistoryDDL(schema, "history")
		revisionsDDL := FormatRevisionsDDL(schema, "revisions")

		assert.Contains(t, schemaDDL, schema)
		assert.Contains(t, entriesDDL, schema+".entries")
		assert.Contains(t, historyDDL, schema+".history")
		assert.Contains(t, revisionsDDL, schema+".revisions")
	})
}
