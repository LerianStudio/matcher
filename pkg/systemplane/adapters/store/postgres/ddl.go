// Copyright 2025 Lerian Studio.

// Package postgres provides PostgreSQL adapters for the systemplane store.
package postgres

import "fmt"

// DDL templates for systemplane PostgreSQL tables.
// Placeholders (%s) are filled with schema and table names from bootstrap
// config — these are operator-controlled, never user input, so Sprintf is safe.

const schemaDDLTemplate = `CREATE SCHEMA IF NOT EXISTS %s`

const entriesDDLTemplate = `CREATE TABLE IF NOT EXISTS %s.%s (
    kind       TEXT        NOT NULL,
    scope      TEXT        NOT NULL,
    subject    TEXT        NOT NULL DEFAULT '',
    key        TEXT        NOT NULL,
    value      JSONB,
    revision   BIGINT      NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by TEXT        NOT NULL DEFAULT '',
    source     TEXT        NOT NULL DEFAULT '',
    PRIMARY KEY (kind, scope, subject, key)
);

CREATE INDEX IF NOT EXISTS idx_%s_target
    ON %s.%s (kind, scope, subject)`

const historyDDLTemplate = `CREATE TABLE IF NOT EXISTS %s.%s (
    id         BIGSERIAL   PRIMARY KEY,
    kind       TEXT         NOT NULL,
    scope      TEXT         NOT NULL,
    subject    TEXT         NOT NULL DEFAULT '',
    key        TEXT         NOT NULL,
    old_value  JSONB,
    new_value  JSONB,
    revision   BIGINT       NOT NULL,
    actor_id   TEXT         NOT NULL DEFAULT '',
    changed_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    source     TEXT         NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_%s_target_key
    ON %s.%s (kind, scope, subject, key);

CREATE INDEX IF NOT EXISTS idx_%s_changed_at
    ON %s.%s (changed_at DESC)`

const revisionsDDLTemplate = `CREATE TABLE IF NOT EXISTS %s.%s (
    kind       TEXT        NOT NULL,
    scope      TEXT        NOT NULL,
    subject    TEXT        NOT NULL DEFAULT '',
    revision   BIGINT      NOT NULL DEFAULT 0,
    apply_behavior TEXT     NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by TEXT        NOT NULL DEFAULT '',
    source     TEXT        NOT NULL DEFAULT '',
    PRIMARY KEY (kind, scope, subject)
 );

 ALTER TABLE %s.%s ADD COLUMN IF NOT EXISTS apply_behavior TEXT NOT NULL DEFAULT ''`

// FormatSchemaDDL returns the DDL to create the schema if it does not exist.
func FormatSchemaDDL(schema string) string {
	return fmt.Sprintf(schemaDDLTemplate, schema)
}

// FormatEntriesDDL returns the DDL to create the runtime_entries table and its
// target index.
func FormatEntriesDDL(schema, table string) string {
	return fmt.Sprintf(entriesDDLTemplate,
		schema, table, // CREATE TABLE
		table,         // index name
		schema, table, // ON clause
	)
}

// FormatHistoryDDL returns the DDL to create the runtime_history table and its
// indexes.
func FormatHistoryDDL(schema, table string) string {
	return fmt.Sprintf(historyDDLTemplate,
		schema, table, // CREATE TABLE
		table,         // target_key index name
		schema, table, // target_key ON clause
		table,         // changed_at index name
		schema, table, // changed_at ON clause
	)
}

// FormatRevisionsDDL returns the DDL to create the runtime_revisions table.
func FormatRevisionsDDL(schema, table string) string {
	return fmt.Sprintf(revisionsDDLTemplate, schema, table, schema, table)
}
