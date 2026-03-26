-- Migration: Rename systemplane keys for cross-product naming standardization.
--
-- Key renames:
--   postgres.max_open_connections -> postgres.max_open_conns    (Go convention)
--   postgres.max_idle_connections -> postgres.max_idle_conns    (Go convention)
--   redis.min_idle_conn           -> redis.min_idle_conns       (plural consistency)
--   rabbitmq.uri                  -> rabbitmq.url               (cross-product standard)
--   server.cors_allowed_origins   -> cors.allowed_origins       (dedicated namespace)
--   server.cors_allowed_methods   -> cors.allowed_methods       (dedicated namespace)
--   server.cors_allowed_headers   -> cors.allowed_headers       (dedicated namespace)

BEGIN;

CREATE SCHEMA IF NOT EXISTS system;

CREATE TABLE IF NOT EXISTS system.runtime_entries (
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

CREATE INDEX IF NOT EXISTS idx_runtime_entries_target
    ON system.runtime_entries (kind, scope, subject);

CREATE TABLE IF NOT EXISTS system.runtime_history (
    id         BIGSERIAL   PRIMARY KEY,
    kind       TEXT        NOT NULL,
    scope      TEXT        NOT NULL,
    subject    TEXT        NOT NULL DEFAULT '',
    key        TEXT        NOT NULL,
    old_value  JSONB,
    new_value  JSONB,
    revision   BIGINT      NOT NULL,
    actor_id   TEXT        NOT NULL DEFAULT '',
    changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source     TEXT        NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_runtime_history_target_key
    ON system.runtime_history (kind, scope, subject, key);

CREATE INDEX IF NOT EXISTS idx_runtime_history_changed_at
    ON system.runtime_history (changed_at DESC);

SELECT CASE
    WHEN EXISTS (
        SELECT 1
        FROM (VALUES
            ('postgres.max_open_connections', 'postgres.max_open_conns'),
            ('postgres.max_idle_connections', 'postgres.max_idle_conns'),
            ('redis.min_idle_conn', 'redis.min_idle_conns'),
            ('rabbitmq.uri', 'rabbitmq.url'),
            ('server.cors_allowed_origins', 'cors.allowed_origins'),
            ('server.cors_allowed_methods', 'cors.allowed_methods'),
            ('server.cors_allowed_headers', 'cors.allowed_headers')
        ) AS renames(old_key, new_key)
        JOIN system.runtime_entries old_entries
          ON old_entries.kind = 'config'
         AND old_entries.scope = 'global'
         AND old_entries.subject = ''
         AND old_entries.key = renames.old_key
        JOIN system.runtime_entries new_entries
          ON new_entries.kind = old_entries.kind
         AND new_entries.scope = old_entries.scope
         AND new_entries.subject = old_entries.subject
         AND new_entries.key = renames.new_key
    ) THEN current_setting(
        'migration_000020_blocked_resolve_systemplane_key_rename_collisions_before_apply'
    )
    ELSE 'ok'
END;

UPDATE system.runtime_entries AS entries
SET key = renames.new_key
FROM (VALUES
    ('postgres.max_open_connections', 'postgres.max_open_conns'),
    ('postgres.max_idle_connections', 'postgres.max_idle_conns'),
    ('redis.min_idle_conn', 'redis.min_idle_conns'),
    ('rabbitmq.uri', 'rabbitmq.url'),
    ('server.cors_allowed_origins', 'cors.allowed_origins'),
    ('server.cors_allowed_methods', 'cors.allowed_methods'),
    ('server.cors_allowed_headers', 'cors.allowed_headers')
) AS renames(old_key, new_key)
WHERE entries.kind = 'config'
  AND entries.scope = 'global'
  AND entries.subject = ''
  AND entries.key = renames.old_key;

UPDATE system.runtime_history AS history
SET key = renames.new_key
FROM (VALUES
    ('postgres.max_open_connections', 'postgres.max_open_conns'),
    ('postgres.max_idle_connections', 'postgres.max_idle_conns'),
    ('redis.min_idle_conn', 'redis.min_idle_conns'),
    ('rabbitmq.uri', 'rabbitmq.url'),
    ('server.cors_allowed_origins', 'cors.allowed_origins'),
    ('server.cors_allowed_methods', 'cors.allowed_methods'),
    ('server.cors_allowed_headers', 'cors.allowed_headers')
) AS renames(old_key, new_key)
WHERE history.kind = 'config'
  AND history.scope = 'global'
  AND history.subject = ''
  AND history.key = renames.old_key;

COMMIT;
