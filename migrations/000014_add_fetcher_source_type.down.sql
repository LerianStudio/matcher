-- Recreate the enum without FETCHER.
-- This down migration will fail if any reconciliation_sources rows still use FETCHER,
-- which is safer than silently pretending rollback succeeded.
ALTER TYPE reconciliation_source_type RENAME TO reconciliation_source_type_old;

CREATE TYPE reconciliation_source_type AS ENUM ('LEDGER', 'BANK', 'GATEWAY', 'CUSTOM');

ALTER TABLE reconciliation_sources
    ALTER COLUMN type TYPE reconciliation_source_type
    USING type::text::reconciliation_source_type;

DROP TYPE reconciliation_source_type_old;
