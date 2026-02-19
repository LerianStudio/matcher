-- Add DRAFT and ARCHIVED statuses to reconciliation_context_status enum.
-- DRAFT is the initial status for newly created contexts before activation.
-- ARCHIVED indicates the context is retired and immutable.
-- State machine: DRAFT -> ACTIVE <-> PAUSED -> ARCHIVED
ALTER TYPE reconciliation_context_status ADD VALUE IF NOT EXISTS 'DRAFT' BEFORE 'ACTIVE';
ALTER TYPE reconciliation_context_status ADD VALUE IF NOT EXISTS 'ARCHIVED' AFTER 'PAUSED';

-- Update the default status for new contexts from ACTIVE to DRAFT.
ALTER TABLE reconciliation_contexts ALTER COLUMN status SET DEFAULT 'DRAFT';
