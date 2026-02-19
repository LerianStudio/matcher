-- Add PENDING_RESOLUTION status to exception_status enum.
-- This intermediate status guards against re-processing while an external
-- gateway call (force-match, adjust-entry) is in progress.
ALTER TYPE exception_status ADD VALUE IF NOT EXISTS 'PENDING_RESOLUTION' BEFORE 'RESOLVED';
