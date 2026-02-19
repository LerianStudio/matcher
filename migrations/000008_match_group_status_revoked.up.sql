-- Add REVOKED status to match_group_status enum.
-- REVOKED means the group was previously confirmed, then undone.
-- This is distinct from REJECTED, which means the group was never confirmed.
ALTER TYPE match_group_status ADD VALUE IF NOT EXISTS 'REVOKED';
