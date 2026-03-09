-- Add FETCHER to the reconciliation_source_type enum.
-- FETCHER sources represent data pulled from Fetcher service connections.
ALTER TYPE reconciliation_source_type ADD VALUE IF NOT EXISTS 'FETCHER';
