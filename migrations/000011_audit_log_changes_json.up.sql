-- Migrate audit_logs.changes from JSONB to JSON to preserve exact byte
-- representation for hash chain verification.
--
-- JSONB normalizes whitespace and key ordering, which means the stored bytes
-- differ from the original input. Since the hash chain computes SHA-256 over
-- the raw JSON bytes, JSONB normalization makes VerifyRecordHash/VerifyChain
-- produce different hashes on read-back than what was computed at write time.
--
-- JSON preserves exact input bytes, ensuring hash verification works after
-- a database round-trip.
--
-- NOTE: Existing records' per-record hashes were computed from pre-JSONB bytes
-- that are no longer recoverable. Chain linkage (prev_hash → record_hash) remains
-- intact and verifiable. Only per-record hash recomputation is affected for
-- historical rows.

ALTER TABLE audit_logs ALTER COLUMN changes TYPE JSON USING changes::TEXT::JSON;
