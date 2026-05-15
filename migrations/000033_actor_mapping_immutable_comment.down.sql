-- Reverts the COMMENT on actor_mapping to the pre-fix description that
-- characterized the table as mutable. The underlying SQL behavior is
-- governed by application-layer code (see ActorMappingRepository.Upsert),
-- not by the COMMENT, so this rollback is purely documentation.
COMMENT ON TABLE actor_mapping IS 'GDPR-compliant actor identity mapping. Mutable: supports pseudonymization (UPDATE) and right-to-erasure (DELETE)';
