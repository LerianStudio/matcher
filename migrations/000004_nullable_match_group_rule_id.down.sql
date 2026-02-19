-- Revert: remove manual-match groups that have no rule_id, then re-add NOT NULL.
DELETE FROM match_groups WHERE rule_id IS NULL;
ALTER TABLE match_groups ALTER COLUMN rule_id SET NOT NULL;
