-- Make rule_id nullable on match_groups to support manual matches (no rule applied).
ALTER TABLE match_groups ALTER COLUMN rule_id DROP NOT NULL;
