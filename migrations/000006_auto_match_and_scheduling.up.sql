-- Add auto_match_on_upload flag to reconciliation_contexts
ALTER TABLE reconciliation_contexts ADD COLUMN IF NOT EXISTS auto_match_on_upload BOOLEAN NOT NULL DEFAULT FALSE;

-- Create reconciliation_schedules table for cron-based scheduling
CREATE TABLE IF NOT EXISTS reconciliation_schedules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    context_id UUID NOT NULL,
    cron_expression VARCHAR(100) NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    last_run_at TIMESTAMP WITH TIME ZONE,
    next_run_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_schedules_context FOREIGN KEY (context_id) REFERENCES reconciliation_contexts(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_schedules_context_id ON reconciliation_schedules(context_id);
CREATE INDEX IF NOT EXISTS idx_schedules_next_run ON reconciliation_schedules(next_run_at) WHERE enabled = TRUE;
