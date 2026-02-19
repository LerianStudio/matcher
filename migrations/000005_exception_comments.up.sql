CREATE TABLE IF NOT EXISTS exception_comments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    exception_id UUID NOT NULL,
    author VARCHAR(255) NOT NULL,
    content TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_exception_comments_exception FOREIGN KEY (exception_id) REFERENCES exceptions(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_exception_comments_exception_id ON exception_comments(exception_id);
CREATE INDEX IF NOT EXISTS idx_exception_comments_created_at ON exception_comments(created_at);
