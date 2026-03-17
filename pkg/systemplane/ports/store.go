// Copyright 2025 Lerian Studio.

package ports

import (
	"context"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
)

// WriteOp describes a single key mutation in a patch batch.
type WriteOp struct {
	Key   string
	Value any  // nil = reset to default
	Reset bool // explicit reset flag
}

// ReadResult contains the entries and current revision for a target.
type ReadResult struct {
	Entries  []domain.Entry
	Revision domain.Revision
}

// Store is the persistence port for configuration entries.
// Implementations must enforce optimistic concurrency via the expected revision
// parameter. When the caller-provided revision does not match the current
// revision in storage, Put must return domain.ErrRevisionMismatch.
type Store interface {
	// Get retrieves all entries for a target at its current revision.
	Get(ctx context.Context, target domain.Target) (ReadResult, error)

	// Put atomically writes a batch of operations for a target.
	// Returns domain.ErrRevisionMismatch if expected does not match the
	// current revision.
	Put(ctx context.Context, target domain.Target, ops []WriteOp,
		expected domain.Revision, actor domain.Actor, source string) (domain.Revision, error)
}
