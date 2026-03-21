// Copyright 2025 Lerian Studio.

package domain

// Revision represents a monotonically increasing version number for optimistic
// concurrency. Each write to the configuration store bumps the revision, and
// callers that provide a stale revision receive ErrRevisionMismatch.
type Revision uint64

// RevisionZero is the initial revision before any writes have occurred.
const RevisionZero Revision = 0

// Next returns the successor revision.
func (r Revision) Next() Revision { return r + 1 }

// Uint64 returns the underlying numeric value.
func (r Revision) Uint64() uint64 { return uint64(r) }
