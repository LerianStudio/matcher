// Copyright 2025 Lerian Studio.

package domain

import "time"

// Entry represents a persisted override record in the runtime store.
type Entry struct {
	Kind      Kind
	Scope     Scope
	Subject   string
	Key       string
	Value     any
	Revision  Revision
	UpdatedAt time.Time
	UpdatedBy string
	Source    string
}
