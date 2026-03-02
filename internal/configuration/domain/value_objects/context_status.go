package value_objects

import (
	"errors"
	"fmt"
	"strings"
)

// ErrInvalidContextStatus indicates an invalid context status value.
var ErrInvalidContextStatus = errors.New("invalid context status")

// ContextStatus defines the lifecycle status of a reconciliation context.
//
// State machine transitions (enforced by domain entity methods):
//
//	#  From       Trigger       To
//	-  ------     ----------    ----------
//	1  DRAFT      Activate()    ACTIVE
//	2  ACTIVE     Pause()       PAUSED
//	3  (same)     Archive()     ARCHIVED
//	4  PAUSED     Activate()    ACTIVE      (recovery path -- must never be blocked by verifiers)
//	5  (same)     Archive()     ARCHIVED
//	6  ARCHIVED   --            --          terminal state, no outbound transitions
//
// SECURITY NOTE: The PAUSED->ACTIVE transition is the recovery path that prevents
// irrecoverable contexts. The configuration verifier intentionally does NOT enforce
// active-status checks to keep this path open. Only matching/ingestion/reporting
// verifiers gate on active status.
//
// @Description Lifecycle status of a reconciliation context
// @Enum DRAFT,ACTIVE,PAUSED,ARCHIVED
// swagger:enum ContextStatus
type ContextStatus string

// Supported context status values.
const (
	// ContextStatusDraft indicates the context is in draft and not yet active.
	ContextStatusDraft ContextStatus = "DRAFT"
	// ContextStatusActive indicates the context is active.
	ContextStatusActive ContextStatus = "ACTIVE"
	// ContextStatusPaused indicates the context is paused.
	ContextStatusPaused ContextStatus = "PAUSED"
	// ContextStatusArchived indicates the context is archived and immutable.
	ContextStatusArchived ContextStatus = "ARCHIVED"
)

// Valid reports whether the context status is supported.
func (cs ContextStatus) Valid() bool {
	switch cs {
	case ContextStatusDraft, ContextStatusActive, ContextStatusPaused, ContextStatusArchived:
		return true
	}

	return false
}

// IsValid reports whether the context status is supported.
// This is an alias for Valid() to maintain API consistency.
func (cs ContextStatus) IsValid() bool {
	return cs.Valid()
}

// String returns the string representation of the context status.
func (cs ContextStatus) String() string {
	return string(cs)
}

// ParseContextStatus parses a string into a ContextStatus.
func ParseContextStatus(s string) (ContextStatus, error) {
	cs := ContextStatus(strings.ToUpper(s))
	if !cs.Valid() {
		return "", fmt.Errorf("%w: %s", ErrInvalidContextStatus, s)
	}

	return cs, nil
}
