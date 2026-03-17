// Copyright 2025 Lerian Studio.

package domain

import "fmt"

// Target identifies the kind/scope/subject combination for a configuration
// operation. SubjectID is empty for global scope and must contain the tenant
// UUID for tenant-scoped entries.
type Target struct {
	Kind      Kind
	Scope     Scope
	SubjectID string
}

// NewTarget creates a validated Target.
//
// Validation rules:
//   - Kind must be valid.
//   - Scope must be valid.
//   - ScopeTenant requires a non-empty SubjectID.
//   - ScopeGlobal requires an empty SubjectID.
func NewTarget(kind Kind, scope Scope, subjectID string) (Target, error) {
	if !kind.IsValid() {
		return Target{}, fmt.Errorf("target kind %q: %w", kind, ErrInvalidKind)
	}

	if !scope.IsValid() {
		return Target{}, fmt.Errorf("target scope %q: %w", scope, ErrInvalidScope)
	}

	if scope == ScopeTenant && subjectID == "" {
		return Target{}, fmt.Errorf("tenant scope requires subject id: %w", ErrScopeInvalid)
	}

	if scope == ScopeGlobal && subjectID != "" {
		return Target{}, fmt.Errorf("global scope must not have subject id: %w", ErrScopeInvalid)
	}

	return Target{
		Kind:      kind,
		Scope:     scope,
		SubjectID: subjectID,
	}, nil
}

// String returns a human-readable representation of the target.
// Global targets format as "config/global"; tenant targets as
// "setting/tenant/<id>".
func (t Target) String() string {
	if t.SubjectID == "" {
		return fmt.Sprintf("%s/%s", t.Kind, t.Scope)
	}

	return fmt.Sprintf("%s/%s/%s", t.Kind, t.Scope, t.SubjectID)
}
