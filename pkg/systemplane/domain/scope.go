// Copyright 2025 Lerian Studio.

package domain

import (
	"errors"
	"fmt"
	"strings"
)

// Scope defines the visibility level for a configuration entry.
type Scope string

// Supported Scope values.
const (
	ScopeGlobal Scope = "global"
	ScopeTenant Scope = "tenant"
)

// ErrInvalidScope indicates an invalid scope value.
var ErrInvalidScope = errors.New("invalid scope")

// IsValid reports whether the scope is supported.
func (s Scope) IsValid() bool {
	switch s {
	case ScopeGlobal, ScopeTenant:
		return true
	}

	return false
}

// String returns the string representation of the scope.
func (s Scope) String() string {
	return string(s)
}

// ParseScope parses a string into a Scope (case-insensitive).
func ParseScope(str string) (Scope, error) {
	s := Scope(strings.ToLower(strings.TrimSpace(str)))
	if !s.IsValid() {
		return "", fmt.Errorf("parse %s: %w", str, ErrInvalidScope)
	}

	return s, nil
}
