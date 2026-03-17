// Copyright 2025 Lerian Studio.

package domain

import (
	"errors"
	"fmt"
	"strings"
)

// BackendKind identifies the storage backend used by the systemplane.
type BackendKind string

// Supported BackendKind values.
const (
	BackendPostgres BackendKind = "postgres"
	BackendMongoDB  BackendKind = "mongodb"
)

// ErrInvalidBackendKind indicates an invalid backend kind value.
var ErrInvalidBackendKind = errors.New("invalid backend kind")

// IsValid reports whether the backend kind is supported.
func (bk BackendKind) IsValid() bool {
	switch bk {
	case BackendPostgres, BackendMongoDB:
		return true
	}

	return false
}

// String returns the string representation of the backend kind.
func (bk BackendKind) String() string {
	return string(bk)
}

// ParseBackendKind parses a string into a BackendKind (case-insensitive).
func ParseBackendKind(s string) (BackendKind, error) {
	bk := BackendKind(strings.ToLower(strings.TrimSpace(s)))
	if !bk.IsValid() {
		return "", fmt.Errorf("parse %s: %w", s, ErrInvalidBackendKind)
	}

	return bk, nil
}
