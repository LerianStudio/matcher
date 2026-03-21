// Copyright 2025 Lerian Studio.

package domain

import (
	"errors"
	"fmt"
	"strings"
)

// Kind distinguishes configuration entries from user-facing settings.
type Kind string

// Supported Kind values.
const (
	KindConfig  Kind = "config"
	KindSetting Kind = "setting"
)

// ErrInvalidKind indicates an invalid kind value.
var ErrInvalidKind = errors.New("invalid kind")

// IsValid reports whether the kind is supported.
func (k Kind) IsValid() bool {
	switch k {
	case KindConfig, KindSetting:
		return true
	}

	return false
}

// String returns the string representation of the kind.
func (k Kind) String() string {
	return string(k)
}

// ParseKind parses a string into a Kind (case-insensitive).
func ParseKind(s string) (Kind, error) {
	k := Kind(strings.ToLower(strings.TrimSpace(s)))
	if !k.IsValid() {
		return "", fmt.Errorf("parse %s: %w", s, ErrInvalidKind)
	}

	return k, nil
}
