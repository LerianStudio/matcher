package value_objects

import (
	"errors"
	"strings"
)

// ErrInvalidExceptionStatus is returned when parsing an invalid exception status.
var ErrInvalidExceptionStatus = errors.New("invalid exception status")

// ExceptionStatus represents lifecycle status for an exception.
type ExceptionStatus string

// ExceptionStatus values.
const (
	ExceptionStatusOpen              ExceptionStatus = "OPEN"
	ExceptionStatusAssigned          ExceptionStatus = "ASSIGNED"
	ExceptionStatusPendingResolution ExceptionStatus = "PENDING_RESOLUTION"
	ExceptionStatusResolved          ExceptionStatus = "RESOLVED"
)

// IsValid checks if the status is valid.
func (status ExceptionStatus) IsValid() bool {
	switch status {
	case ExceptionStatusOpen,
		ExceptionStatusAssigned,
		ExceptionStatusPendingResolution,
		ExceptionStatusResolved:
		return true
	default:
		return false
	}
}

// String returns the string representation of the status.
func (status ExceptionStatus) String() string {
	return string(status)
}

// ParseExceptionStatus parses a string into an ExceptionStatus.
// Input is normalized to uppercase for case-insensitive parsing.
func ParseExceptionStatus(value string) (ExceptionStatus, error) {
	status := ExceptionStatus(strings.ToUpper(strings.TrimSpace(value)))
	if !status.IsValid() {
		return "", ErrInvalidExceptionStatus
	}

	return status, nil
}
