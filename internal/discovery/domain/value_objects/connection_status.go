// Package value_objects provides value types for the discovery bounded context.
package value_objects

import (
	"errors"
	"fmt"
	"strings"
)

// ErrInvalidConnectionStatus indicates an invalid connection status value.
var ErrInvalidConnectionStatus = errors.New("invalid connection status")

// ConnectionStatus represents the health state of a Fetcher connection.
// @Description Health status of a discovered database connection
// @Enum AVAILABLE,UNREACHABLE,UNKNOWN
type ConnectionStatus string

const (
	// ConnectionStatusAvailable indicates the connection is healthy and usable.
	ConnectionStatusAvailable ConnectionStatus = "AVAILABLE"
	// ConnectionStatusUnreachable indicates the connection could not be reached.
	ConnectionStatusUnreachable ConnectionStatus = "UNREACHABLE"
	// ConnectionStatusUnknown indicates the connection status has not been determined.
	ConnectionStatusUnknown ConnectionStatus = "UNKNOWN"
)

// IsValid reports whether the connection status is supported.
func (cs ConnectionStatus) IsValid() bool {
	switch cs {
	case ConnectionStatusAvailable, ConnectionStatusUnreachable, ConnectionStatusUnknown:
		return true
	}

	return false
}

// Valid is an alias for IsValid, preserved for backward compatibility.
func (cs ConnectionStatus) Valid() bool {
	return cs.IsValid()
}

// String returns the string representation.
func (cs ConnectionStatus) String() string {
	return string(cs)
}

// ParseConnectionStatus parses a string into a ConnectionStatus.
func ParseConnectionStatus(s string) (ConnectionStatus, error) {
	cs := ConnectionStatus(strings.ToUpper(s))
	if !cs.IsValid() {
		return "", fmt.Errorf("%w: %s", ErrInvalidConnectionStatus, s)
	}

	return cs, nil
}
