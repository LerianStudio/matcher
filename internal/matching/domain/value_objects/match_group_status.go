// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package value_objects

import "errors"

// ErrInvalidMatchGroupStatus is returned when parsing an invalid match group status.
var ErrInvalidMatchGroupStatus = errors.New("invalid match group status")

// MatchGroupStatus represents lifecycle status for a match group.
// @Description Lifecycle status of a match group
// @Enum PROPOSED,CONFIRMED,REJECTED,REVOKED
// swagger:enum MatchGroupStatus
type MatchGroupStatus string

// MatchGroupStatus values.
const (
	MatchGroupStatusProposed  MatchGroupStatus = "PROPOSED"
	MatchGroupStatusConfirmed MatchGroupStatus = "CONFIRMED"
	MatchGroupStatusRejected  MatchGroupStatus = "REJECTED"
	MatchGroupStatusRevoked   MatchGroupStatus = "REVOKED"
)

// IsValid checks if the status is a valid match group status.
func (s MatchGroupStatus) IsValid() bool {
	switch s {
	case MatchGroupStatusProposed, MatchGroupStatusConfirmed, MatchGroupStatusRejected, MatchGroupStatusRevoked:
		return true
	default:
		return false
	}
}

// String returns the string representation of the status.
func (s MatchGroupStatus) String() string {
	return string(s)
}

// ParseMatchGroupStatus parses a string into a MatchGroupStatus.
func ParseMatchGroupStatus(value string) (MatchGroupStatus, error) {
	status := MatchGroupStatus(value)
	if !status.IsValid() {
		return "", ErrInvalidMatchGroupStatus
	}

	return status, nil
}
