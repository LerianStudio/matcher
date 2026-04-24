// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package value_objects

import "errors"

// ErrInvalidMatchRunMode is returned when parsing an invalid match run mode.
var ErrInvalidMatchRunMode = errors.New("invalid match run mode")

// MatchRunMode defines execution mode for a match run.
// @Description Execution mode for a matching run
// @Enum DRY_RUN,COMMIT
// swagger:enum MatchRunMode
type MatchRunMode string

// MatchRunMode values.
const (
	MatchRunModeDryRun MatchRunMode = "DRY_RUN"
	MatchRunModeCommit MatchRunMode = "COMMIT"
)

// IsValid checks if the mode is a valid match run mode.
func (m MatchRunMode) IsValid() bool {
	switch m {
	case MatchRunModeDryRun, MatchRunModeCommit:
		return true
	default:
		return false
	}
}

// String returns the string representation of the mode.
func (m MatchRunMode) String() string {
	return string(m)
}

// ParseMatchRunMode parses a string into a MatchRunMode.
func ParseMatchRunMode(value string) (MatchRunMode, error) {
	mode := MatchRunMode(value)
	if !mode.IsValid() {
		return "", ErrInvalidMatchRunMode
	}

	return mode, nil
}
