package value_objects

import "errors"

// ErrInvalidMatchRunStatus is returned when parsing an invalid match run status.
var ErrInvalidMatchRunStatus = errors.New("invalid match run status")

// MatchRunStatus represents lifecycle status for a match run.
// @Description Lifecycle status of a matching run
// @Enum PROCESSING,COMPLETED,FAILED
// swagger:enum MatchRunStatus
type MatchRunStatus string

// MatchRunStatus values.
const (
	MatchRunStatusProcessing MatchRunStatus = "PROCESSING"
	MatchRunStatusCompleted  MatchRunStatus = "COMPLETED"
	MatchRunStatusFailed     MatchRunStatus = "FAILED"
)

// IsValid checks if the status is a valid match run status.
func (s MatchRunStatus) IsValid() bool {
	switch s {
	case MatchRunStatusProcessing, MatchRunStatusCompleted, MatchRunStatusFailed:
		return true
	default:
		return false
	}
}

// String returns the string representation of the status.
func (s MatchRunStatus) String() string {
	return string(s)
}

// ParseMatchRunStatus parses a string into a MatchRunStatus.
func ParseMatchRunStatus(value string) (MatchRunStatus, error) {
	status := MatchRunStatus(value)
	if !status.IsValid() {
		return "", ErrInvalidMatchRunStatus
	}

	return status, nil
}
