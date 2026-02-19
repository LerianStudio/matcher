package value_objects

import (
	"errors"
	"fmt"
	"strconv"
)

// ErrConfidenceScoreOutOfRange is returned when a confidence score is outside 0-100.
var ErrConfidenceScoreOutOfRange = errors.New("confidence score out of range")

// ConfidenceScore represents a bounded confidence score between 0 and 100.
// Serializes as an integer in JSON (0-100).
//
// @Description Match confidence score ranging from 0 to 100
type ConfidenceScore struct {
	value int
}

// ParseConfidenceScore creates a ConfidenceScore from an integer value.
func ParseConfidenceScore(value int) (ConfidenceScore, error) {
	if value < 0 || value > 100 {
		return ConfidenceScore{}, ErrConfidenceScoreOutOfRange
	}

	return ConfidenceScore{value: value}, nil
}

// Value returns the numeric confidence score.
func (c ConfidenceScore) Value() int {
	return c.value
}

// IsValid returns true if the confidence score is within the valid range (0-100).
// A zero-value ConfidenceScore (value=0) is considered valid as 0 is a valid score.
func (c ConfidenceScore) IsValid() bool {
	return c.value >= 0 && c.value <= 100
}

// MarshalJSON implements json.Marshaler for ConfidenceScore.
func (c ConfidenceScore) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Itoa(c.value)), nil
}

// UnmarshalJSON implements json.Unmarshaler for ConfidenceScore.
//
//nolint:varnamelen // single-letter receiver is idiomatic Go
func (c *ConfidenceScore) UnmarshalJSON(data []byte) error {
	s := string(data)

	value, err := strconv.Atoi(s)
	if err != nil {
		return fmt.Errorf("failed to parse confidence score: %w", err)
	}

	if value < 0 || value > 100 {
		return ErrConfidenceScoreOutOfRange
	}

	c.value = value

	return nil
}
