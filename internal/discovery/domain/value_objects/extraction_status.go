// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package value_objects

import (
	"errors"
	"fmt"
	"strings"
)

// ErrInvalidExtractionStatus indicates an invalid extraction status value.
var ErrInvalidExtractionStatus = errors.New("invalid extraction status")

// ExtractionStatus represents the lifecycle state of a data extraction request.
// @Description Lifecycle status of a data extraction job
// @Enum PENDING,SUBMITTED,EXTRACTING,COMPLETE,FAILED,CANCELLED
type ExtractionStatus string

const (
	// ExtractionStatusPending indicates the extraction request has been created but not yet submitted.
	ExtractionStatusPending ExtractionStatus = "PENDING"
	// ExtractionStatusSubmitted indicates the extraction request has been sent to the Fetcher service.
	ExtractionStatusSubmitted ExtractionStatus = "SUBMITTED"
	// ExtractionStatusExtracting indicates the Fetcher is actively extracting data.
	ExtractionStatusExtracting ExtractionStatus = "EXTRACTING"
	// ExtractionStatusComplete indicates the extraction finished successfully.
	ExtractionStatusComplete ExtractionStatus = "COMPLETE"
	// ExtractionStatusFailed indicates the extraction encountered an error.
	ExtractionStatusFailed ExtractionStatus = "FAILED"
	// ExtractionStatusCancelled indicates the extraction was cancelled by the user.
	ExtractionStatusCancelled ExtractionStatus = "CANCELLED"
)

// IsValid reports whether the extraction status is supported.
func (es ExtractionStatus) IsValid() bool {
	switch es {
	case ExtractionStatusPending, ExtractionStatusSubmitted, ExtractionStatusExtracting,
		ExtractionStatusComplete, ExtractionStatusFailed, ExtractionStatusCancelled:
		return true
	}

	return false
}

// Valid is an alias for IsValid, preserved for backward compatibility.
func (es ExtractionStatus) Valid() bool {
	return es.IsValid()
}

// IsTerminal reports whether the extraction status represents a final state.
func (es ExtractionStatus) IsTerminal() bool {
	return es == ExtractionStatusComplete || es == ExtractionStatusFailed || es == ExtractionStatusCancelled
}

// String returns the string representation.
func (es ExtractionStatus) String() string {
	return string(es)
}

// ParseExtractionStatus parses a string into an ExtractionStatus.
func ParseExtractionStatus(s string) (ExtractionStatus, error) {
	es := ExtractionStatus(strings.ToUpper(s))
	if !es.IsValid() {
		return "", fmt.Errorf("%w: %s", ErrInvalidExtractionStatus, s)
	}

	return es, nil
}
