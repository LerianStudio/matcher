// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package value_objects provides value objects for the ingestion domain.
package value_objects

import "errors"

// ErrInvalidJobStatus indicates the job status is invalid.
var ErrInvalidJobStatus = errors.New(
	"invalid job status: must be QUEUED, PROCESSING, COMPLETED, or FAILED",
)

// JobStatus matches PostgreSQL enum `ingestion_job_status` from 000001_init_schema.up.sql
// @Description Lifecycle status of an ingestion job
// @Enum QUEUED,PROCESSING,COMPLETED,FAILED
// swagger:enum JobStatus
type JobStatus string

// JobStatus constants representing valid job states.
const (
	JobStatusQueued     JobStatus = "QUEUED" // Initial state (not PENDING!)
	JobStatusProcessing JobStatus = "PROCESSING"
	JobStatusCompleted  JobStatus = "COMPLETED"
	JobStatusFailed     JobStatus = "FAILED"
)

var validJobStatuses = map[JobStatus]bool{
	JobStatusQueued:     true,
	JobStatusProcessing: true,
	JobStatusCompleted:  true,
	JobStatusFailed:     true,
}

// IsValid returns true if the status is valid.
func (s JobStatus) IsValid() bool {
	return validJobStatuses[s]
}

// String returns the string representation of the status.
func (s JobStatus) String() string {
	return string(s)
}

// ParseJobStatus parses a string into a JobStatus.
func ParseJobStatus(s string) (JobStatus, error) {
	status := JobStatus(s)
	if !status.IsValid() {
		return "", ErrInvalidJobStatus
	}

	return status, nil
}
