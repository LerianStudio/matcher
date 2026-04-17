// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
)

// Polish Fix 4: the prior `stampExtractionID(job, metadata)` two-step
// helper (parse-from-metadata + apply-to-job) was deleted in favor of
// `canonicalExtractionIDFromMetadata` + `stampExtractionIDOnJob`. The new
// split keeps the metadata-parsing step at the trusted-stream boundary and
// the job-mutation step inside createAndStartJob, where it lands atomically
// with the initial INSERT (no follow-up Update window).

// TestStampExtractionIDOnJob_NilJob is a defensive smoke test on the helper
// used by createAndStartJob (Polish Fix 4).
func TestStampExtractionIDOnJob_NilJob(t *testing.T) {
	t.Parallel()

	// Should not panic.
	stampExtractionIDOnJob(nil, uuid.NewString())
}

// TestStampExtractionIDOnJob_HappyPath asserts the createAndStartJob-side
// helper canonicalizes consistently with the metadata-side helper.
func TestStampExtractionIDOnJob_HappyPath(t *testing.T) {
	t.Parallel()

	id := uuid.New()
	job := &entities.IngestionJob{}

	stampExtractionIDOnJob(job, strings.ToUpper(id.String()))
	assert.Equal(t, id.String(), job.Metadata.ExtractionID)
}

// TestStampExtractionIDOnJob_EmptyOrInvalid_NoOp documents the silent-skip
// invariant: callers can pass empty/invalid strings (e.g., upload path) and
// the helper leaves the job clean rather than panicking.
func TestStampExtractionIDOnJob_EmptyOrInvalid_NoOp(t *testing.T) {
	t.Parallel()

	job := &entities.IngestionJob{}
	stampExtractionIDOnJob(job, "")
	assert.Empty(t, job.Metadata.ExtractionID)

	stampExtractionIDOnJob(job, "   ")
	assert.Empty(t, job.Metadata.ExtractionID)

	stampExtractionIDOnJob(job, "not-a-uuid")
	assert.Empty(t, job.Metadata.ExtractionID)
}

// TestCanonicalExtractionIDFromMetadata covers the helper that bridges
// SourceMetadata into the canonical string passed via StartIngestionInput
// (Polish Fix 4 + Fix 7). Includes the canonical-casing regression: an
// uppercase UUID input must be lowercased before storage so the downstream
// FindLatestByExtractionID query (which keys on uuid.UUID.String() — always
// lowercase) does not miss.
func TestCanonicalExtractionIDFromMetadata(t *testing.T) {
	t.Parallel()

	id := uuid.New()

	tests := []struct {
		name string
		md   map[string]string
		want string
	}{
		{"nil metadata", nil, ""},
		{"empty metadata", map[string]string{}, ""},
		{"missing key", map[string]string{"filename": "x"}, ""},
		{"blank value", map[string]string{"extraction_id": "   "}, ""},
		{"invalid uuid", map[string]string{"extraction_id": "not-a-uuid"}, ""},
		{"happy path lowercase", map[string]string{"extraction_id": id.String()}, id.String()},
		{"uppercase canonicalized", map[string]string{"extraction_id": strings.ToUpper(id.String())}, id.String()},
		{"trims whitespace", map[string]string{"extraction_id": "  " + id.String() + "  "}, id.String()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, canonicalExtractionIDFromMetadata(context.Background(), tt.md))
		})
	}
}
