// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package query provides read operations for reporting.
package query

import (
	"errors"

	"github.com/LerianStudio/matcher/internal/reporting/domain/repositories"
)

// ErrNilReportRepository is returned when a nil repository is provided.
var ErrNilReportRepository = errors.New("report repository is required")

// ErrStreamingNotSupported is returned when streaming is requested but not available.
var ErrStreamingNotSupported = errors.New("streaming not supported by repository")

// MaxExportRecords defines the maximum number of records allowed in a single export.
// This prevents OOM errors when exporting large datasets.
const MaxExportRecords = 100000

// MaxPDFExportRecords defines a lower limit for PDF exports, which are more memory-intensive
// than CSV due to in-memory layout rendering. This reduces OOM risk for concurrent PDF exports.
const MaxPDFExportRecords = 25000

// UseCase orchestrates report queries and export generation.
type UseCase struct {
	repo          repositories.ReportRepository
	streamingRepo repositories.StreamingReportRepository
}

// NewUseCase creates a new query use case with the required repository.
func NewUseCase(repo repositories.ReportRepository) (*UseCase, error) {
	if repo == nil {
		return nil, ErrNilReportRepository
	}

	uc := &UseCase{repo: repo}

	if streamingRepo, ok := repo.(repositories.StreamingReportRepository); ok {
		uc.streamingRepo = streamingRepo
	}

	return uc, nil
}

// SupportsStreaming returns true if the repository supports streaming exports.
func (uc *UseCase) SupportsStreaming() bool {
	return uc.streamingRepo != nil
}
