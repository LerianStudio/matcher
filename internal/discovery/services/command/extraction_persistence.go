// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package command

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
)

func (uc *UseCase) persistSubmittedExtraction(
	ctx context.Context,
	span trace.Span,
	extractionReq *entities.ExtractionRequest,
) error {
	err := uc.extractionRepo.Update(ctx, extractionReq)
	if err == nil {
		return nil
	}

	libOpentelemetry.HandleSpanError(span, "persist submitted extraction request", err)

	recovered, recoverErr := uc.recoverSubmittedExtraction(ctx, extractionReq)
	if recoverErr == nil {
		if recovered != nil && recovered != extractionReq {
			*extractionReq = *recovered
		}

		return nil
	}

	libOpentelemetry.HandleSpanError(span, "recover submitted extraction request", recoverErr)

	return fmt.Errorf("%w: extraction %s: %w", ErrExtractionTrackingIncomplete, extractionReq.ID, recoverErr)
}

func (uc *UseCase) recoverSubmittedExtraction(
	ctx context.Context,
	submitted *entities.ExtractionRequest,
) (*entities.ExtractionRequest, error) {
	latest, err := uc.extractionRepo.FindByID(ctx, submitted.ID)
	if err != nil {
		if errors.Is(err, repositories.ErrExtractionNotFound) {
			return nil, ErrExtractionNotFound
		}

		return nil, fmt.Errorf("reload extraction request: %w", err)
	}

	if latest == nil {
		return nil, ErrExtractionNotFound
	}

	if latest.Status == submitted.Status && latest.FetcherJobID == submitted.FetcherJobID {
		return latest, nil
	}

	if strings.TrimSpace(latest.FetcherJobID) != "" {
		return latest, nil
	}

	expectedUpdatedAt := latest.UpdatedAt
	if expectedUpdatedAt.IsZero() {
		expectedUpdatedAt = submitted.CreatedAt
	}

	err = uc.extractionRepo.UpdateIfUnchanged(ctx, submitted, expectedUpdatedAt)
	if err == nil {
		return submitted, nil
	}

	if !errors.Is(err, repositories.ErrExtractionConflict) {
		return nil, fmt.Errorf("repair submitted extraction request: %w", err)
	}

	return uc.reloadRecoveredExtraction(ctx, submitted.ID)
}

func (uc *UseCase) reloadRecoveredExtraction(ctx context.Context, extractionID uuid.UUID) (*entities.ExtractionRequest, error) {
	reloaded, err := uc.extractionRepo.FindByID(ctx, extractionID)
	if err != nil {
		if errors.Is(err, repositories.ErrExtractionNotFound) {
			return nil, ErrExtractionNotFound
		}

		return nil, fmt.Errorf("reload extraction request after repair conflict: %w", err)
	}

	if reloaded == nil {
		return nil, ErrExtractionNotFound
	}

	if strings.TrimSpace(reloaded.FetcherJobID) == "" {
		return nil, fmt.Errorf("repair submitted extraction request: %w", repositories.ErrExtractionConflict)
	}

	return reloaded, nil
}
