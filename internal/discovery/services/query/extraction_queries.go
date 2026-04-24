// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package query

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
)

// GetExtraction returns a single extraction request by its internal ID.
func (uc *UseCase) GetExtraction(ctx context.Context, id uuid.UUID) (*entities.ExtractionRequest, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "query.discovery.get_extraction")
	defer span.End()

	req, err := uc.extractionRepo.FindByID(ctx, id)
	if err != nil {
		if errors.Is(err, repositories.ErrExtractionNotFound) {
			libOpentelemetry.HandleSpanBusinessErrorEvent(span, "extraction not found", err)

			return nil, ErrExtractionNotFound
		}

		libOpentelemetry.HandleSpanError(span, "get extraction", err)

		return nil, fmt.Errorf("get extraction: %w", err)
	}

	if req == nil {
		return nil, ErrExtractionNotFound
	}

	return req, nil
}
