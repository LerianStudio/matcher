package query

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

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
		libOpentelemetry.HandleSpanError(span, "get extraction", err)

		if errors.Is(err, repositories.ErrExtractionNotFound) {
			return nil, ErrExtractionNotFound
		}

		return nil, fmt.Errorf("get extraction: %w", err)
	}

	if req == nil {
		return nil, ErrExtractionNotFound
	}

	return req, nil
}
