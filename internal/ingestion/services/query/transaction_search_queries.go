package query

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-uncommons/v2/uncommons"
	libOpentelemetry "github.com/LerianStudio/lib-uncommons/v2/uncommons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/repositories"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// SearchTransactions searches transactions within a context using the given filter parameters.
func (uc *UseCase) SearchTransactions(
	ctx context.Context,
	contextID uuid.UUID,
	params repositories.TransactionSearchParams,
) ([]*shared.Transaction, int64, error) {
	if uc == nil {
		return nil, 0, ErrNilUseCase
	}

	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // only tracer needed

	ctx, span := tracer.Start(ctx, "query.ingestion.search_transactions")
	defer span.End()

	_ = libOpentelemetry.SetSpanAttributesFromValue(span, "query", struct {
		ContextID string `json:"contextId"`
		Query     string `json:"query"`
	}{ContextID: contextID.String(), Query: params.Query}, nil)

	transactions, total, err := uc.transactionRepo.SearchTransactions(ctx, contextID, params)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to search transactions", err)

		return nil, 0, fmt.Errorf("searching transactions: %w", err)
	}

	return transactions, total, nil
}
