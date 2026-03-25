//go:build unit

package cross

import (
	"context"
	"testing"

	"github.com/google/uuid"

	ingestionEntities "github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

type transactionFinderStub struct {
	transaction *shared.Transaction
	err         error
}

func (stub *transactionFinderStub) FindByID(_ context.Context, _ uuid.UUID) (*shared.Transaction, error) {
	return stub.transaction, stub.err
}

type jobFinderStub struct {
	job *ingestionEntities.IngestionJob
	err error
}

func (stub *jobFinderStub) FindByID(_ context.Context, _ uuid.UUID) (*ingestionEntities.IngestionJob, error) {
	return stub.job, stub.err
}

func TestNewTransactionContextLookup_ValidDependencies(t *testing.T) {
	t.Parallel()

	lookup, err := NewTransactionContextLookup(&transactionFinderStub{}, &jobFinderStub{}, nil)
	if err != nil {
		t.Fatalf("expected constructor to succeed: %v", err)
	}

	if lookup == nil {
		t.Fatal("expected transaction context lookup instance")
	}
}

func TestTransactionContextLookup_GetContextIDByTransactionID_ResolvesViaJob(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	transactionID := uuid.New()
	ingestionJobID := uuid.New()
	contextID := uuid.New()

	lookup, err := NewTransactionContextLookup(
		&transactionFinderStub{transaction: &shared.Transaction{ID: transactionID, IngestionJobID: ingestionJobID}},
		&jobFinderStub{job: &ingestionEntities.IngestionJob{ID: ingestionJobID, ContextID: contextID}},
		nil,
	)
	if err != nil {
		t.Fatalf("expected constructor to succeed: %v", err)
	}

	resolvedContextID, resolveErr := lookup.GetContextIDByTransactionID(ctx, transactionID)
	if resolveErr != nil {
		t.Fatalf("expected context id resolution to succeed: %v", resolveErr)
	}

	if resolvedContextID != contextID {
		t.Fatalf("expected context %s, got %s", contextID, resolvedContextID)
	}
}
