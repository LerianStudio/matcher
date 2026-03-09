//go:build unit

package repositories

import (
	"context"
	"testing"

	"github.com/google/uuid"

	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestTransactionRepositoryInterfaceCompiles(t *testing.T) {
	t.Parallel()

	// This test verifies the interface compiles correctly.
	// The actual implementation is tested via integration tests.
	var _ TransactionRepository = (*mockTransactionRepository)(nil)
}

// mockTransactionRepository is a minimal mock to verify the interface.
type mockTransactionRepository struct{}

func (m *mockTransactionRepository) Create(
	_ context.Context,
	_ *shared.Transaction,
) (*shared.Transaction, error) {
	return nil, nil
}

func (m *mockTransactionRepository) CreateBatch(
	_ context.Context,
	_ []*shared.Transaction,
) ([]*shared.Transaction, error) {
	return nil, nil
}

func (m *mockTransactionRepository) FindByID(
	_ context.Context,
	_ uuid.UUID,
) (*shared.Transaction, error) {
	return nil, nil
}

func (m *mockTransactionRepository) FindByJobID(
	_ context.Context,
	_ uuid.UUID,
	_ CursorFilter,
) ([]*shared.Transaction, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (m *mockTransactionRepository) FindByJobAndContextID(
	_ context.Context,
	_, _ uuid.UUID,
	_ CursorFilter,
) ([]*shared.Transaction, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (m *mockTransactionRepository) FindBySourceAndExternalID(
	_ context.Context,
	_ uuid.UUID,
	_ string,
) (*shared.Transaction, error) {
	return nil, nil
}

func (m *mockTransactionRepository) ExistsBySourceAndExternalID(
	_ context.Context,
	_ uuid.UUID,
	_ string,
) (bool, error) {
	return false, nil
}

func (m *mockTransactionRepository) ExistsBulkBySourceAndExternalID(
	_ context.Context,
	_ []ExternalIDKey,
) (map[ExternalIDKey]bool, error) {
	return make(map[ExternalIDKey]bool), nil
}

func (m *mockTransactionRepository) UpdateStatus(
	_ context.Context,
	_, _ uuid.UUID,
	_ shared.TransactionStatus,
) (*shared.Transaction, error) {
	return nil, nil
}

func (m *mockTransactionRepository) SearchTransactions(
	_ context.Context,
	_ uuid.UUID,
	_ TransactionSearchParams,
) ([]*shared.Transaction, int64, error) {
	return nil, 0, nil
}
