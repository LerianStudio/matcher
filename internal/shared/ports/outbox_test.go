//go:build unit

package ports

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"

	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestOutboxRepositoryInterfaceExists(t *testing.T) {
	t.Parallel()

	var _ OutboxRepository = (*mockOutboxRepository)(nil)
}

type mockOutboxRepository struct{}

func (m *mockOutboxRepository) Create(_ context.Context, _ *sharedDomain.OutboxEvent) (*sharedDomain.OutboxEvent, error) {
	return nil, nil
}

func (m *mockOutboxRepository) CreateWithTx(
	_ context.Context,
	_ *sql.Tx,
	_ *sharedDomain.OutboxEvent,
) (*sharedDomain.OutboxEvent, error) {
	return nil, nil
}

func (m *mockOutboxRepository) ListPending(_ context.Context, _ int) ([]*sharedDomain.OutboxEvent, error) {
	return nil, nil
}

func (m *mockOutboxRepository) ListPendingByType(_ context.Context, _ string, _ int) ([]*sharedDomain.OutboxEvent, error) {
	return nil, nil
}

func (m *mockOutboxRepository) ListTenants(_ context.Context) ([]string, error) {
	return nil, nil
}

func (m *mockOutboxRepository) GetByID(_ context.Context, _ uuid.UUID) (*sharedDomain.OutboxEvent, error) {
	return nil, nil
}

func (m *mockOutboxRepository) MarkPublished(_ context.Context, _ uuid.UUID, _ time.Time) error {
	return nil
}

func (m *mockOutboxRepository) MarkFailed(_ context.Context, _ uuid.UUID, _ string, _ int) error {
	return nil
}

func (m *mockOutboxRepository) ListFailedForRetry(
	_ context.Context,
	_ int,
	_ time.Time,
	_ int,
) ([]*sharedDomain.OutboxEvent, error) {
	return nil, nil
}

func (m *mockOutboxRepository) ResetForRetry(
	_ context.Context,
	_ int,
	_ time.Time,
	_ int,
) ([]*sharedDomain.OutboxEvent, error) {
	return nil, nil
}

func (m *mockOutboxRepository) ResetStuckProcessing(
	_ context.Context,
	_ int,
	_ time.Time,
	_ int,
) ([]*sharedDomain.OutboxEvent, error) {
	return nil, nil
}

func (m *mockOutboxRepository) MarkInvalid(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}
