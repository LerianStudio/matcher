//go:build unit

package ports

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/lib-commons/v5/commons/outbox"
)

func TestOutboxRepositoryInterfaceAlias(t *testing.T) {
	t.Parallel()

	// Verify the type alias resolves to the canonical interface.
	var _ OutboxRepository = (outbox.OutboxRepository)(nil)
}

// TestOutboxRepository_ConformsToLibContract exercises every method of the
// OutboxRepository alias through a zero-behavior stub. This acts as a
// compile-time regression guard: if lib-commons/v5 renames, removes, or
// reshapes any method on OutboxRepository, this test will fail to compile
// and the contract drift is caught before integration tests run.
func TestOutboxRepository_ConformsToLibContract(t *testing.T) {
	t.Parallel()

	var repo OutboxRepository = &fakeOutboxRepo{}

	ctx := context.Background()
	ev := &outbox.OutboxEvent{
		ID:          uuid.New(),
		EventType:   "test",
		AggregateID: uuid.New(),
		Payload:     []byte(`{}`),
		Status:      outbox.OutboxStatusPending,
	}

	// Exercise every method — any missing/renamed method breaks compilation.
	_, _ = repo.Create(ctx, ev)
	_, _ = repo.CreateWithTx(ctx, nil, ev)
	_, _ = repo.ListPending(ctx, 10)
	_, _ = repo.ListPendingByType(ctx, "test", 10)
	_, _ = repo.ListTenants(ctx)
	_, _ = repo.GetByID(ctx, ev.ID)
	_ = repo.MarkPublished(ctx, ev.ID, time.Now().UTC())
	_ = repo.MarkFailed(ctx, ev.ID, "err", 1)
	_, _ = repo.ListFailedForRetry(ctx, 10, time.Now().UTC(), 3)
	_, _ = repo.ResetForRetry(ctx, 10, time.Now().UTC(), 3)
	_, _ = repo.ResetStuckProcessing(ctx, 10, time.Now().UTC(), 3)
	_ = repo.MarkInvalid(ctx, ev.ID, "bad payload")

	require.NotNil(t, repo)
}

// fakeOutboxRepo is a zero-behavior stub implementing OutboxRepository.
// Its sole purpose is compile-time verification that the interface alias
// matches the set of methods matcher code invokes.
type fakeOutboxRepo struct{}

func (f *fakeOutboxRepo) Create(
	_ context.Context,
	_ *outbox.OutboxEvent,
) (*outbox.OutboxEvent, error) {
	return &outbox.OutboxEvent{}, nil
}

func (f *fakeOutboxRepo) CreateWithTx(
	_ context.Context,
	_ outbox.Tx,
	_ *outbox.OutboxEvent,
) (*outbox.OutboxEvent, error) {
	return &outbox.OutboxEvent{}, nil
}

func (f *fakeOutboxRepo) ListPending(
	_ context.Context,
	_ int,
) ([]*outbox.OutboxEvent, error) {
	return nil, nil
}

func (f *fakeOutboxRepo) ListPendingByType(
	_ context.Context,
	_ string,
	_ int,
) ([]*outbox.OutboxEvent, error) {
	return nil, nil
}

func (f *fakeOutboxRepo) ListTenants(_ context.Context) ([]string, error) {
	return nil, nil
}

func (f *fakeOutboxRepo) GetByID(
	_ context.Context,
	_ uuid.UUID,
) (*outbox.OutboxEvent, error) {
	return &outbox.OutboxEvent{}, nil
}

func (f *fakeOutboxRepo) MarkPublished(
	_ context.Context,
	_ uuid.UUID,
	_ time.Time,
) error {
	return nil
}

func (f *fakeOutboxRepo) MarkFailed(
	_ context.Context,
	_ uuid.UUID,
	_ string,
	_ int,
) error {
	return nil
}

func (f *fakeOutboxRepo) ListFailedForRetry(
	_ context.Context,
	_ int,
	_ time.Time,
	_ int,
) ([]*outbox.OutboxEvent, error) {
	return nil, nil
}

func (f *fakeOutboxRepo) ResetForRetry(
	_ context.Context,
	_ int,
	_ time.Time,
	_ int,
) ([]*outbox.OutboxEvent, error) {
	return nil, nil
}

func (f *fakeOutboxRepo) ResetStuckProcessing(
	_ context.Context,
	_ int,
	_ time.Time,
	_ int,
) ([]*outbox.OutboxEvent, error) {
	return nil, nil
}

func (f *fakeOutboxRepo) MarkInvalid(
	_ context.Context,
	_ uuid.UUID,
	_ string,
) error {
	return nil
}
