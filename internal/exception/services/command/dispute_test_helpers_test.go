//go:build unit

package command

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/exception/domain/dispute"
	"github.com/LerianStudio/matcher/internal/exception/domain/repositories"
)

var (
	errTestDisputeCreate = errTestUpdate
	errTestDisputeFind   = errTestFind
	errTestDisputeUpdate = errTestUpdate
)

type stubDisputeRepo struct {
	dispute    *dispute.Dispute
	findErr    error
	createErr  error
	updateErr  error
	listResult []*dispute.Dispute
	listErr    error
}

func (repo *stubDisputeRepo) Create(
	_ context.Context,
	d *dispute.Dispute,
) (*dispute.Dispute, error) {
	if repo.createErr != nil {
		return nil, repo.createErr
	}

	return d, nil
}

func (repo *stubDisputeRepo) CreateWithTx(
	ctx context.Context,
	_ repositories.Tx,
	d *dispute.Dispute,
) (*dispute.Dispute, error) {
	return repo.Create(ctx, d)
}

func (repo *stubDisputeRepo) FindByID(_ context.Context, _ uuid.UUID) (*dispute.Dispute, error) {
	if repo.findErr != nil {
		return nil, repo.findErr
	}

	return repo.dispute, nil
}

func (repo *stubDisputeRepo) FindByExceptionID(
	_ context.Context,
	_ uuid.UUID,
) (*dispute.Dispute, error) {
	if repo.findErr != nil {
		return nil, repo.findErr
	}

	return repo.dispute, nil
}

func (repo *stubDisputeRepo) List(
	_ context.Context,
	_ repositories.DisputeFilter,
	_ repositories.CursorFilter,
) ([]*dispute.Dispute, libHTTP.CursorPagination, error) {
	if repo.listErr != nil {
		return nil, libHTTP.CursorPagination{}, repo.listErr
	}

	return repo.listResult, libHTTP.CursorPagination{}, nil
}

func (repo *stubDisputeRepo) Update(
	_ context.Context,
	d *dispute.Dispute,
) (*dispute.Dispute, error) {
	if repo.updateErr != nil {
		return nil, repo.updateErr
	}

	return d, nil
}

func (repo *stubDisputeRepo) UpdateWithTx(
	ctx context.Context,
	_ repositories.Tx,
	d *dispute.Dispute,
) (*dispute.Dispute, error) {
	return repo.Update(ctx, d)
}

func createTestDispute(ctx context.Context, tb testing.TB, exceptionID uuid.UUID) *dispute.Dispute {
	tb.Helper()

	testDispute, err := dispute.NewDispute(
		ctx,
		exceptionID,
		dispute.DisputeCategoryBankFeeError,
		"test description",
		"analyst-1",
	)
	require.NoError(tb, err)

	err = testDispute.Open(ctx)
	require.NoError(tb, err)

	return testDispute
}
