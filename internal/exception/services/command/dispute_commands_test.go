//go:build unit

package command

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDisputeUseCase_Success(t *testing.T) {
	t.Parallel()

	disputeRepo := &stubDisputeRepo{}
	exceptionRepo := &stubExceptionRepo{}
	audit := &stubAuditPublisher{}
	actor := actorExtractor("analyst-1")
	infra := &stubInfraProvider{}

	uc, err := NewExceptionUseCase(exceptionRepo, actor, audit, infra, WithDisputeRepository(disputeRepo))

	require.NoError(t, err)
	require.NotNil(t, uc)
	assert.Equal(t, disputeRepo, uc.disputeRepo)
	assert.Equal(t, exceptionRepo, uc.exceptionRepo)
	assert.Equal(t, audit, uc.auditPublisher)
	assert.Equal(t, actor, uc.actorExtractor)
}

// TestNewDisputeUseCase_NilDisputeRepository verifies the method-level
// validation that now owns the optional-dependency check: the merged
// constructor no longer rejects a nil dispute repository (it is optional),
// so the caller discovers the missing dependency when invoking OpenDispute.
func TestNewDisputeUseCase_NilDisputeRepository(t *testing.T) {
	t.Parallel()

	exceptionRepo := &stubExceptionRepo{}
	audit := &stubAuditPublisher{}
	actor := actorExtractor("analyst-1")
	infra := &stubInfraProvider{}

	uc, err := NewExceptionUseCase(exceptionRepo, actor, audit, infra)
	require.NoError(t, err)
	require.NotNil(t, uc)

	_, err = uc.OpenDispute(context.Background(), OpenDisputeCommand{
		ExceptionID: uuid.New(),
		Category:    "BANK_FEE_ERROR",
		Description: "test",
	})

	require.ErrorIs(t, err, ErrNilDisputeRepository)
}

func TestNewDisputeUseCase_NilExceptionRepository(t *testing.T) {
	t.Parallel()

	disputeRepo := &stubDisputeRepo{}
	audit := &stubAuditPublisher{}
	actor := actorExtractor("analyst-1")
	infra := &stubInfraProvider{}

	uc, err := NewExceptionUseCase(nil, actor, audit, infra, WithDisputeRepository(disputeRepo))

	require.ErrorIs(t, err, ErrNilExceptionRepository)
	assert.Nil(t, uc)
}

func TestNewDisputeUseCase_NilAuditPublisher(t *testing.T) {
	t.Parallel()

	disputeRepo := &stubDisputeRepo{}
	exceptionRepo := &stubExceptionRepo{}
	actor := actorExtractor("analyst-1")
	infra := &stubInfraProvider{}

	uc, err := NewExceptionUseCase(exceptionRepo, actor, nil, infra, WithDisputeRepository(disputeRepo))

	require.ErrorIs(t, err, ErrNilAuditPublisher)
	assert.Nil(t, uc)
}

func TestNewDisputeUseCase_NilActorExtractor(t *testing.T) {
	t.Parallel()

	disputeRepo := &stubDisputeRepo{}
	exceptionRepo := &stubExceptionRepo{}
	audit := &stubAuditPublisher{}
	infra := &stubInfraProvider{}

	uc, err := NewExceptionUseCase(exceptionRepo, nil, audit, infra, WithDisputeRepository(disputeRepo))

	require.ErrorIs(t, err, ErrNilActorExtractor)
	assert.Nil(t, uc)
}

func TestNewDisputeUseCase_NilInfraProvider(t *testing.T) {
	t.Parallel()

	disputeRepo := &stubDisputeRepo{}
	exceptionRepo := &stubExceptionRepo{}
	audit := &stubAuditPublisher{}
	actor := actorExtractor("analyst-1")

	uc, err := NewExceptionUseCase(exceptionRepo, actor, audit, nil, WithDisputeRepository(disputeRepo))

	require.ErrorIs(t, err, ErrNilInfraProvider)
	assert.Nil(t, uc)
}
