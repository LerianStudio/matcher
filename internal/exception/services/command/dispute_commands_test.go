//go:build unit

package command

import (
	"testing"

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

	uc, err := NewDisputeUseCase(disputeRepo, exceptionRepo, audit, actor, infra)

	require.NoError(t, err)
	require.NotNil(t, uc)
	assert.Equal(t, disputeRepo, uc.disputeRepo)
	assert.Equal(t, exceptionRepo, uc.exceptionRepo)
	assert.Equal(t, audit, uc.auditPublisher)
	assert.Equal(t, actor, uc.actorExtractor)
}

func TestNewDisputeUseCase_NilDisputeRepository(t *testing.T) {
	t.Parallel()

	exceptionRepo := &stubExceptionRepo{}
	audit := &stubAuditPublisher{}
	actor := actorExtractor("analyst-1")
	infra := &stubInfraProvider{}

	uc, err := NewDisputeUseCase(nil, exceptionRepo, audit, actor, infra)

	require.ErrorIs(t, err, ErrNilDisputeRepository)
	assert.Nil(t, uc)
}

func TestNewDisputeUseCase_NilExceptionRepository(t *testing.T) {
	t.Parallel()

	disputeRepo := &stubDisputeRepo{}
	audit := &stubAuditPublisher{}
	actor := actorExtractor("analyst-1")
	infra := &stubInfraProvider{}

	uc, err := NewDisputeUseCase(disputeRepo, nil, audit, actor, infra)

	require.ErrorIs(t, err, ErrNilExceptionRepository)
	assert.Nil(t, uc)
}

func TestNewDisputeUseCase_NilAuditPublisher(t *testing.T) {
	t.Parallel()

	disputeRepo := &stubDisputeRepo{}
	exceptionRepo := &stubExceptionRepo{}
	actor := actorExtractor("analyst-1")
	infra := &stubInfraProvider{}

	uc, err := NewDisputeUseCase(disputeRepo, exceptionRepo, nil, actor, infra)

	require.ErrorIs(t, err, ErrNilAuditPublisher)
	assert.Nil(t, uc)
}

func TestNewDisputeUseCase_NilActorExtractor(t *testing.T) {
	t.Parallel()

	disputeRepo := &stubDisputeRepo{}
	exceptionRepo := &stubExceptionRepo{}
	audit := &stubAuditPublisher{}
	infra := &stubInfraProvider{}

	uc, err := NewDisputeUseCase(disputeRepo, exceptionRepo, audit, nil, infra)

	require.ErrorIs(t, err, ErrNilActorExtractor)
	assert.Nil(t, uc)
}

func TestNewDisputeUseCase_NilInfraProvider(t *testing.T) {
	t.Parallel()

	disputeRepo := &stubDisputeRepo{}
	exceptionRepo := &stubExceptionRepo{}
	audit := &stubAuditPublisher{}
	actor := actorExtractor("analyst-1")

	uc, err := NewDisputeUseCase(disputeRepo, exceptionRepo, audit, actor, nil)

	require.ErrorIs(t, err, ErrNilInfraProvider)
	assert.Nil(t, uc)
}
