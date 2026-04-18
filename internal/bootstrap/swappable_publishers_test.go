//go:build unit

package bootstrap

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

type stubIngestionPublisher struct{ err error }

func (p *stubIngestionPublisher) PublishIngestionCompleted(context.Context, *sharedDomain.IngestionCompletedEvent) error {
	return p.err
}

func (p *stubIngestionPublisher) PublishIngestionFailed(context.Context, *sharedDomain.IngestionFailedEvent) error {
	return p.err
}

type stubMatchPublisher struct{ err error }

func (p *stubMatchPublisher) PublishMatchConfirmed(context.Context, *sharedDomain.MatchConfirmedEvent) error {
	return p.err
}

func (p *stubMatchPublisher) PublishMatchUnmatched(context.Context, *sharedDomain.MatchUnmatchedEvent) error {
	return p.err
}

var (
	_ sharedPorts.IngestionEventPublisher = (*stubIngestionPublisher)(nil)
	_ sharedDomain.MatchEventPublisher    = (*stubMatchPublisher)(nil)
)

func TestSwappableIngestionPublisher_DelegatesPublishCalls(t *testing.T) {
	t.Parallel()

	delegateErr := errors.New("delegate failure")
	publisher := newSwappableIngestionPublisher(&stubIngestionPublisher{err: delegateErr})

	require.ErrorIs(t, publisher.PublishIngestionCompleted(context.Background(), nil), delegateErr)
	require.ErrorIs(t, publisher.PublishIngestionFailed(context.Background(), nil), delegateErr)
}

func TestSwappableIngestionPublisher_NilDelegateReturnsSentinel(t *testing.T) {
	t.Parallel()

	publisher := newSwappableIngestionPublisher(nil)

	assert.ErrorIs(t, publisher.PublishIngestionCompleted(context.Background(), nil), errIngestionPublisherUnavailable)
	assert.ErrorIs(t, publisher.PublishIngestionFailed(context.Background(), nil), errIngestionPublisherUnavailable)
}

func TestSwappableMatchPublisher_DelegatesPublishCalls(t *testing.T) {
	t.Parallel()

	delegateErr := errors.New("delegate failure")
	publisher := newSwappableMatchPublisher(&stubMatchPublisher{err: delegateErr})

	require.ErrorIs(t, publisher.PublishMatchConfirmed(context.Background(), nil), delegateErr)
	require.ErrorIs(t, publisher.PublishMatchUnmatched(context.Background(), nil), delegateErr)
}

func TestSwappableMatchPublisher_NilDelegateReturnsSentinel(t *testing.T) {
	t.Parallel()

	publisher := newSwappableMatchPublisher(nil)

	assert.ErrorIs(t, publisher.PublishMatchConfirmed(context.Background(), nil), errMatchPublisherUnavailable)
	assert.ErrorIs(t, publisher.PublishMatchUnmatched(context.Background(), nil), errMatchPublisherUnavailable)
}
