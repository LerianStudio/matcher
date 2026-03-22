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

func TestSwappableIngestionPublisher_SwapReturnsPreviousAndDelegates(t *testing.T) {
	t.Parallel()

	first := &stubIngestionPublisher{}
	second := &stubIngestionPublisher{err: errors.New("new publisher")}
	publisher := newSwappableIngestionPublisher(first)

	previous := publisher.Swap(second)
	assert.Same(t, first, previous)
	require.Error(t, publisher.PublishIngestionCompleted(context.Background(), nil))
}

func TestSwappableMatchPublisher_SwapReturnsPreviousAndDelegates(t *testing.T) {
	t.Parallel()

	first := &stubMatchPublisher{}
	second := &stubMatchPublisher{err: errors.New("new publisher")}
	publisher := newSwappableMatchPublisher(first)

	previous := publisher.Swap(second)
	assert.Same(t, first, previous)
	require.Error(t, publisher.PublishMatchConfirmed(context.Background(), nil))
}
