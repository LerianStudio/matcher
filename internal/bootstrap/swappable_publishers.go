// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"sync"

	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

type swappableIngestionPublisher struct {
	mu      sync.RWMutex
	current sharedPorts.IngestionEventPublisher
}

func newSwappableIngestionPublisher(current sharedPorts.IngestionEventPublisher) *swappableIngestionPublisher {
	return &swappableIngestionPublisher{current: current}
}

func (publisher *swappableIngestionPublisher) Swap(next sharedPorts.IngestionEventPublisher) sharedPorts.IngestionEventPublisher {
	publisher.mu.Lock()
	defer publisher.mu.Unlock()

	previous := publisher.current
	publisher.current = next

	return previous
}

func (publisher *swappableIngestionPublisher) PublishIngestionCompleted(ctx context.Context, event *sharedDomain.IngestionCompletedEvent) error {
	publisher.mu.RLock()
	defer publisher.mu.RUnlock()

	return publisher.current.PublishIngestionCompleted(ctx, event)
}

func (publisher *swappableIngestionPublisher) PublishIngestionFailed(ctx context.Context, event *sharedDomain.IngestionFailedEvent) error {
	publisher.mu.RLock()
	defer publisher.mu.RUnlock()

	return publisher.current.PublishIngestionFailed(ctx, event)
}

type swappableMatchPublisher struct {
	mu      sync.RWMutex
	current sharedDomain.MatchEventPublisher
}

func newSwappableMatchPublisher(current sharedDomain.MatchEventPublisher) *swappableMatchPublisher {
	return &swappableMatchPublisher{current: current}
}

func (publisher *swappableMatchPublisher) Swap(next sharedDomain.MatchEventPublisher) sharedDomain.MatchEventPublisher {
	publisher.mu.Lock()
	defer publisher.mu.Unlock()

	previous := publisher.current
	publisher.current = next

	return previous
}

func (publisher *swappableMatchPublisher) PublishMatchConfirmed(ctx context.Context, event *sharedDomain.MatchConfirmedEvent) error {
	publisher.mu.RLock()
	defer publisher.mu.RUnlock()

	return publisher.current.PublishMatchConfirmed(ctx, event)
}

func (publisher *swappableMatchPublisher) PublishMatchUnmatched(ctx context.Context, event *sharedDomain.MatchUnmatchedEvent) error {
	publisher.mu.RLock()
	defer publisher.mu.RUnlock()

	return publisher.current.PublishMatchUnmatched(ctx, event)
}
