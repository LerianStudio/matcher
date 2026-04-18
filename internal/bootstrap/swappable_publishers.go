// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"sync"

	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

var (
	errIngestionPublisherUnavailable = errors.New("ingestion publisher is unavailable")
	errMatchPublisherUnavailable     = errors.New("match publisher is unavailable")
)

type swappableIngestionPublisher struct {
	mu      sync.RWMutex
	current sharedPorts.IngestionEventPublisher
}

func newSwappableIngestionPublisher(current sharedPorts.IngestionEventPublisher) *swappableIngestionPublisher {
	return &swappableIngestionPublisher{current: current}
}

// PublishIngestionCompleted forwards the completion event to the current delegate.
func (publisher *swappableIngestionPublisher) PublishIngestionCompleted(ctx context.Context, event *sharedDomain.IngestionCompletedEvent) error {
	publisher.mu.RLock()
	current := publisher.current
	publisher.mu.RUnlock()

	if current == nil {
		return errIngestionPublisherUnavailable
	}

	if err := current.PublishIngestionCompleted(ctx, event); err != nil {
		return fmt.Errorf("publish ingestion completed event: %w", err)
	}

	return nil
}

// PublishIngestionFailed forwards the failure event to the current delegate.
func (publisher *swappableIngestionPublisher) PublishIngestionFailed(ctx context.Context, event *sharedDomain.IngestionFailedEvent) error {
	publisher.mu.RLock()
	current := publisher.current
	publisher.mu.RUnlock()

	if current == nil {
		return errIngestionPublisherUnavailable
	}

	if err := current.PublishIngestionFailed(ctx, event); err != nil {
		return fmt.Errorf("publish ingestion failed event: %w", err)
	}

	return nil
}

type swappableMatchPublisher struct {
	mu      sync.RWMutex
	current sharedDomain.MatchEventPublisher
}

func newSwappableMatchPublisher(current sharedDomain.MatchEventPublisher) *swappableMatchPublisher {
	return &swappableMatchPublisher{current: current}
}

// PublishMatchConfirmed forwards the confirmed event to the current delegate.
func (publisher *swappableMatchPublisher) PublishMatchConfirmed(ctx context.Context, event *sharedDomain.MatchConfirmedEvent) error {
	publisher.mu.RLock()
	current := publisher.current
	publisher.mu.RUnlock()

	if current == nil {
		return errMatchPublisherUnavailable
	}

	if err := current.PublishMatchConfirmed(ctx, event); err != nil {
		return fmt.Errorf("publish match confirmed event: %w", err)
	}

	return nil
}

// PublishMatchUnmatched forwards the unmatched event to the current delegate.
func (publisher *swappableMatchPublisher) PublishMatchUnmatched(ctx context.Context, event *sharedDomain.MatchUnmatchedEvent) error {
	publisher.mu.RLock()
	current := publisher.current
	publisher.mu.RUnlock()

	if current == nil {
		return errMatchPublisherUnavailable
	}

	if err := current.PublishMatchUnmatched(ctx, event); err != nil {
		return fmt.Errorf("publish match unmatched event: %w", err)
	}

	return nil
}
