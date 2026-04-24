// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package shared provides shared domain types used across bounded contexts.
package shared

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v5/commons/outbox"
)

// OutboxEvent is a type alias for the canonical lib-commons/v5 outbox event.
// All bounded contexts use this alias so the canonical outbox internals
// remain transparent to callers.
type OutboxEvent = outbox.OutboxEvent

// NewOutboxEvent creates a new OutboxEvent via the canonical lib-commons constructor.
// This is a thin wrapper that preserves the existing call-site signature used
// throughout matcher's bounded contexts.
func NewOutboxEvent(
	ctx context.Context,
	eventType string,
	aggregateID uuid.UUID,
	payload []byte,
) (*OutboxEvent, error) {
	event, err := outbox.NewOutboxEvent(ctx, eventType, aggregateID, payload)
	if err != nil {
		return nil, fmt.Errorf("new outbox event: %w", err)
	}

	return event, nil
}
