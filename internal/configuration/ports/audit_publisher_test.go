// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package ports

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestAuditEvent_FieldAccess(t *testing.T) {
	t.Parallel()

	entityID := uuid.New()
	occurredAt := time.Now()

	event := AuditEvent{
		EntityType: "reconciliation",
		EntityID:   entityID,
		Action:     "created",
		Actor:      "system",
		OccurredAt: occurredAt,
		Changes:    nil,
	}

	assert.Equal(t, "reconciliation", event.EntityType)
	assert.Equal(t, entityID, event.EntityID)
	assert.Equal(t, "created", event.Action)
	assert.Equal(t, "system", event.Actor)
	assert.Equal(t, occurredAt, event.OccurredAt)
	assert.Nil(t, event.Changes)
}

func TestAuditEvent_ChangesMap(t *testing.T) {
	t.Parallel()

	changes := map[string]any{
		"status":     "active",
		"count":      42,
		"enabled":    true,
		"percentage": 99.5,
		"nested":     map[string]any{"key": "value"},
	}

	event := AuditEvent{
		Changes: changes,
	}

	assert.Len(t, event.Changes, 5)
	assert.Equal(t, "active", event.Changes["status"])
	assert.Equal(t, 42, event.Changes["count"])
	assert.Equal(t, true, event.Changes["enabled"])
	assert.InDelta(t, 99.5, event.Changes["percentage"], 0.001)

	nested, ok := event.Changes["nested"].(map[string]any)
	assert.True(t, ok)
	assert.Equal(t, "value", nested["key"])
}
