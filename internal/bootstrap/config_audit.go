// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	"github.com/LerianStudio/matcher/internal/auth"
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// systemConfigEntityType is the audit entity type for system configuration changes.
// Queries against the audit log use this value to retrieve config history.
const systemConfigEntityType = "system_config"

// systemConfigNamespace is a UUID v5 namespace for generating a deterministic
// EntityID from the string "system_config". AuditLogCreatedEvent requires a
// uuid.UUID for EntityID — using a deterministic UUID (instead of random) means
// all config audit events share the same entity, enabling efficient history queries.
//
// This is a project-specific, arbitrarily chosen UUID v5 namespace. The specific
// value has no external meaning but is stable — it MUST NOT change, as existing
// audit log entries reference the EntityID derived from it.
var systemConfigNamespace = uuid.MustParse("a1b2c3d4-e5f6-7890-abcd-ef1234567890")

// systemConfigEntityID is the stable EntityID for all system config audit events.
// Derived via UUID v5 so every process instance produces the same value.
var systemConfigEntityID = uuid.NewSHA1(systemConfigNamespace, []byte(systemConfigEntityType))

// Sentinel errors for config audit publisher.
var (
	ErrNilOutboxRepoForConfigAudit = errors.New("outbox repository is required for config audit publisher")
)

// ConfigAuditPublisher publishes audit events for system configuration changes
// through the transactional outbox pattern. It mirrors the exact approach used
// by the configuration bounded context's OutboxPublisher but is scoped to
// bootstrap-level config operations (API updates, file watcher reloads).
type ConfigAuditPublisher struct {
	outboxRepo sharedPorts.OutboxRepository
	logger     libLog.Logger
}

// NewConfigAuditPublisher creates a new audit publisher for config changes.
func NewConfigAuditPublisher(
	outboxRepo sharedPorts.OutboxRepository,
	logger libLog.Logger,
) (*ConfigAuditPublisher, error) {
	if outboxRepo == nil {
		return nil, ErrNilOutboxRepoForConfigAudit
	}

	if logger == nil {
		logger = &libLog.NopLogger{}
	}

	return &ConfigAuditPublisher{
		outboxRepo: outboxRepo,
		logger:     logger,
	}, nil
}

// PublishConfigChange publishes an audit event recording a system configuration
// change. The event flows through the outbox → dispatcher → governance consumer
// pipeline, ultimately landing in the immutable audit log.
//
// Parameters:
//   - ctx: request context (must carry tenant ID for multi-tenant environments)
//   - actor: who made the change — user ID from JWT for API, "system" for file watcher
//   - action: what happened — "updated" for API changes, "reloaded" for file/manual reloads
//   - changes: the before/after diff of configuration keys
func (publisher *ConfigAuditPublisher) PublishConfigChange(
	ctx context.Context,
	actor string,
	action string,
	changes []ConfigChange,
) error {
	if publisher == nil || publisher.outboxRepo == nil {
		return ErrNilOutboxRepoForConfigAudit
	}

	// Extract tenant ID from context — same pattern as configuration/adapters/audit.
	tenantIDStr := auth.GetTenantID(ctx)

	tenantID, err := uuid.Parse(tenantIDStr)
	if err != nil {
		return fmt.Errorf("config audit: parse tenant id: %w", err)
	}

	// Build the changes map for the audit event payload.
	changesMap := buildConfigChangesMap(changes)

	var actorPtr *string
	if actor != "" {
		actorPtr = &actor
	}

	now := time.Now().UTC()

	auditEvent := sharedDomain.AuditLogCreatedEvent{
		UniqueID:   uuid.New(),
		EventType:  sharedDomain.EventTypeAuditLogCreated,
		TenantID:   tenantID,
		EntityType: systemConfigEntityType,
		EntityID:   systemConfigEntityID,
		Action:     action,
		Actor:      actorPtr,
		Changes:    changesMap,
		OccurredAt: now,
		Timestamp:  now,
	}

	payload, err := json.Marshal(auditEvent)
	if err != nil {
		return fmt.Errorf("config audit: marshal event: %w", err)
	}

	outboxEvent, err := sharedDomain.NewOutboxEvent(
		ctx,
		sharedDomain.EventTypeAuditLogCreated,
		systemConfigEntityID,
		payload,
	)
	if err != nil {
		return fmt.Errorf("config audit: create outbox event: %w", err)
	}

	if _, err := publisher.outboxRepo.Create(ctx, outboxEvent); err != nil {
		return fmt.Errorf("config audit: persist outbox event: %w", err)
	}

	publisher.logger.Log(ctx, libLog.LevelInfo, "config change audit event published",
		libLog.String("action", action),
		libLog.String("actor", actor),
		libLog.Int("changes", len(changes)))

	return nil
}

// buildConfigChangesMap converts a slice of ConfigChange into the map[string]any
// format expected by AuditLogCreatedEvent.Changes.
func buildConfigChangesMap(changes []ConfigChange) map[string]any {
	if len(changes) == 0 {
		return nil
	}

	items := make([]map[string]any, 0, len(changes))
	for _, c := range changes {
		items = append(items, map[string]any{
			"key":       c.Key,
			"old_value": redactIfSensitive(c.Key, c.OldValue),
			"new_value": redactIfSensitive(c.Key, c.NewValue),
		})
	}

	return map[string]any{
		"config_changes": items,
	}
}

// SetAuditCallback registers a callback on the ConfigManager that publishes
// audit events when the file watcher detects changes. This decouples
// ConfigManager from the outbox infrastructure — ConfigManager remains a pure
// config management concern, while audit publishing is wired at the bootstrap layer.
//
// The callback receives the changes from a successful Reload() and publishes
// them as an audit event with actor="system" and action="reloaded".
func SetAuditCallback(
	cm *ConfigManager,
	publisher *ConfigAuditPublisher,
	logger libLog.Logger,
) {
	if cm == nil || publisher == nil {
		return
	}

	if logger == nil {
		logger = &libLog.NopLogger{}
	}

	// We can't directly hook into the file watcher's debounced Reload() call,
	// but we CAN use the Subscribe mechanism: subscribers fire after every
	// successful reload (both manual and file-watcher triggered).
	//
	// Seed baseline from current version at registration time. Subscribers are
	// not invoked immediately on registration, so this avoids dropping the first
	// real change notification after callback wiring.
	var lastVersion atomic.Uint64

	lastVersion.Store(cm.Version())

	cm.Subscribe(func(newCfg *Config) {
		currentVersion := cm.Version()

		// Version unchanged means duplicate notification — skip.
		if currentVersion <= lastVersion.Load() {
			return
		}

		lastVersion.Store(currentVersion)

		// Skip API-driven updates/reloads — the API handler in config_api.go
		// already publishes its own audit event with actor and changes.
		if source, ok := cm.lastUpdateSource.Load().(string); ok && (source == configUpdateSourceAPI || source == configUpdateSourceReloadAPI) {
			return
		}

		// Retrieve the field-level changes stored by reloadLocked() before
		// notifying subscribers. Falls back to empty (no audit) if unavailable.
		changes, _ := cm.lastChanges.Load().([]ConfigChange)
		if len(changes) == 0 {
			return // no actual changes to audit
		}

		// Use a background context because subscriber callbacks run outside
		// any HTTP request context. The tenant ID comes from auth's stable
		// default-tenant source so config history remains in one audit stream.
		if newCfg == nil {
			return
		}

		stableTenantID := strings.TrimSpace(auth.GetDefaultTenantID())
		if stableTenantID == "" {
			logger.Log(context.Background(), libLog.LevelWarn, "skipping config audit: no default tenant ID configured")
			return
		}

		ctx := context.Background()

		ctx = context.WithValue(ctx, auth.TenantIDKey, stableTenantID)

		if err := publisher.PublishConfigChange(ctx, "system", "reloaded", changes); err != nil {
			logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to publish config audit event from file watcher: %v", err))
		}
	})
}
