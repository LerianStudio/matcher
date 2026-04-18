// Package bootstrap provides application initialization and dependency wiring.
package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v5/commons/outbox"

	"github.com/LerianStudio/matcher/internal/auth"
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// defaultTenantDiscoverer wraps a canonical SchemaResolver and injects the
// default tenant (public schema) which uses no UUID-named schema in pg_namespace.
// This preserves matcher's critical behavior: the default tenant's outbox events
// are always dispatched even though its schema name is not UUID-shaped.
type defaultTenantDiscoverer struct {
	inner outbox.TenantDiscoverer
}

func (d *defaultTenantDiscoverer) DiscoverTenants(ctx context.Context) ([]string, error) {
	tenants, err := d.inner.DiscoverTenants(ctx)
	if err != nil {
		return nil, fmt.Errorf("discover tenants: %w", err)
	}

	defaultTenantID := auth.GetDefaultTenantID()
	if defaultTenantID == "" {
		return tenants, nil
	}

	for _, t := range tenants {
		if t == defaultTenantID {
			return tenants, nil
		}
	}

	return append(tenants, defaultTenantID), nil
}

// Verify interface satisfaction at compile time.
var _ outbox.TenantDiscoverer = (*defaultTenantDiscoverer)(nil)

// Sentinel errors for outbox event validation (non-retryable).
var (
	errUnsupportedEventType        = errors.New("unsupported outbox event type")
	errOutboxEventPayloadRequired  = errors.New("outbox event payload is required")
	errInvalidPayload              = errors.New("invalid payload format")
	errMissingJobID                = errors.New("payload missing job id")
	errMissingContextID            = errors.New("payload missing context id")
	errMissingSourceID             = errors.New("payload missing source id")
	errMissingError                = errors.New("payload missing error")
	errMissingMatchID              = errors.New("payload missing match id")
	errMissingMatchRunID           = errors.New("payload missing run id")
	errMissingMatchRuleID          = errors.New("payload missing rule id")
	errMissingTransactionIDs       = errors.New("payload missing transaction ids")
	errMissingTenantID             = errors.New("payload missing tenant id")
	errMissingEntityType           = errors.New("payload missing entity type")
	errMissingEntityID             = errors.New("payload missing entity id")
	errMissingAction               = errors.New("payload missing action")
	errMissingReason               = errors.New("payload missing reason")
	errAuditPublisherNotConfigured = errors.New("audit publisher not configured")
)

// nonRetryableErrors lists all errors that indicate permanent validation failures.
var nonRetryableErrors = []error{
	errUnsupportedEventType,
	errInvalidPayload,
	errMissingJobID,
	errMissingContextID,
	errMissingSourceID,
	errMissingError,
	errMissingMatchID,
	errMissingMatchRunID,
	errMissingMatchRuleID,
	errMissingTransactionIDs,
	errMissingTenantID,
	outbox.ErrOutboxEventRequired,
	errOutboxEventPayloadRequired,
	errMissingEntityType,
	errMissingEntityID,
	errMissingAction,
	errMissingReason,
	errAuditPublisherNotConfigured,
}

// isNonRetryableOutboxError checks if an error is a permanent validation failure.
func isNonRetryableOutboxError(err error) bool {
	if err == nil {
		return false
	}

	for _, target := range nonRetryableErrors {
		if errors.Is(err, target) {
			return true
		}
	}

	return false
}

// registerOutboxHandlers registers all event-type handlers on the canonical HandlerRegistry.
// Each handler replaces a case from the old bespoke dispatcher's publishEvent switch.
func registerOutboxHandlers(
	registry *outbox.HandlerRegistry,
	ingestPub sharedPorts.IngestionEventPublisher,
	matchPub sharedDomain.MatchEventPublisher,
	auditPub sharedDomain.AuditEventPublisher,
) error {
	if err := registry.Register(sharedDomain.EventTypeIngestionCompleted, func(ctx context.Context, event *outbox.OutboxEvent) error {
		return publishIngestionCompleted(ctx, ingestPub, event.Payload)
	}); err != nil {
		return fmt.Errorf("register ingestion completed handler: %w", err)
	}

	if err := registry.Register(sharedDomain.EventTypeIngestionFailed, func(ctx context.Context, event *outbox.OutboxEvent) error {
		return publishIngestionFailed(ctx, ingestPub, event.Payload)
	}); err != nil {
		return fmt.Errorf("register ingestion failed handler: %w", err)
	}

	if err := registry.Register(sharedDomain.EventTypeMatchConfirmed, func(ctx context.Context, event *outbox.OutboxEvent) error {
		return publishMatchConfirmed(ctx, matchPub, event.Payload)
	}); err != nil {
		return fmt.Errorf("register match confirmed handler: %w", err)
	}

	if err := registry.Register(sharedDomain.EventTypeMatchUnmatched, func(ctx context.Context, event *outbox.OutboxEvent) error {
		return publishMatchUnmatched(ctx, matchPub, event.Payload)
	}); err != nil {
		return fmt.Errorf("register match unmatched handler: %w", err)
	}

	if err := registry.Register(sharedDomain.EventTypeAuditLogCreated, func(ctx context.Context, event *outbox.OutboxEvent) error {
		return publishAuditLogCreated(ctx, auditPub, event.Payload)
	}); err != nil {
		return fmt.Errorf("register audit log created handler: %w", err)
	}

	return nil
}

// --- Event publishing functions (extracted from bespoke dispatcher) ---

func publishIngestionCompleted(ctx context.Context, pub sharedPorts.IngestionEventPublisher, payload []byte) error {
	var event sharedDomain.IngestionCompletedEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return fmt.Errorf("ingestion completed %w: %w", errInvalidPayload, err)
	}

	if err := validateIngestionCompletedPayload(event); err != nil {
		return err
	}

	if err := pub.PublishIngestionCompleted(ctx, &event); err != nil {
		return fmt.Errorf("publish ingestion completed: %w", err)
	}

	return nil
}

func publishIngestionFailed(ctx context.Context, pub sharedPorts.IngestionEventPublisher, payload []byte) error {
	var event sharedDomain.IngestionFailedEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return fmt.Errorf("ingestion failed %w: %w", errInvalidPayload, err)
	}

	if err := validateIngestionFailedPayload(event); err != nil {
		return err
	}

	if err := pub.PublishIngestionFailed(ctx, &event); err != nil {
		return fmt.Errorf("publish ingestion failed: %w", err)
	}

	return nil
}

func publishMatchConfirmed(ctx context.Context, pub sharedDomain.MatchEventPublisher, payload []byte) error {
	var event sharedDomain.MatchConfirmedEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return fmt.Errorf("match confirmed %w: %w", errInvalidPayload, err)
	}

	if err := validateMatchConfirmedPayload(event); err != nil {
		return err
	}

	if err := pub.PublishMatchConfirmed(ctx, &event); err != nil {
		return fmt.Errorf("publish match confirmed: %w", err)
	}

	return nil
}

func publishMatchUnmatched(ctx context.Context, pub sharedDomain.MatchEventPublisher, payload []byte) error {
	var event sharedDomain.MatchUnmatchedEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return fmt.Errorf("match unmatched %w: %w", errInvalidPayload, err)
	}

	if err := validateMatchUnmatchedPayload(event); err != nil {
		return err
	}

	if err := pub.PublishMatchUnmatched(ctx, &event); err != nil {
		return fmt.Errorf("publish match unmatched: %w", err)
	}

	return nil
}

func publishAuditLogCreated(ctx context.Context, pub sharedDomain.AuditEventPublisher, payload []byte) error {
	if sharedPorts.IsNilValue(pub) {
		return errAuditPublisherNotConfigured
	}

	var event sharedDomain.AuditLogCreatedEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return fmt.Errorf("audit log created %w: %w", errInvalidPayload, err)
	}

	if err := validateAuditLogCreatedPayload(event); err != nil {
		return err
	}

	if err := pub.PublishAuditLogCreated(ctx, &event); err != nil {
		return fmt.Errorf("publish audit log created: %w", err)
	}

	return nil
}

// --- Payload validation functions ---

func validateIngestionCompletedPayload(payload sharedDomain.IngestionCompletedEvent) error {
	if payload.JobID == uuid.Nil {
		return fmt.Errorf("ingestion completed: %w", errMissingJobID)
	}

	if payload.ContextID == uuid.Nil {
		return fmt.Errorf("ingestion completed: %w", errMissingContextID)
	}

	if payload.SourceID == uuid.Nil {
		return fmt.Errorf("ingestion completed: %w", errMissingSourceID)
	}

	return nil
}

func validateIngestionFailedPayload(payload sharedDomain.IngestionFailedEvent) error {
	if payload.JobID == uuid.Nil {
		return fmt.Errorf("ingestion failed: %w", errMissingJobID)
	}

	if payload.ContextID == uuid.Nil {
		return fmt.Errorf("ingestion failed: %w", errMissingContextID)
	}

	if payload.SourceID == uuid.Nil {
		return fmt.Errorf("ingestion failed: %w", errMissingSourceID)
	}

	if payload.Error == "" {
		return fmt.Errorf("ingestion failed: %w", errMissingError)
	}

	return nil
}

func validateMatchConfirmedPayload(payload sharedDomain.MatchConfirmedEvent) error {
	if payload.TenantID == uuid.Nil {
		return fmt.Errorf("match confirmed: %w", errMissingTenantID)
	}

	if payload.ContextID == uuid.Nil {
		return fmt.Errorf("match confirmed: %w", errMissingContextID)
	}

	if payload.RunID == uuid.Nil {
		return fmt.Errorf("match confirmed: %w", errMissingMatchRunID)
	}

	if payload.MatchID == uuid.Nil {
		return fmt.Errorf("match confirmed: %w", errMissingMatchID)
	}

	if payload.RuleID == uuid.Nil {
		return fmt.Errorf("match confirmed: %w", errMissingMatchRuleID)
	}

	if len(payload.TransactionIDs) == 0 {
		return fmt.Errorf("match confirmed: %w", errMissingTransactionIDs)
	}

	return nil
}

func validateMatchUnmatchedPayload(payload sharedDomain.MatchUnmatchedEvent) error {
	if payload.TenantID == uuid.Nil {
		return fmt.Errorf("match unmatched: %w", errMissingTenantID)
	}

	if payload.ContextID == uuid.Nil {
		return fmt.Errorf("match unmatched: %w", errMissingContextID)
	}

	if payload.RunID == uuid.Nil {
		return fmt.Errorf("match unmatched: %w", errMissingMatchRunID)
	}

	if payload.MatchID == uuid.Nil {
		return fmt.Errorf("match unmatched: %w", errMissingMatchID)
	}

	if len(payload.TransactionIDs) == 0 {
		return fmt.Errorf("match unmatched: %w", errMissingTransactionIDs)
	}

	if payload.Reason == "" {
		return fmt.Errorf("match unmatched: %w", errMissingReason)
	}

	return nil
}

func validateAuditLogCreatedPayload(payload sharedDomain.AuditLogCreatedEvent) error {
	if payload.TenantID == uuid.Nil {
		return fmt.Errorf("audit log created: %w", errMissingTenantID)
	}

	if payload.EntityType == "" {
		return fmt.Errorf("audit log created: %w", errMissingEntityType)
	}

	if payload.EntityID == uuid.Nil {
		return fmt.Errorf("audit log created: %w", errMissingEntityID)
	}

	if payload.Action == "" {
		return fmt.Errorf("audit log created: %w", errMissingAction)
	}

	return nil
}
