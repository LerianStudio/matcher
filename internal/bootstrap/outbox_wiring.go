// Package bootstrap provides application initialization and dependency wiring.
package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

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

// defaultTenantIDLookup is the package-level seam production uses to read
// the configured default tenant ID. It indirects through a var rather than
// calling auth.GetDefaultTenantID() directly so tests can exercise the
// empty-string short-circuit (which cannot be hit via auth.SetDefaultTenantID
// because an empty argument resets to the compile-time DefaultTenantID
// constant). Tests that override this MUST restore it in t.Cleanup.
var defaultTenantIDLookup = auth.GetDefaultTenantID

// DiscoverTenants returns the list of tenant IDs to dispatch outbox events for,
// appending the matcher default tenant (public schema) when the wrapped
// discoverer does not include it. This preserves the invariant that audit
// events originating in the default tenant are always delivered.
func (d *defaultTenantDiscoverer) DiscoverTenants(ctx context.Context) ([]string, error) {
	if d == nil || d.inner == nil {
		return nil, errDefaultTenantDiscovererUninitialized
	}

	tenants, err := d.inner.DiscoverTenants(ctx)
	if err != nil {
		return nil, fmt.Errorf("discover tenants: %w", err)
	}

	defaultTenantID := defaultTenantIDLookup()
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

// NewDefaultTenantDiscoverer wraps the given TenantDiscoverer with matcher's
// default-tenant behaviour: DiscoverTenants always appends the default tenant
// ID (public schema) when it isn't already present. Integration-test harnesses
// use this to mirror production tenant-discovery semantics without rebuilding
// the full composition root.
func NewDefaultTenantDiscoverer(inner outbox.TenantDiscoverer) outbox.TenantDiscoverer {
	return &defaultTenantDiscoverer{inner: inner}
}

// Sentinel errors for outbox event validation (non-retryable).
var (
	errInvalidPayload                       = errors.New("invalid payload format")
	errMissingJobID                         = errors.New("payload missing job id")
	errMissingContextID                     = errors.New("payload missing context id")
	errMissingSourceID                      = errors.New("payload missing source id")
	errMissingError                         = errors.New("payload missing error")
	errMissingMatchID                       = errors.New("payload missing match id")
	errMissingMatchRunID                    = errors.New("payload missing run id")
	errMissingMatchRuleID                   = errors.New("payload missing rule id")
	errMissingTransactionIDs                = errors.New("payload missing transaction ids")
	errMissingTenantID                      = errors.New("payload missing tenant id")
	errMissingEntityType                    = errors.New("payload missing entity type")
	errMissingEntityID                      = errors.New("payload missing entity id")
	errMissingAction                        = errors.New("payload missing action")
	errMissingReason                        = errors.New("payload missing reason")
	errAuditPublisherNotConfigured          = errors.New("audit publisher not configured")
	errDefaultTenantDiscovererUninitialized = errors.New("default tenant discoverer not initialized")
	errIngestionPublisherUnavailable        = errors.New("ingestion publisher is unavailable")
	errMatchPublisherUnavailable            = errors.New("match publisher is unavailable")
)

// nonRetryableErrors lists all errors that indicate permanent validation failures.
var nonRetryableErrors = []error{
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
	// Oversized or malformed payloads are structural defects that no amount
	// of retries can fix; surface them as permanent failures so the
	// dispatcher marks the event invalid instead of thrashing the retry
	// queue.
	outbox.ErrOutboxEventPayloadTooLarge,
	outbox.ErrOutboxEventPayloadNotJSON,
	errMissingEntityType,
	errMissingEntityID,
	errMissingAction,
	errMissingReason,
	errAuditPublisherNotConfigured,
	// Publisher-unavailable sentinels classify a misconfigured handler
	// wiring: the underlying concrete publisher is nil despite the
	// interface being non-nil. Retries cannot repair wiring — mark these
	// terminal so the event is flagged invalid immediately.
	errIngestionPublisherUnavailable,
	errMatchPublisherUnavailable,
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

// IsNonRetryableOutboxError exposes the production retry classifier so
// integration tests can wire a dispatcher through the same path the
// composition root uses. Exported as a wrapper (rather than exporting the
// unexported func directly) so internal refactors of the sentinel list do
// not leak through the API surface.
func IsNonRetryableOutboxError(err error) bool {
	return isNonRetryableOutboxError(err)
}

// RegisterOutboxHandlers is the exported entry point used by the composition root
// and by integration tests that need production wiring. Delegates to the
// unexported registerOutboxHandlers.
func RegisterOutboxHandlers(
	registry *outbox.HandlerRegistry,
	ingestPub sharedPorts.IngestionEventPublisher,
	matchPub sharedDomain.MatchEventPublisher,
	auditPub sharedDomain.AuditEventPublisher,
) error {
	return registerOutboxHandlers(registry, ingestPub, matchPub, auditPub)
}

// registerOutboxHandlers registers all event-type handlers on the canonical HandlerRegistry.
// Each handler is invoked when the outbox dispatcher processes an event of the matching type.
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

// Event publishing functions (one per outbox event type).

func publishIngestionCompleted(ctx context.Context, pub sharedPorts.IngestionEventPublisher, payload []byte) error {
	if sharedPorts.IsNilValue(pub) {
		return errIngestionPublisherUnavailable
	}

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
	if sharedPorts.IsNilValue(pub) {
		return errIngestionPublisherUnavailable
	}

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
	if sharedPorts.IsNilValue(pub) {
		return errMatchPublisherUnavailable
	}

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
	if sharedPorts.IsNilValue(pub) {
		return errMatchPublisherUnavailable
	}

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

// Payload validators live in outbox_payload_validation.go (same package).
