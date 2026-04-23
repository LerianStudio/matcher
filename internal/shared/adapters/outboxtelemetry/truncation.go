// Package outboxtelemetry records outbox-payload truncation events to the
// structured log and the shared OTel counter.
//
// Truncation decisions (whether a payload exceeds the broker cap, how to
// rewrite it) remain in the domain (internal/shared/domain/outbox_payload.go).
// The domain function is side-effect free; this package is where the audit
// trail for those decisions lives. Separating the two keeps the domain pure
// so the depguard domain-no-logging rule can remain strict.
//
// Two flavors of truncation event are supported:
//   - RecordAuditChangesTruncated: the audit event's Changes map was
//     replaced with a marker because the serialized envelope exceeded
//     the broker cap.
//   - RecordIDListTruncated: a cross-context match event's TransactionIDs
//     slice was trimmed to fit the broker cap.
//
// Both share the outbox_payload_truncated_total{entity_type=...} counter so
// operators can alert on silent-but-lossy paths without distinguishing
// between the two producer shapes.
package outboxtelemetry

import (
	"context"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-commons/v5/commons/opentelemetry/metrics"
)

// outboxPayloadTruncatedMetric is the OTel counter that records truncation
// events across all publishers. Labels: entity_type.
var outboxPayloadTruncatedMetric = metrics.Metric{
	Name:        "outbox_payload_truncated_total",
	Unit:        "{event}",
	Description: "Outbox events whose payload exceeded the broker cap and was truncated before publishing.",
}

// AuditChangesTruncationNote is the operator-facing message embedded in an
// audit event's Changes marker when the serialized envelope exceeded the
// outbox cap. Re-exported from here so callers that record the log line and
// build the marker do not need to import the domain package for the
// constant alone.
const AuditChangesTruncationNote = "audit diff exceeded outbox payload cap; original not persisted"

// idListTruncationNote is the log-line message emitted when a cross-context
// ID-list event's TransactionIDs field had to be trimmed to fit the broker
// cap. The marker itself lives on the typed event struct via
// TruncatedIDCount, not inside a map.
const idListTruncationNote = "id list exceeded outbox cap; event published with truncated ids"

// auditTruncationNote is the WARN line message for audit-diff truncation.
// Kept distinct from AuditChangesTruncationNote (which goes on the wire in
// the marker map) so operators can grep the log stream independently of the
// consumer-facing note.
const auditTruncationNote = "outbox payload truncated due to broker cap"

// RecordAuditChangesTruncated writes the audit truncation WARN line and
// increments the outbox_payload_truncated_total counter tagged with
// entityType. Publishers call this after swapping Changes for the
// truncation marker so the event is visible to operators even when the
// downstream consumer is still healthy.
//
// entityType SHOULD identify the producer (e.g. "audit_config",
// "audit_exception") so alerts can group by publisher rather than by
// business entity.
func RecordAuditChangesTruncated(
	ctx context.Context,
	entityType string,
	entityID uuid.UUID,
	originalBytes, maxAllowedBytes int,
) {
	logger, _, _, metricFactory := libCommons.NewTrackingFromContext(ctx)
	if logger == nil {
		logger = &libLog.NopLogger{}
	}

	logger.Log(ctx, libLog.LevelWarn,
		auditTruncationNote,
		libLog.String("entity_type", entityType),
		libLog.String("entity_id", entityID.String()),
		libLog.Int("original_size_bytes", originalBytes),
		libLog.Int("max_allowed_bytes", maxAllowedBytes),
	)

	emitCounter(ctx, metricFactory, entityType)
}

// RecordIDListTruncated writes the ID-list truncation WARN line and
// increments the outbox_payload_truncated_total counter. Matching callers
// invoke this when domain-layer TruncateIDListIfTooLarge returned a
// truncated slice (detected via len(truncated) != originalCount).
//
// entityType SHOULD identify the producer (e.g. "match_confirmed",
// "match_unmatched").
func RecordIDListTruncated(
	ctx context.Context,
	entityType string,
	entityID uuid.UUID,
	originalCount, truncatedCount, maxAllowedBytes int,
) {
	logger, _, _, metricFactory := libCommons.NewTrackingFromContext(ctx)
	if logger == nil {
		logger = &libLog.NopLogger{}
	}

	logger.Log(ctx, libLog.LevelWarn,
		idListTruncationNote,
		libLog.String("entity_type", entityType),
		libLog.String("entity_id", entityID.String()),
		libLog.Int("original_count", originalCount),
		libLog.Int("truncated_count", truncatedCount),
		libLog.Int("max_allowed_bytes", maxAllowedBytes),
	)

	emitCounter(ctx, metricFactory, entityType)
}

func emitCounter(
	ctx context.Context,
	factory *metrics.MetricsFactory,
	entityType string,
) {
	if factory == nil {
		return
	}

	counter, err := factory.Counter(outboxPayloadTruncatedMetric)
	if err != nil || counter == nil {
		return
	}

	// Best-effort emission — on failure the WARN log above still captures
	// the event. Surfacing a secondary error would conflate broker-cap
	// truncation (the real signal) with telemetry-pipeline failures.
	_ = counter.WithLabels(map[string]string{"entity_type": entityType}).AddOne(ctx)
}
