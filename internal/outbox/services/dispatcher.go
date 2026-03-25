// Package services provides outbox event dispatching logic.
package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	"github.com/LerianStudio/lib-commons/v4/commons/backoff"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v4/commons/runtime"

	"github.com/LerianStudio/matcher/internal/auth"
	outboxEntities "github.com/LerianStudio/matcher/internal/outbox/domain/entities"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// Sentinel errors for dispatcher validation.
var (
	ErrOutboxRepositoryRequired    = errors.New("outbox repository is required")
	ErrIngestionPublisherRequired  = errors.New("ingestion publisher is required")
	ErrMatchingPublisherRequired   = errors.New("matching publisher is required")
	ErrOutboxDispatcherRequired    = errors.New("outbox dispatcher is required")
	ErrUnsupportedEventType        = errors.New("unsupported outbox event type")
	ErrAuditPublisherNotConfigured = errors.New("audit publisher not configured")
	ErrOutboxEventPayloadRequired  = errors.New("outbox event payload is required")
	ErrMissingJobID                = errors.New("payload missing job id")
	ErrMissingContextID            = errors.New("payload missing context id")
	ErrMissingSourceID             = errors.New("payload missing source id")
	ErrMissingError                = errors.New("payload missing error")
	ErrMissingMatchID              = errors.New("payload missing match id")
	ErrMissingMatchRunID           = errors.New("payload missing run id")
	ErrMissingMatchRuleID          = errors.New("payload missing rule id")
	ErrMissingTransactionIDs       = errors.New("payload missing transaction ids")
	ErrMissingTenantID             = errors.New("payload missing tenant id")
	ErrInvalidPayload              = errors.New("invalid payload format")
	ErrMissingEntityType           = errors.New("payload missing entity type")
	ErrMissingEntityID             = errors.New("payload missing entity id")
	ErrMissingAction               = errors.New("payload missing action")
	ErrMissingReason               = errors.New("payload missing reason")
)

const (
	defaultDispatchInterval   = 2 * time.Second
	defaultBatchSize          = 50
	defaultPublishMaxAttempts = 3
	defaultPublishBackoff     = 200 * time.Millisecond
	// defaultListPendingFailureThreshold defines how many consecutive ListPending failures
	// per tenant are tolerated before escalating to a high-priority error log.
	//
	// Rationale for choosing 3:
	//   - Tolerates transient failures such as network blips or momentary DB unavailability.
	//   - At the default polling interval (~2s), 3 consecutive failures correspond to roughly
	//     6 seconds of sustained failure, long enough to filter out brief hiccups but short
	//     enough to surface persistent issues quickly.
	//   - Below this threshold the failure is logged at normal level and retried silently.
	//   - At or above this threshold it is treated as a persistent issue and an Errorf is
	//     emitted so alerting rules can fire on the tenant-specific failure pattern.
	defaultListPendingFailureThreshold = 3
	defaultRetryWindow                 = 5 * time.Minute
	defaultMaxDispatchAttempts         = 10
	defaultProcessingTimeout           = 10 * time.Minute
	defaultPriorityBudget              = 10
	defaultMaxFailedPerBatch           = 25
)

// Dispatcher handles publishing outbox events to message queues.
type Dispatcher struct {
	repo                        sharedPorts.OutboxRepository
	ingestPub                   sharedPorts.IngestionEventPublisher
	matchPub                    sharedDomain.MatchEventPublisher
	auditPub                    sharedDomain.AuditEventPublisher
	logger                      libLog.Logger
	tracer                      trace.Tracer
	interval                    time.Duration
	batchSize                   int
	publishMaxAttempts          int
	publishBackoff              time.Duration
	listPendingFailureCounts    map[string]int
	failureCountsMu             sync.Mutex
	listPendingFailureThreshold int
	retryWindow                 time.Duration
	retryWindowGetter           func() time.Duration
	maxDispatchAttempts         int
	processingTimeout           time.Duration
	stop                        chan struct{}
	stopOnce                    sync.Once
	cancelFunc                  context.CancelFunc
	dispatchWg                  sync.WaitGroup

	// production indicates whether the application is running in production.
	// Governs SafeError behavior (suppresses internal error details when true).
	production bool

	// OTel metrics
	eventsDispatched metric.Int64Counter
	eventsFailed     metric.Int64Counter
	dispatchLatency  metric.Float64Histogram
	queueDepth       metric.Int64Gauge
}

// SetRetryWindowGetter injects a live config-backed retry window source.
func (dispatcher *Dispatcher) SetRetryWindowGetter(getter func() time.Duration) {
	if dispatcher == nil {
		return
	}

	dispatcher.retryWindowGetter = getter
}

func (dispatcher *Dispatcher) currentRetryWindow() time.Duration {
	if dispatcher == nil {
		return defaultRetryWindow
	}

	if dispatcher.retryWindowGetter != nil {
		if window := dispatcher.retryWindowGetter(); window > 0 {
			return window
		}
	}

	if dispatcher.retryWindow > 0 {
		return dispatcher.retryWindow
	}

	return defaultRetryWindow
}

// DispatcherOption configures optional dispatcher dependencies.
type DispatcherOption func(*Dispatcher)

// WithAuditPublisher sets the audit event publisher for the dispatcher.
func WithAuditPublisher(pub sharedDomain.AuditEventPublisher) DispatcherOption {
	return func(dispatcher *Dispatcher) {
		if sharedPorts.IsNilValue(pub) {
			dispatcher.auditPub = nil
			return
		}

		dispatcher.auditPub = pub
	}
}

// WithBatchSize configures the maximum number of events processed per dispatch cycle.
func WithBatchSize(size int) DispatcherOption {
	return func(dispatcher *Dispatcher) {
		if size > 0 {
			dispatcher.batchSize = size
		}
	}
}

// WithProduction sets whether the dispatcher runs in production mode.
// When true, SafeError suppresses internal error details from logs.
func WithProduction(production bool) DispatcherOption {
	return func(dispatcher *Dispatcher) {
		dispatcher.production = production
	}
}

// WithDispatchInterval configures how often the dispatcher polls for new events.
func WithDispatchInterval(interval time.Duration) DispatcherOption {
	return func(dispatcher *Dispatcher) {
		if interval > 0 {
			dispatcher.interval = interval
		}
	}
}

// NewDispatcher creates a new Dispatcher with the given dependencies.
func NewDispatcher(
	repo sharedPorts.OutboxRepository,
	ingestPub sharedPorts.IngestionEventPublisher,
	matchPub sharedDomain.MatchEventPublisher,
	logger libLog.Logger,
	tracer trace.Tracer,
	opts ...DispatcherOption,
) (*Dispatcher, error) {
	if repo == nil {
		return nil, ErrOutboxRepositoryRequired
	}

	if ingestPub == nil {
		return nil, ErrIngestionPublisherRequired
	}

	if matchPub == nil {
		return nil, ErrMatchingPublisherRequired
	}

	if tracer == nil {
		tracer = noop.NewTracerProvider().Tracer("commons.noop")
	}

	if logger == nil {
		logger = &libLog.NopLogger{}
	}

	dispatcher := &Dispatcher{
		repo:                        repo,
		ingestPub:                   ingestPub,
		matchPub:                    matchPub,
		logger:                      logger,
		tracer:                      tracer,
		interval:                    defaultDispatchInterval,
		batchSize:                   defaultBatchSize,
		publishMaxAttempts:          defaultPublishMaxAttempts,
		publishBackoff:              defaultPublishBackoff,
		listPendingFailureCounts:    make(map[string]int),
		listPendingFailureThreshold: defaultListPendingFailureThreshold,
		retryWindow:                 defaultRetryWindow,
		maxDispatchAttempts:         defaultMaxDispatchAttempts,
		processingTimeout:           defaultProcessingTimeout,
		stop:                        make(chan struct{}),
	}

	for _, opt := range opts {
		opt(dispatcher)
	}

	if err := dispatcher.initMetrics(); err != nil {
		return nil, fmt.Errorf("init outbox metrics: %w", err)
	}

	return dispatcher, nil
}

// initMetrics creates the OTel metric instruments for the dispatch cycle.
func (dispatcher *Dispatcher) initMetrics() error {
	meter := otel.Meter("matcher.outbox.dispatcher")

	var err error

	dispatcher.eventsDispatched, err = meter.Int64Counter("outbox.events.dispatched",
		metric.WithDescription("Number of outbox events successfully published"),
		metric.WithUnit("{event}"))
	if err != nil {
		return fmt.Errorf("create outbox.events.dispatched counter: %w", err)
	}

	dispatcher.eventsFailed, err = meter.Int64Counter("outbox.events.failed",
		metric.WithDescription("Number of outbox events that failed to publish"),
		metric.WithUnit("{event}"))
	if err != nil {
		return fmt.Errorf("create outbox.events.failed counter: %w", err)
	}

	dispatcher.dispatchLatency, err = meter.Float64Histogram("outbox.dispatch.latency",
		metric.WithDescription("Time taken per dispatch cycle"),
		metric.WithUnit("s"))
	if err != nil {
		return fmt.Errorf("create outbox.dispatch.latency histogram: %w", err)
	}

	dispatcher.queueDepth, err = meter.Int64Gauge("outbox.queue.depth",
		metric.WithDescription("Number of pending outbox events per tenant"),
		metric.WithUnit("{event}"))
	if err != nil {
		return fmt.Errorf("create outbox.queue.depth gauge: %w", err)
	}

	return nil
}

// SetRetryWindow configures the retry window for failed events.
func (dispatcher *Dispatcher) SetRetryWindow(window time.Duration) {
	if window > 0 {
		dispatcher.retryWindow = window
	}
}

// SetMaxDispatchAttempts configures the maximum dispatch attempts.
func (dispatcher *Dispatcher) SetMaxDispatchAttempts(attempts int) {
	if attempts > 0 {
		dispatcher.maxDispatchAttempts = attempts
	}
}

// SetProcessingTimeout configures how long a processing event can be stuck before retry.
func (dispatcher *Dispatcher) SetProcessingTimeout(timeout time.Duration) {
	if timeout > 0 {
		dispatcher.processingTimeout = timeout
	}
}

// Run starts the dispatcher loop until Stop is called.
func (dispatcher *Dispatcher) Run(_ *libCommons.Launcher) error {
	if dispatcher == nil {
		return ErrOutboxDispatcherRequired
	}

	if dispatcher.repo == nil || dispatcher.ingestPub == nil || dispatcher.matchPub == nil {
		return ErrOutboxDispatcherRequired
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dispatcher.cancelFunc = cancel

	defer runtime.RecoverAndLogWithContext(
		ctx,
		dispatcher.logger,
		constants.ApplicationName,
		"outbox.dispatcher_run",
	)

	ticker := time.NewTicker(dispatcher.interval)
	defer ticker.Stop()

	func() {
		dispatcher.dispatchWg.Add(1)
		defer dispatcher.dispatchWg.Done()

		initCtx, span := dispatcher.tracer.Start(ctx, "outbox.dispatcher.initial_dispatch")
		defer span.End()
		defer runtime.RecoverAndLogWithContext(
			initCtx,
			dispatcher.logger,
			constants.ApplicationName,
			"outbox.dispatcher_initial",
		)

		dispatcher.dispatchAcrossTenants(initCtx)
	}()

	for {
		select {
		case <-dispatcher.stop:
			return nil
		case <-ticker.C:
			select {
			case <-dispatcher.stop:
				return nil
			default:
			}

			func() {
				dispatcher.dispatchWg.Add(1)
				defer dispatcher.dispatchWg.Done()

				tickCtx, span := dispatcher.tracer.Start(ctx, "outbox.dispatcher.dispatch_once")
				defer span.End()
				defer runtime.RecoverAndLogWithContext(
					tickCtx,
					dispatcher.logger,
					constants.ApplicationName,
					"outbox.dispatcher_tick",
				)

				dispatcher.dispatchAcrossTenants(tickCtx)
			}()
		}
	}
}

// Stop signals the dispatcher to stop processing events.
func (dispatcher *Dispatcher) Stop() {
	if dispatcher == nil {
		return
	}

	dispatcher.stopOnce.Do(func() {
		if dispatcher.cancelFunc != nil {
			dispatcher.cancelFunc()
		}

		close(dispatcher.stop)
	})
}

// Shutdown gracefully stops the dispatcher. It signals the dispatch loop to stop
// accepting new ticks and then waits for any in-flight dispatch cycle to complete.
// If the provided context expires before draining completes, Shutdown returns the
// context error.
func (dispatcher *Dispatcher) Shutdown(ctx context.Context) error {
	if dispatcher == nil {
		return nil
	}

	dispatcher.Stop()

	done := make(chan struct{})

	runtime.SafeGo(dispatcher.logger, "outbox.dispatcher_shutdown_wait", runtime.KeepRunning, func() {
		dispatcher.dispatchWg.Wait()
		close(done)
	})

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("dispatcher shutdown: %w", ctx.Err())
	}
}

// DispatchOnce processes a single batch of pending outbox events.
// Returns the number of events that were processed (successfully or not).
//
// Ordering: Events are ordered by created_at within each category. Priority events
// (e.g., audit logs) are collected first. There is no strict per-aggregate ordering
// guarantee. Consumers should not assume a specific sequence of events.
func (dispatcher *Dispatcher) DispatchOnce(ctx context.Context) int {
	logger := dispatcher.logger
	start := time.Now().UTC()

	ctx, span := dispatcher.tracer.Start(ctx, "outbox.dispatch")
	defer span.End()

	events := dispatcher.collectEvents(ctx, span)
	processed := 0
	succeeded := 0
	failed := 0

	tenantKey := tenantKeyFromContext(ctx)
	tenantAttr := attribute.String("tenant", tenantKey)

	// Record queue depth: number of events collected for this tenant in this cycle.
	if dispatcher.queueDepth != nil {
		dispatcher.queueDepth.Record(ctx, int64(len(events)), metric.WithAttributes(tenantAttr))
	}

	// AT-LEAST-ONCE DELIVERY: Events are published to the message broker BEFORE being marked
	// as PUBLISHED in the database. If the process crashes between publish and mark, the event
	// will be re-dispatched after processingTimeout. All consumers MUST implement idempotency
	// using the event's ID to handle potential duplicates.
	for _, event := range events {
		if event == nil {
			continue
		}

		processed++

		if err := dispatcher.publishEventWithRetry(ctx, event); err != nil {
			dispatcher.handlePublishError(ctx, logger, event, err)

			failed++

			continue
		}

		if err := dispatcher.repo.MarkPublished(ctx, event.ID, time.Now().UTC()); err != nil {
			libLog.SafeError(logger, ctx, "failed to mark outbox published", err, dispatcher.production)
		}

		succeeded++
	}

	// Record dispatch cycle metrics.
	if dispatcher.eventsDispatched != nil && succeeded > 0 {
		dispatcher.eventsDispatched.Add(ctx, int64(succeeded), metric.WithAttributes(tenantAttr))
	}

	if dispatcher.eventsFailed != nil && failed > 0 {
		dispatcher.eventsFailed.Add(ctx, int64(failed), metric.WithAttributes(tenantAttr))
	}

	if dispatcher.dispatchLatency != nil {
		elapsed := time.Since(start).Seconds()
		dispatcher.dispatchLatency.Record(ctx, elapsed, metric.WithAttributes(tenantAttr))
	}

	return processed
}

func (dispatcher *Dispatcher) dispatchAcrossTenants(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	if logger == nil {
		logger = dispatcher.logger
	}

	if tracer == nil {
		tracer = dispatcher.tracer
	}

	if tracer == nil {
		tracer = noop.NewTracerProvider().Tracer("commons.noop")
	}

	ctx, span := tracer.Start(ctx, "outbox.dispatcher.tenants")
	defer span.End()

	tenants, err := dispatcher.repo.ListTenants(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list tenants", err)

		libLog.SafeError(logger, ctx, "failed to list tenants", err, dispatcher.production)

		return
	}

	for _, tenantID := range tenants {
		if tenantID == "" {
			continue
		}

		tenantCtx := context.WithValue(ctx, auth.TenantIDKey, tenantID)

		tenantCtx, tenantSpan := tracer.Start(tenantCtx, "outbox.dispatcher.tenant")
		_ = dispatcher.DispatchOnce(tenantCtx)

		tenantSpan.End()
	}
}

func (dispatcher *Dispatcher) collectEvents(
	ctx context.Context,
	span trace.Span,
) []*outboxEntities.OutboxEvent {
	logger := dispatcher.logger
	failedBefore := time.Now().UTC().Add(-dispatcher.currentRetryWindow())
	processingBefore := time.Now().UTC().Add(-dispatcher.processingTimeout)

	priorityBudget := min(defaultPriorityBudget, dispatcher.batchSize)

	priorityEvents := dispatcher.collectPriorityEvents(ctx, span, priorityBudget)
	collected := len(priorityEvents)

	stuckLimit := dispatcher.batchSize - collected
	if stuckLimit <= 0 {
		return priorityEvents
	}

	stuckEvents, err := dispatcher.repo.ResetStuckProcessing(
		ctx,
		stuckLimit,
		processingBefore,
		dispatcher.maxDispatchAttempts,
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to reset stuck events", err)
		libLog.SafeError(logger, ctx, "failed to reset stuck events", err, dispatcher.production)
	}

	collected += len(stuckEvents)

	failedLimit := min(dispatcher.batchSize-collected, defaultMaxFailedPerBatch)

	if failedLimit <= 0 {
		return append(priorityEvents, stuckEvents...)
	}

	failedEvents, err := dispatcher.repo.ResetForRetry(
		ctx,
		failedLimit,
		failedBefore,
		dispatcher.maxDispatchAttempts,
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to reset failed events for retry", err)
		libLog.SafeError(logger, ctx, "failed to reset failed events for retry", err, dispatcher.production)
	}

	collected += len(failedEvents)

	remaining := dispatcher.batchSize - collected
	if remaining <= 0 {
		return append(append(priorityEvents, stuckEvents...), failedEvents...)
	}

	pendingEvents, err := dispatcher.repo.ListPending(ctx, remaining)
	if err != nil {
		tenantKey := tenantKeyFromContext(ctx)
		dispatcher.handleListPendingError(ctx, span, tenantKey, err)

		return append(append(priorityEvents, stuckEvents...), failedEvents...)
	}

	tenantKey := tenantKeyFromContext(ctx)
	dispatcher.clearListPendingFailureCount(tenantKey)

	all := make([]*outboxEntities.OutboxEvent, 0, collected+len(pendingEvents))
	all = append(all, priorityEvents...)
	all = append(all, stuckEvents...)
	all = append(all, failedEvents...)
	all = append(all, pendingEvents...)

	return deduplicateEvents(all)
}

// deduplicateEvents removes duplicate events by ID, preserving order.
// Priority events appear first in the slice, so their position is retained
// while later duplicates from pending/failed queries are dropped.
func deduplicateEvents(events []*outboxEntities.OutboxEvent) []*outboxEntities.OutboxEvent {
	if len(events) == 0 {
		return events
	}

	seen := make(map[uuid.UUID]bool, len(events))
	result := make([]*outboxEntities.OutboxEvent, 0, len(events))

	for _, event := range events {
		if event == nil {
			continue
		}

		if seen[event.ID] {
			continue
		}

		seen[event.ID] = true

		result = append(result, event)
	}

	return result
}

// priorityEventTypes returns a fresh slice each call so the list is immutable.
func priorityEventTypes() []string {
	return []string{
		sharedDomain.EventTypeAuditLogCreated,
	}
}

func (dispatcher *Dispatcher) collectPriorityEvents(
	ctx context.Context,
	span trace.Span,
	budget int,
) []*outboxEntities.OutboxEvent {
	if budget <= 0 {
		return nil
	}

	var result []*outboxEntities.OutboxEvent

	for _, eventType := range priorityEventTypes() {
		remaining := budget - len(result)
		if remaining <= 0 {
			break
		}

		events, err := dispatcher.repo.ListPendingByType(ctx, eventType, remaining)
		if err != nil {
			libOpentelemetry.HandleSpanError(span, "failed to list priority events", err)
			libLog.SafeError(dispatcher.logger, ctx, "failed to list priority events", err, dispatcher.production)

			continue
		}

		result = append(result, events...)
	}

	return result
}

// tenantKeyFromContext extracts the tenant identifier from context for use as a map key.
func tenantKeyFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(auth.TenantIDKey).(string); ok && v != "" {
		return v
	}

	return "_default"
}

func (dispatcher *Dispatcher) handleListPendingError(ctx context.Context, span trace.Span, tenantKey string, err error) {
	logger := dispatcher.logger

	libOpentelemetry.HandleSpanError(span, "failed to list outbox events", err)
	libLog.SafeError(logger, ctx, "failed to list outbox events", err, dispatcher.production)

	dispatcher.failureCountsMu.Lock()
	dispatcher.listPendingFailureCounts[tenantKey]++
	count := dispatcher.listPendingFailureCounts[tenantKey]
	dispatcher.failureCountsMu.Unlock()

	if count == dispatcher.listPendingFailureThreshold {
		logger.Log(ctx, libLog.LevelError, "outbox list pending failures exceeded threshold",
			libLog.String("tenant", tenantKey),
			libLog.Int("count", count),
		)
	}
}

func (dispatcher *Dispatcher) clearListPendingFailureCount(tenantKey string) {
	dispatcher.failureCountsMu.Lock()
	delete(dispatcher.listPendingFailureCounts, tenantKey)
	dispatcher.failureCountsMu.Unlock()
}

func (dispatcher *Dispatcher) publishEventWithRetry(
	ctx context.Context,
	event *outboxEntities.OutboxEvent,
) error {
	maxAttempts := dispatcher.publishMaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = defaultPublishMaxAttempts
	}

	dispatcherBackoff := dispatcher.publishBackoff
	if dispatcherBackoff <= 0 {
		dispatcherBackoff = defaultPublishBackoff
	}

	var lastErr error

	for attempt := 0; attempt < maxAttempts; attempt++ {
		err := dispatcher.publishEvent(ctx, event)
		if err == nil {
			return nil
		}

		lastErr = fmt.Errorf("publish attempt %d/%d failed: %w", attempt+1, maxAttempts, err)
		if isNonRetryableError(err) || attempt == maxAttempts-1 {
			break
		}

		delay := backoff.ExponentialWithJitter(dispatcherBackoff, attempt)
		if waitErr := waitForRetry(ctx, delay); waitErr != nil {
			break
		}
	}

	return lastErr
}

func waitForRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return fmt.Errorf("outbox publish retry wait interrupted: %w", ctx.Err())
	case <-timer.C:
		return nil
	}
}

func (dispatcher *Dispatcher) publishEvent(
	ctx context.Context,
	event *outboxEntities.OutboxEvent,
) error {
	if event == nil {
		return fmt.Errorf("%w", outboxEntities.ErrOutboxEventRequired)
	}

	if len(event.Payload) == 0 {
		return fmt.Errorf("%w", ErrOutboxEventPayloadRequired)
	}

	switch event.EventType {
	case sharedDomain.EventTypeIngestionCompleted:
		return dispatcher.publishIngestionCompleted(ctx, event.Payload)
	case sharedDomain.EventTypeIngestionFailed:
		return dispatcher.publishIngestionFailed(ctx, event.Payload)
	case sharedDomain.EventTypeMatchConfirmed:
		return dispatcher.publishMatchConfirmed(ctx, event.Payload)
	case sharedDomain.EventTypeMatchUnmatched:
		return dispatcher.publishMatchUnmatched(ctx, event.Payload)
	case sharedDomain.EventTypeAuditLogCreated:
		return dispatcher.publishAuditLogCreated(ctx, event.Payload)
	default:
		return ErrUnsupportedEventType
	}
}

func (dispatcher *Dispatcher) publishIngestionCompleted(ctx context.Context, payload []byte) error {
	var event sharedDomain.IngestionCompletedEvent

	if err := json.Unmarshal(payload, &event); err != nil {
		return fmt.Errorf("ingestion completed %w: %w", ErrInvalidPayload, err)
	}

	if err := validateIngestionCompletedPayload(event); err != nil {
		return err
	}

	if err := dispatcher.ingestPub.PublishIngestionCompleted(ctx, &event); err != nil {
		return fmt.Errorf("publishing ingestion completed event: %w", err)
	}

	return nil
}

func (dispatcher *Dispatcher) publishIngestionFailed(ctx context.Context, payload []byte) error {
	var event sharedDomain.IngestionFailedEvent

	if err := json.Unmarshal(payload, &event); err != nil {
		return fmt.Errorf("ingestion failed %w: %w", ErrInvalidPayload, err)
	}

	if err := validateIngestionFailedPayload(event); err != nil {
		return err
	}

	if err := dispatcher.ingestPub.PublishIngestionFailed(ctx, &event); err != nil {
		return fmt.Errorf("publishing ingestion failed event: %w", err)
	}

	return nil
}

func (dispatcher *Dispatcher) publishMatchConfirmed(ctx context.Context, payload []byte) error {
	var event sharedDomain.MatchConfirmedEvent

	if err := json.Unmarshal(payload, &event); err != nil {
		return fmt.Errorf("match confirmed %w: %w", ErrInvalidPayload, err)
	}

	if err := validateMatchConfirmedPayload(event); err != nil {
		return err
	}

	if err := dispatcher.matchPub.PublishMatchConfirmed(ctx, &event); err != nil {
		return fmt.Errorf("publishing match confirmed event: %w", err)
	}

	return nil
}

func (dispatcher *Dispatcher) publishMatchUnmatched(ctx context.Context, payload []byte) error {
	var event sharedDomain.MatchUnmatchedEvent

	if err := json.Unmarshal(payload, &event); err != nil {
		return fmt.Errorf("match unmatched %w: %w", ErrInvalidPayload, err)
	}

	if err := validateMatchUnmatchedPayload(event); err != nil {
		return err
	}

	if err := dispatcher.matchPub.PublishMatchUnmatched(ctx, &event); err != nil {
		return fmt.Errorf("publishing match unmatched event: %w", err)
	}

	return nil
}

func (dispatcher *Dispatcher) publishAuditLogCreated(ctx context.Context, payload []byte) error {
	if sharedPorts.IsNilValue(dispatcher.auditPub) {
		return ErrAuditPublisherNotConfigured
	}

	var event sharedDomain.AuditLogCreatedEvent

	if err := json.Unmarshal(payload, &event); err != nil {
		return fmt.Errorf("audit log created %w: %w", ErrInvalidPayload, err)
	}

	if err := validateAuditLogCreatedPayload(event); err != nil {
		return err
	}

	if err := dispatcher.auditPub.PublishAuditLogCreated(ctx, &event); err != nil {
		return fmt.Errorf("publishing audit log created event: %w", err)
	}

	return nil
}

func validateIngestionCompletedPayload(payload sharedDomain.IngestionCompletedEvent) error {
	if payload.JobID == uuid.Nil {
		return fmt.Errorf("ingestion completed: %w", ErrMissingJobID)
	}

	if payload.ContextID == uuid.Nil {
		return fmt.Errorf("ingestion completed: %w", ErrMissingContextID)
	}

	if payload.SourceID == uuid.Nil {
		return fmt.Errorf("ingestion completed: %w", ErrMissingSourceID)
	}

	return nil
}

func validateIngestionFailedPayload(payload sharedDomain.IngestionFailedEvent) error {
	if payload.JobID == uuid.Nil {
		return fmt.Errorf("ingestion failed: %w", ErrMissingJobID)
	}

	if payload.ContextID == uuid.Nil {
		return fmt.Errorf("ingestion failed: %w", ErrMissingContextID)
	}

	if payload.SourceID == uuid.Nil {
		return fmt.Errorf("ingestion failed: %w", ErrMissingSourceID)
	}

	if payload.Error == "" {
		return fmt.Errorf("ingestion failed: %w", ErrMissingError)
	}

	return nil
}

func validateMatchConfirmedPayload(payload sharedDomain.MatchConfirmedEvent) error {
	if payload.TenantID == uuid.Nil {
		return fmt.Errorf("match confirmed: %w", ErrMissingTenantID)
	}

	if payload.ContextID == uuid.Nil {
		return fmt.Errorf("match confirmed: %w", ErrMissingContextID)
	}

	if payload.RunID == uuid.Nil {
		return fmt.Errorf("match confirmed: %w", ErrMissingMatchRunID)
	}

	if payload.MatchID == uuid.Nil {
		return fmt.Errorf("match confirmed: %w", ErrMissingMatchID)
	}

	if payload.RuleID == uuid.Nil {
		return fmt.Errorf("match confirmed: %w", ErrMissingMatchRuleID)
	}

	if len(payload.TransactionIDs) == 0 {
		return fmt.Errorf("match confirmed: %w", ErrMissingTransactionIDs)
	}

	return nil
}

func validateMatchUnmatchedPayload(payload sharedDomain.MatchUnmatchedEvent) error {
	if payload.TenantID == uuid.Nil {
		return fmt.Errorf("match unmatched: %w", ErrMissingTenantID)
	}

	if payload.ContextID == uuid.Nil {
		return fmt.Errorf("match unmatched: %w", ErrMissingContextID)
	}

	if payload.RunID == uuid.Nil {
		return fmt.Errorf("match unmatched: %w", ErrMissingMatchRunID)
	}

	if payload.MatchID == uuid.Nil {
		return fmt.Errorf("match unmatched: %w", ErrMissingMatchID)
	}

	if len(payload.TransactionIDs) == 0 {
		return fmt.Errorf("match unmatched: %w", ErrMissingTransactionIDs)
	}

	if payload.Reason == "" {
		return fmt.Errorf("match unmatched: %w", ErrMissingReason)
	}

	return nil
}

func validateAuditLogCreatedPayload(payload sharedDomain.AuditLogCreatedEvent) error {
	if payload.TenantID == uuid.Nil {
		return fmt.Errorf("audit log created: %w", ErrMissingTenantID)
	}

	if payload.EntityType == "" {
		return fmt.Errorf("audit log created: %w", ErrMissingEntityType)
	}

	if payload.EntityID == uuid.Nil {
		return fmt.Errorf("audit log created: %w", ErrMissingEntityID)
	}

	if payload.Action == "" {
		return fmt.Errorf("audit log created: %w", ErrMissingAction)
	}

	return nil
}

// sanitizeErrorForStorage redacts sensitive values and enforces bounded length
// before storing error messages in the last_error database column (CWE-209).
const maxErrorLength = 256

const errorTruncatedSuffix = "... (truncated)"

const redactedValue = "[REDACTED]"

type sensitiveDataPattern struct {
	pattern     *regexp.Regexp
	replacement string
}

var sensitiveDataPatterns = []sensitiveDataPattern{
	{
		pattern:     regexp.MustCompile(`(?i)\bbearer\s+[a-z0-9\-._~+/]+=*\b`),
		replacement: "Bearer " + redactedValue,
	},
	{
		pattern:     regexp.MustCompile(`\beyJ[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\b`),
		replacement: redactedValue,
	},
	{
		pattern:     regexp.MustCompile(`(?i)\b(api[-_ ]?key|access[-_ ]?token|refresh[-_ ]?token|password|secret)\s*[:=]\s*([^\s,;]+)`),
		replacement: `$1=` + redactedValue,
	},
	{
		pattern:     regexp.MustCompile(`(?i)\b[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}\b`),
		replacement: redactedValue,
	},
	{
		pattern:     regexp.MustCompile(`\b\d{12,19}\b`),
		replacement: redactedValue,
	},
}

func sanitizeErrorForStorage(err error) string {
	if err == nil {
		return ""
	}

	msg := err.Error()
	redacted := redactSensitiveData(msg)

	return truncateError(redacted, maxErrorLength, errorTruncatedSuffix)
}

func redactSensitiveData(msg string) string {
	redacted := msg

	for _, matcher := range sensitiveDataPatterns {
		redacted = matcher.pattern.ReplaceAllString(redacted, matcher.replacement)
	}

	return redacted
}

func truncateError(msg string, maxRunes int, suffix string) string {
	runes := []rune(msg)
	if len(runes) <= maxRunes {
		return msg
	}

	suffixRunes := []rune(suffix)
	if maxRunes <= len(suffixRunes) {
		return string(runes[:maxRunes])
	}

	trimmed := string(runes[:maxRunes-len(suffixRunes)])

	return trimmed + suffix
}

// handlePublishError marks an event as invalid or failed based on error type.
func (dispatcher *Dispatcher) handlePublishError(
	ctx context.Context,
	logger libLog.Logger,
	event *outboxEntities.OutboxEvent,
	err error,
) {
	if isNonRetryableError(err) {
		if markErr := dispatcher.repo.MarkInvalid(ctx, event.ID, sanitizeErrorForStorage(err)); markErr != nil {
			logger.Log(ctx, libLog.LevelError, "failed to mark outbox invalid",
				libLog.String("error", sanitizeErrorForStorage(markErr)),
			)
		}

		return
	}

	if markErr := dispatcher.repo.MarkFailed(ctx, event.ID, sanitizeErrorForStorage(err), dispatcher.maxDispatchAttempts); markErr != nil {
		logger.Log(ctx, libLog.LevelError, "failed to mark outbox failed",
			libLog.String("error", sanitizeErrorForStorage(markErr)),
		)
	}
}

// nonRetryableErrors lists all errors that indicate permanent validation failures.
var nonRetryableErrors = []error{
	ErrUnsupportedEventType,
	ErrInvalidPayload,
	ErrMissingJobID,
	ErrMissingContextID,
	ErrMissingSourceID,
	ErrMissingError,
	ErrMissingMatchID,
	ErrMissingMatchRunID,
	ErrMissingMatchRuleID,
	ErrMissingTransactionIDs,
	ErrMissingTenantID,
	outboxEntities.ErrOutboxEventRequired,
	ErrOutboxEventPayloadRequired,
	ErrMissingEntityType,
	ErrMissingEntityID,
	ErrMissingAction,
	ErrMissingReason,
}

// isNonRetryableError checks if an error is a permanent validation failure.
func isNonRetryableError(err error) bool {
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
