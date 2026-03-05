// Package outbox provides PostgreSQL-based outbox event persistence.
package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-uncommons/v2/uncommons"
	libLog "github.com/LerianStudio/lib-uncommons/v2/uncommons/log"
	libOpentelemetry "github.com/LerianStudio/lib-uncommons/v2/uncommons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/auth"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

// Sentinel errors for repository operations.
var (
	ErrRepositoryNotInitialized  = errors.New("outbox repository not initialized")
	ErrLimitMustBePositive       = errors.New("limit must be greater than zero")
	ErrIDRequired                = errors.New("id is required")
	ErrMaxAttemptsMustBePositive = errors.New("maxAttempts must be greater than zero")
	ErrEventTypeRequired         = errors.New("event type is required")
)

const outboxColumns = "id, event_type, aggregate_id, payload, status, attempts, published_at, last_error, created_at, updated_at"

// uuidSchemaRegex matches PostgreSQL schema names that follow the UUID format.
// In multi-tenant mode, each tenant's data is isolated in a schema named after
// its tenant UUID (e.g., "550e8400-e29b-41d4-a716-446655440000"). This regex
// is used to discover tenant schemas from pg_namespace for outbox event dispatch.
const uuidSchemaRegex = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"

// Repository implements outbox event persistence using PostgreSQL.
type Repository struct {
	provider ports.InfrastructureProvider
}

// GetByID retrieves an outbox event by ID.
func (repo *Repository) GetByID(ctx context.Context, id uuid.UUID) (*sharedDomain.OutboxEvent, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepositoryNotInitialized
	}

	if id == uuid.Nil {
		return nil, ErrIDRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.get_outbox_by_id")

	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*sharedDomain.OutboxEvent, error) {
			row := tx.QueryRowContext(
				ctx,
				"SELECT "+outboxColumns+" FROM outbox_events WHERE id = $1",
				id,
			)

			return scanOutboxEvent(row)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to get outbox event", err)

			logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to get outbox event: %v", err))
		}

		return nil, fmt.Errorf("getting outbox event: %w", err)
	}

	return result, nil
}

// NewRepository creates a new outbox Repository with the given provider.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

// Create stores a new outbox event using a new transaction.
func (repo *Repository) Create(
	ctx context.Context,
	event *sharedDomain.OutboxEvent,
) (*sharedDomain.OutboxEvent, error) {
	return repo.create(ctx, nil, event)
}

// CreateWithTx stores a new outbox event using an existing transaction.
func (repo *Repository) CreateWithTx(
	ctx context.Context,
	tx *sql.Tx,
	event *sharedDomain.OutboxEvent,
) (*sharedDomain.OutboxEvent, error) {
	return repo.create(ctx, tx, event)
}

func (repo *Repository) create(
	ctx context.Context,
	tx *sql.Tx,
	event *sharedDomain.OutboxEvent,
) (*sharedDomain.OutboxEvent, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepositoryNotInitialized
	}

	if event == nil {
		return nil, sharedDomain.ErrOutboxEventRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.create_outbox_event")
	defer span.End()

	result, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(execTx *sql.Tx) (*sharedDomain.OutboxEvent, error) {
			query := `INSERT INTO outbox_events (id, event_type, aggregate_id, payload, status, attempts, published_at, last_error, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			RETURNING ` + outboxColumns

			row := execTx.QueryRowContext(ctx, query,
				event.ID,
				event.EventType,
				event.AggregateID,
				event.Payload,
				event.Status,
				event.Attempts,
				event.PublishedAt,
				event.LastError,
				event.CreatedAt,
				event.UpdatedAt,
			)

			return scanOutboxEvent(row)
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to create outbox event", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to create outbox event: %v", err))

		return nil, fmt.Errorf("creating outbox event: %w", err)
	}

	return result, nil
}

// ListPending retrieves pending outbox events up to the given limit.
func (repo *Repository) ListPending(
	ctx context.Context,
	limit int,
) ([]*sharedDomain.OutboxEvent, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepositoryNotInitialized
	}

	if limit <= 0 {
		return nil, ErrLimitMustBePositive
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.list_outbox_pending")
	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) ([]*sharedDomain.OutboxEvent, error) {
			events, err := listPendingRows(ctx, tx, limit)
			if err != nil {
				return nil, err
			}

			if len(events) == 0 {
				return events, nil
			}

			ids := collectEventIDs(events)
			if len(ids) == 0 {
				return events, nil
			}

			now := time.Now().UTC()
			if err := markEventsProcessing(ctx, tx, now, ids); err != nil {
				return nil, err
			}

			applyProcessingState(events, now)

			return events, nil
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list outbox events", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to list outbox events: %v", err))

		return nil, fmt.Errorf("listing pending events: %w", err)
	}

	return result, nil
}

// ListTenants returns tenant IDs based on database schemas.
// The default tenant (public schema) is always included regardless of
// whether a UUID-named schema exists for it.
func (repo *Repository) ListTenants(ctx context.Context) ([]string, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepositoryNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.list_outbox_tenants")
	defer span.End()

	result, err := pgcommon.WithTenantRead(
		ctx,
		repo.provider,
		func(conn *sql.Conn) ([]string, error) {
			rows, err := conn.QueryContext(
				ctx,
				"SELECT nspname FROM pg_namespace WHERE nspname ~* $1",
				uuidSchemaRegex,
			)
			if err != nil {
				return nil, fmt.Errorf("querying tenant schemas: %w", err)
			}
			defer rows.Close()

			var tenants []string

			for rows.Next() {
				var tenant string
				if scanErr := rows.Scan(&tenant); scanErr != nil {
					return nil, fmt.Errorf("scanning tenant schema: %w", scanErr)
				}

				tenants = append(tenants, tenant)
			}

			if err := rows.Err(); err != nil {
				return nil, fmt.Errorf("iterating tenant schemas: %w", err)
			}

			// The default tenant uses the public schema (no UUID-named schema
			// in pg_namespace), so the query above will never discover it.
			// Always ensure it is included so its outbox events are dispatched.
			defaultTenantID := auth.GetDefaultTenantID()
			if defaultTenantID != "" && !slices.Contains(tenants, defaultTenantID) {
				tenants = append(tenants, defaultTenantID)
			}

			return tenants, nil
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list tenant schemas", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to list tenant schemas: %v", err))

		return nil, fmt.Errorf("list tenant schemas: %w", err)
	}

	return result, nil
}

func listPendingRows(ctx context.Context, tx *sql.Tx, limit int) ([]*sharedDomain.OutboxEvent, error) {
	query := `SELECT ` + outboxColumns + ` FROM outbox_events
		WHERE status = $1
		ORDER BY created_at ASC
		LIMIT $2
		FOR UPDATE SKIP LOCKED`

	rows, err := tx.QueryContext(ctx, query, sharedDomain.OutboxStatusPending, limit)
	if err != nil {
		return nil, fmt.Errorf("querying pending events: %w", err)
	}

	defer rows.Close()

	events := make([]*sharedDomain.OutboxEvent, 0, limit)

	for rows.Next() {
		event, err := scanOutboxEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("scanning outbox event: %w", err)
		}

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}

	return events, nil
}

// ListPendingByType retrieves pending outbox events filtered by event type up to the given limit.
func (repo *Repository) ListPendingByType(
	ctx context.Context,
	eventType string,
	limit int,
) ([]*sharedDomain.OutboxEvent, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepositoryNotInitialized
	}

	if limit <= 0 {
		return nil, ErrLimitMustBePositive
	}

	if eventType == "" {
		return nil, ErrEventTypeRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.list_outbox_pending_by_type")
	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) ([]*sharedDomain.OutboxEvent, error) {
			events, err := listPendingByTypeRows(ctx, tx, eventType, limit)
			if err != nil {
				return nil, err
			}

			if len(events) == 0 {
				return events, nil
			}

			ids := collectEventIDs(events)
			if len(ids) == 0 {
				return events, nil
			}

			now := time.Now().UTC()
			if err := markEventsProcessing(ctx, tx, now, ids); err != nil {
				return nil, err
			}

			applyProcessingState(events, now)

			return events, nil
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list outbox events by type", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to list outbox events by type: %v", err))

		return nil, fmt.Errorf("listing pending events by type: %w", err)
	}

	return result, nil
}

func listPendingByTypeRows(
	ctx context.Context,
	tx *sql.Tx,
	eventType string,
	limit int,
) ([]*sharedDomain.OutboxEvent, error) {
	query := `SELECT ` + outboxColumns + ` FROM outbox_events
		WHERE status = $1 AND event_type = $2
		ORDER BY created_at ASC
		LIMIT $3
		FOR UPDATE SKIP LOCKED`

	rows, err := tx.QueryContext(ctx, query, sharedDomain.OutboxStatusPending, eventType, limit)
	if err != nil {
		return nil, fmt.Errorf("querying pending events by type: %w", err)
	}

	defer rows.Close()

	events := make([]*sharedDomain.OutboxEvent, 0, limit)

	for rows.Next() {
		event, err := scanOutboxEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("scanning outbox event: %w", err)
		}

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}

	return events, nil
}

func collectEventIDs(events []*sharedDomain.OutboxEvent) []uuid.UUID {
	ids := make([]uuid.UUID, 0, len(events))

	for _, event := range events {
		if event == nil || event.ID == uuid.Nil {
			continue
		}

		ids = append(ids, event.ID)
	}

	return ids
}

// MarkPublished marks an outbox event as published.
func (repo *Repository) MarkPublished(
	ctx context.Context,
	id uuid.UUID,
	publishedAt time.Time,
) error {
	if repo == nil || repo.provider == nil {
		return ErrRepositoryNotInitialized
	}

	if id == uuid.Nil {
		return ErrIDRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.mark_outbox_published")
	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (struct{}, error) {
		query := `UPDATE outbox_events
			SET status = $1::outbox_event_status, published_at = $2, updated_at = $3
			WHERE id = $4 AND status <> $5::outbox_event_status AND status <> $6::outbox_event_status`
		if _, execErr := tx.ExecContext(
			ctx,
			query,
			sharedDomain.OutboxStatusPublished,
			publishedAt,
			time.Now().UTC(),
			id,
			sharedDomain.OutboxStatusPublished,
			sharedDomain.OutboxStatusInvalid,
		); execErr != nil {
			return struct{}{}, fmt.Errorf("executing update: %w", execErr)
		}

		return struct{}{}, nil
	})
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to mark outbox published", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to mark outbox published: %v", err))

		return fmt.Errorf("marking published: %w", err)
	}

	return nil
}

// MarkFailed marks an outbox event as failed with the given error message.
// When the incremented attempt count reaches maxAttempts, the event is atomically
// transitioned to INVALID status to prevent infinite retry loops.
func (repo *Repository) MarkFailed(ctx context.Context, id uuid.UUID, errMsg string, maxAttempts int) error {
	if repo == nil || repo.provider == nil {
		return ErrRepositoryNotInitialized
	}

	if id == uuid.Nil {
		return ErrIDRequired
	}

	if maxAttempts <= 0 {
		return ErrMaxAttemptsMustBePositive
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.mark_outbox_failed")
	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (struct{}, error) {
		query := `UPDATE outbox_events SET
			status = CASE WHEN attempts + 1 >= $1 THEN $2 ELSE $3 END::outbox_event_status,
			attempts = attempts + 1,
			last_error = CASE WHEN attempts + 1 >= $1 THEN $4 ELSE $5 END,
			updated_at = $6
			WHERE id = $7 AND status <> $8::outbox_event_status AND status <> $9::outbox_event_status`
		if _, execErr := tx.ExecContext(ctx, query,
			maxAttempts,
			sharedDomain.OutboxStatusInvalid,
			sharedDomain.OutboxStatusFailed,
			"max dispatch attempts exceeded",
			errMsg,
			time.Now().UTC(),
			id,
			sharedDomain.OutboxStatusPublished,
			sharedDomain.OutboxStatusInvalid,
		); execErr != nil {
			return struct{}{}, fmt.Errorf("executing update: %w", execErr)
		}

		return struct{}{}, nil
	})
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to mark outbox failed", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to mark outbox failed: %v", err))

		return fmt.Errorf("marking failed: %w", err)
	}

	return nil
}

func scanOutboxEvent(scanner interface{ Scan(dest ...any) error }) (*sharedDomain.OutboxEvent, error) {
	var event sharedDomain.OutboxEvent

	var lastError sql.NullString

	if err := scanner.Scan(
		&event.ID,
		&event.EventType,
		&event.AggregateID,
		&event.Payload,
		&event.Status,
		&event.Attempts,
		&event.PublishedAt,
		&lastError,
		&event.CreatedAt,
		&event.UpdatedAt,
	); err != nil {
		return nil, fmt.Errorf("scanning outbox event: %w", err)
	}

	if lastError.Valid {
		event.LastError = lastError.String
	}

	return &event, nil
}

// ListFailedForRetry is an administrative/diagnostic query for inspecting failed events.
// It is NOT used in the dispatch path -- the dispatcher uses ResetForRetry instead,
// which atomically lists and re-claims events in a single transaction.
func (repo *Repository) ListFailedForRetry(
	ctx context.Context,
	limit int,
	failedBefore time.Time,
	maxAttempts int,
) ([]*sharedDomain.OutboxEvent, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepositoryNotInitialized
	}

	if limit <= 0 {
		return nil, ErrLimitMustBePositive
	}

	if maxAttempts <= 0 {
		return nil, ErrMaxAttemptsMustBePositive
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.list_failed_for_retry")
	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) ([]*sharedDomain.OutboxEvent, error) {
			query := `SELECT ` + outboxColumns + ` FROM outbox_events
			WHERE status = $1 AND attempts < $2 AND updated_at <= $3
			ORDER BY updated_at ASC
			LIMIT $4`

			rows, err := tx.QueryContext(
				ctx,
				query,
				sharedDomain.OutboxStatusFailed,
				maxAttempts,
				failedBefore,
				limit,
			)
			if err != nil {
				return nil, fmt.Errorf("querying failed events: %w", err)
			}

			defer rows.Close()

			events := make([]*sharedDomain.OutboxEvent, 0, limit)

			for rows.Next() {
				event, err := scanOutboxEvent(rows)
				if err != nil {
					return nil, fmt.Errorf("scanning outbox event: %w", err)
				}

				events = append(events, event)
			}

			if err := rows.Err(); err != nil {
				return nil, fmt.Errorf("iterating rows: %w", err)
			}

			return events, nil
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list failed events for retry", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to list failed events for retry: %v", err))

		return nil, fmt.Errorf("listing failed events for retry: %w", err)
	}

	return result, nil
}

// ResetForRetry atomically selects and resets failed events to PROCESSING for retry.
func (repo *Repository) ResetForRetry(
	ctx context.Context,
	limit int,
	failedBefore time.Time,
	maxAttempts int,
) ([]*sharedDomain.OutboxEvent, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepositoryNotInitialized
	}

	if limit <= 0 {
		return nil, ErrLimitMustBePositive
	}

	if maxAttempts <= 0 {
		return nil, ErrMaxAttemptsMustBePositive
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.reset_for_retry")
	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) ([]*sharedDomain.OutboxEvent, error) {
			events, err := listFailedForRetryRows(ctx, tx, limit, failedBefore, maxAttempts)
			if err != nil {
				return nil, err
			}

			if len(events) == 0 {
				return events, nil
			}

			ids := collectEventIDs(events)
			if len(ids) == 0 {
				return events, nil
			}

			now := time.Now().UTC()
			if err := markEventsProcessing(ctx, tx, now, ids); err != nil {
				return nil, err
			}

			applyProcessingState(events, now)

			return events, nil
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to reset events for retry", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to reset events for retry: %v", err))

		return nil, fmt.Errorf("resetting events for retry: %w", err)
	}

	return result, nil
}

func listFailedForRetryRows(
	ctx context.Context,
	tx *sql.Tx,
	limit int,
	failedBefore time.Time,
	maxAttempts int,
) ([]*sharedDomain.OutboxEvent, error) {
	query := `SELECT ` + outboxColumns + ` FROM outbox_events
		WHERE status = $1 AND attempts < $2 AND updated_at <= $3
		ORDER BY updated_at ASC
		LIMIT $4
		FOR UPDATE SKIP LOCKED`

	rows, err := tx.QueryContext(
		ctx,
		query,
		sharedDomain.OutboxStatusFailed,
		maxAttempts,
		failedBefore,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("querying failed events for retry: %w", err)
	}

	defer rows.Close()

	events := make([]*sharedDomain.OutboxEvent, 0, limit)

	for rows.Next() {
		event, err := scanOutboxEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("scanning outbox event: %w", err)
		}

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}

	return events, nil
}

func markEventsProcessing(ctx context.Context, tx *sql.Tx, now time.Time, ids []uuid.UUID) error {
	return markEventsWithStatus(ctx, tx, now, sharedDomain.OutboxStatusProcessing, ids)
}

func markEventsWithStatus(
	ctx context.Context,
	tx *sql.Tx,
	now time.Time,
	status sharedDomain.OutboxEventStatus,
	ids []uuid.UUID,
) error {
	updateQuery := `UPDATE outbox_events SET status = $1::outbox_event_status, updated_at = $2 WHERE id = ANY($3::uuid[])`

	if _, err := tx.ExecContext(ctx, updateQuery, status, now, ids); err != nil {
		return fmt.Errorf("updating status to %s: %w", status, err)
	}

	return nil
}

func applyProcessingState(events []*sharedDomain.OutboxEvent, now time.Time) {
	applyStatusState(events, now, sharedDomain.OutboxStatusProcessing)
}

func applyStatusState(events []*sharedDomain.OutboxEvent, now time.Time, status sharedDomain.OutboxEventStatus) {
	for _, event := range events {
		if event == nil {
			continue
		}

		event.Status = status
		event.UpdatedAt = now
	}
}

// ResetStuckProcessing reclaims long-running processing events for the current dispatch cycle.
// Events are kept in PROCESSING status because they are returned to the calling dispatcher
// and will be published in this cycle. Using PROCESSING (rather than PENDING) prevents other
// dispatcher instances from picking them up concurrently.
// Events that have exceeded maxAttempts are not reset to prevent infinite loops.
func (repo *Repository) ResetStuckProcessing(
	ctx context.Context,
	limit int,
	processingBefore time.Time,
	maxAttempts int,
) ([]*sharedDomain.OutboxEvent, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepositoryNotInitialized
	}

	if limit <= 0 {
		return nil, ErrLimitMustBePositive
	}

	if maxAttempts <= 0 {
		return nil, ErrMaxAttemptsMustBePositive
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.reset_outbox_processing")
	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) ([]*sharedDomain.OutboxEvent, error) {
			events, err := listStuckProcessingRows(ctx, tx, limit, processingBefore, maxAttempts)
			if err != nil {
				return nil, err
			}

			if len(events) == 0 {
				return events, nil
			}

			ids := collectEventIDs(events)
			if len(ids) == 0 {
				return events, nil
			}

			now := time.Now().UTC()
			if err := markEventsProcessing(ctx, tx, now, ids); err != nil {
				return nil, err
			}

			applyProcessingState(events, now)

			return events, nil
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to reset stuck events", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to reset stuck events: %v", err))

		return nil, fmt.Errorf("reset stuck events: %w", err)
	}

	return result, nil
}

func listStuckProcessingRows(
	ctx context.Context,
	tx *sql.Tx,
	limit int,
	processingBefore time.Time,
	maxAttempts int,
) ([]*sharedDomain.OutboxEvent, error) {
	query := `SELECT ` + outboxColumns + ` FROM outbox_events
		WHERE status = $1 AND updated_at <= $2 AND attempts < $3
		ORDER BY updated_at ASC
		LIMIT $4
		FOR UPDATE SKIP LOCKED`

	rows, err := tx.QueryContext(
		ctx,
		query,
		sharedDomain.OutboxStatusProcessing,
		processingBefore,
		maxAttempts,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("querying stuck events: %w", err)
	}
	defer rows.Close()

	events := make([]*sharedDomain.OutboxEvent, 0, limit)

	for rows.Next() {
		event, err := scanOutboxEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("scanning outbox event: %w", err)
		}

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}

	return events, nil
}

// MarkInvalid marks an outbox event as invalid (non-retryable permanent failure).
func (repo *Repository) MarkInvalid(ctx context.Context, id uuid.UUID, errMsg string) error {
	if repo == nil || repo.provider == nil {
		return ErrRepositoryNotInitialized
	}

	if id == uuid.Nil {
		return ErrIDRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.mark_outbox_invalid")
	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (struct{}, error) {
		query := `UPDATE outbox_events
			SET status = $1::outbox_event_status, last_error = $2, updated_at = $3
			WHERE id = $4 AND status <> $5::outbox_event_status AND status <> $6::outbox_event_status`
		if _, execErr := tx.ExecContext(
			ctx,
			query,
			sharedDomain.OutboxStatusInvalid,
			errMsg,
			time.Now().UTC(),
			id,
			sharedDomain.OutboxStatusPublished,
			sharedDomain.OutboxStatusInvalid,
		); execErr != nil {
			return struct{}{}, fmt.Errorf("executing update: %w", execErr)
		}

		return struct{}{}, nil
	})
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to mark outbox invalid", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to mark outbox invalid: %v", err))

		return fmt.Errorf("marking invalid: %w", err)
	}

	return nil
}
