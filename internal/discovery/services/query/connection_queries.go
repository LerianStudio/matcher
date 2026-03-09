package query

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v4/commons/runtime"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// DiscoveryStatus represents the overall status of Fetcher integration.
type DiscoveryStatus struct {
	FetcherHealthy  bool      `json:"fetcherHealthy"`
	ConnectionCount int       `json:"connectionCount"`
	LastSyncAt      time.Time `json:"lastSyncAt,omitempty"`
}

// GetDiscoveryStatus returns the current discovery status including
// Fetcher health, total connection count, and the most recent sync timestamp.
func (uc *UseCase) GetDiscoveryStatus(ctx context.Context) (*DiscoveryStatus, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "query.discovery.get_status")
	defer span.End()

	healthy := uc.fetcherClient.IsHealthy(ctx)

	conns, err := uc.connRepo.FindAll(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "list connections for status", err)

		return nil, fmt.Errorf("list connections: %w", err)
	}

	status := &DiscoveryStatus{
		FetcherHealthy:  healthy,
		ConnectionCount: len(conns),
	}

	// Find the most recent LastSeenAt as a proxy for last sync time.
	for _, conn := range conns {
		if conn == nil {
			continue
		}

		if conn.LastSeenAt.After(status.LastSyncAt) {
			status.LastSyncAt = conn.LastSeenAt
		}
	}

	return status, nil
}

// ListConnections returns all discovered Fetcher connections.
func (uc *UseCase) ListConnections(ctx context.Context) ([]*entities.FetcherConnection, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "query.discovery.list_connections")
	defer span.End()

	conns, err := uc.connRepo.FindAll(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "list connections", err)

		return nil, fmt.Errorf("list connections: %w", err)
	}

	return conns, nil
}

// GetConnection returns a single connection by its internal ID.
func (uc *UseCase) GetConnection(ctx context.Context, id uuid.UUID) (*entities.FetcherConnection, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "query.discovery.get_connection")
	defer span.End()

	conn, err := uc.connRepo.FindByID(ctx, id)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "get connection", err)

		// Use domain-level sentinel from repositories package for proper error matching.
		if errors.Is(err, repositories.ErrConnectionNotFound) {
			return nil, ErrConnectionNotFound
		}

		return nil, fmt.Errorf("get connection: %w", err)
	}

	return conn, nil
}

// GetConnectionSchema returns all discovered table schemas for a connection.
func (uc *UseCase) GetConnectionSchema(ctx context.Context, connectionID uuid.UUID) ([]*entities.DiscoveredSchema, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "query.discovery.get_connection_schema")
	defer span.End()

	// Try cache first (if configured).
	if uc.schemaCache != nil {
		cached, err := uc.schemaCache.GetSchema(ctx, connectionID.String())
		if err == nil && cached != nil {
			// Convert FetcherSchema to domain entities.
			return convertFetcherSchemaToEntities(ctx, connectionID, cached), nil
		}
		// Cache miss or error — fall through to DB.
	}

	schemas, err := uc.schemaRepo.FindByConnectionID(ctx, connectionID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "get connection schema", err)

		return nil, fmt.Errorf("get connection schema: %w", err)
	}

	// Populate cache asynchronously (if configured).
	if uc.schemaCache != nil && len(schemas) > 0 {
		detachedCtx := context.WithoutCancel(ctx)
		connIDCopy := connectionID
		schemasCopy := schemas

		runtime.SafeGoWithContextAndComponent(
			detachedCtx,
			uc.logger,
			"discovery",
			"cache_schemas",
			runtime.KeepRunning,
			func(goCtx context.Context) { uc.cacheSchemas(goCtx, connIDCopy, schemasCopy) },
		)
	}

	return schemas, nil
}

// convertFetcherSchemaToEntities converts a FetcherSchema to domain entities.
// Invalid tables (e.g., empty table names) are logged and skipped.
func convertFetcherSchemaToEntities(ctx context.Context, connectionID uuid.UUID, schema *sharedPorts.FetcherSchema) []*entities.DiscoveredSchema {
	logger, _, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // only logger needed from tracking context

	result := make([]*entities.DiscoveredSchema, 0, len(schema.Tables))

	for _, table := range schema.Tables {
		cols := make([]entities.ColumnInfo, 0, len(table.Columns))
		for _, col := range table.Columns {
			cols = append(cols, entities.ColumnInfo{
				Name:     col.Name,
				Type:     col.Type,
				Nullable: col.Nullable,
			})
		}

		discovered, err := entities.NewDiscoveredSchema(ctx, connectionID, table.TableName, cols)
		if err != nil {
			if logger != nil {
				logger.With(
					libLog.Any("table", table.TableName),
					libLog.Any("connectionID", connectionID.String()),
					libLog.Any("error", err.Error()),
				).Log(ctx, libLog.LevelWarn, "skipping invalid cached schema entry during conversion")
			}

			continue
		}

		result = append(result, discovered)
	}

	return result
}

// cacheSchemas stores schemas in the cache asynchronously.
func (uc *UseCase) cacheSchemas(ctx context.Context, connectionID uuid.UUID, schemas []*entities.DiscoveredSchema) {
	// Convert domain entities to FetcherSchema for caching.
	tables := make([]sharedPorts.FetcherTableSchema, 0, len(schemas))

	for _, schema := range schemas {
		cols := make([]sharedPorts.FetcherColumnInfo, 0, len(schema.Columns))
		for _, col := range schema.Columns {
			cols = append(cols, sharedPorts.FetcherColumnInfo{
				Name:     col.Name,
				Type:     col.Type,
				Nullable: col.Nullable,
			})
		}

		tables = append(tables, sharedPorts.FetcherTableSchema{
			TableName: schema.TableName,
			Columns:   cols,
		})
	}

	fetcherSchema := &sharedPorts.FetcherSchema{Tables: tables}
	if err := uc.schemaCache.SetSchema(ctx, connectionID.String(), fetcherSchema, uc.cacheTTL); err != nil {
		uc.logger.Log(ctx, libLog.LevelWarn, "failed to cache schemas in Redis",
			libLog.String("connectionID", connectionID.String()),
			libLog.Any("error", err.Error()))
	}
}
