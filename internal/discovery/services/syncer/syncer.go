// Package syncer provides shared connection synchronization logic for the discovery context.
package syncer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	discoveryPorts "github.com/LerianStudio/matcher/internal/discovery/ports"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// Sentinel errors for the connection syncer.
var (
	ErrNilSyncer               = errors.New("connection syncer is nil")
	ErrNilConnection           = errors.New("fetcher connection is nil")
	ErrNilFetcher              = errors.New("schema fetcher is nil")
	ErrNilConnectionRepository = errors.New("connection repository is required")
	ErrNilSchemaRepository     = errors.New("schema repository is required")
	ErrAllTablesFailed         = errors.New("all tables failed schema entity creation")
)

// SchemaFetcher resolves the latest schema for a Fetcher connection.
type SchemaFetcher func(ctx context.Context, connectionID string) (*sharedPorts.FetcherSchema, error)

// ConnectionSyncer centralizes connection/schema synchronization logic shared by
// manual refreshes and the background discovery worker.
type ConnectionSyncer struct {
	connRepo    repositories.ConnectionRepository
	schemaRepo  repositories.SchemaRepository
	schemaCache discoveryPorts.SchemaCache
	cacheTTL    time.Duration
}

// NewConnectionSyncer creates a reusable synchronizer for discovery flows.
// Returns an error if either repository is nil.
func NewConnectionSyncer(
	connRepo repositories.ConnectionRepository,
	schemaRepo repositories.SchemaRepository,
) (*ConnectionSyncer, error) {
	if connRepo == nil {
		return nil, ErrNilConnectionRepository
	}

	if schemaRepo == nil {
		return nil, ErrNilSchemaRepository
	}

	return &ConnectionSyncer{connRepo: connRepo, schemaRepo: schemaRepo}, nil
}

// WithSchemaCache wires an optional schema cache into the syncer so successful
// refreshes immediately replace stale cached schemas.
func (cs *ConnectionSyncer) WithSchemaCache(cache discoveryPorts.SchemaCache, ttl time.Duration) {
	if cs == nil {
		return
	}

	cs.schemaCache = cache
	cs.cacheTTL = ttl
}

// SyncConnection upserts a Fetcher connection and best-effort synchronizes its schema.
func (cs *ConnectionSyncer) SyncConnection(
	ctx context.Context,
	logger libLog.Logger,
	fc *sharedPorts.FetcherConnection,
	fetchSchema SchemaFetcher,
) error {
	if cs == nil {
		return ErrNilSyncer
	}

	if fc == nil {
		return ErrNilConnection
	}

	if fetchSchema == nil {
		return ErrNilFetcher
	}

	conn, err := cs.upsertConnection(ctx, fc)
	if err != nil {
		return err
	}

	schema, err := fetchSchema(ctx, fc.ID)
	if err != nil {
		return fmt.Errorf("fetch schema for connection %s: %w", fc.ID, err)
	}

	if err := cs.SyncSchema(ctx, conn, schema); err != nil {
		return fmt.Errorf("sync schema for connection %s: %w", fc.ID, err)
	}

	return nil
}

func (cs *ConnectionSyncer) upsertConnection(
	ctx context.Context,
	fc *sharedPorts.FetcherConnection,
) (*entities.FetcherConnection, error) {
	existing, err := cs.connRepo.FindByFetcherID(ctx, fc.ID)
	if err != nil && !errors.Is(err, repositories.ErrConnectionNotFound) {
		return nil, fmt.Errorf("find connection by fetcher id: %w", err)
	}

	if existing != nil {
		existing.ConfigName = fc.ConfigName
		existing.DatabaseType = fc.DatabaseType

		if err := existing.UpdateDetails(fc.Host, fc.Port, fc.DatabaseName, fc.ProductName, fc.Schema, fc.UserName); err != nil {
			return nil, fmt.Errorf("update existing connection details: %w", err)
		}

		existing.MarkAvailable()

		if err := cs.connRepo.Upsert(ctx, existing); err != nil {
			return nil, fmt.Errorf("upsert existing connection: %w", err)
		}

		return existing, nil
	}

	newConn, err := entities.NewFetcherConnection(ctx, fc.ID, fc.ConfigName, fc.DatabaseType)
	if err != nil {
		return nil, fmt.Errorf("create connection entity: %w", err)
	}

	if err := newConn.UpdateDetails(fc.Host, fc.Port, fc.DatabaseName, fc.ProductName, fc.Schema, fc.UserName); err != nil {
		return nil, fmt.Errorf("update new connection details: %w", err)
	}

	newConn.MarkAvailable()

	if err := cs.connRepo.Upsert(ctx, newConn); err != nil {
		return nil, fmt.Errorf("upsert new connection: %w", err)
	}

	return newConn, nil
}

// SyncSchema persists discovered table schemas and marks the connection as schema-discovered.
// It processes all tables best-effort: invalid individual tables are logged and skipped
// rather than aborting the entire batch.
func (cs *ConnectionSyncer) SyncSchema(ctx context.Context, conn *entities.FetcherConnection, schema *sharedPorts.FetcherSchema) error {
	if conn == nil {
		return ErrNilConnection
	}

	if schema == nil {
		return nil
	}

	logger, _, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // only logger needed from tracking context

	schemas := make([]*entities.DiscoveredSchema, 0, len(schema.Tables))
	validTables := make([]sharedPorts.FetcherTableSchema, 0, len(schema.Tables))
	skipped := 0

	for _, tbl := range schema.Tables {
		cols := make([]entities.ColumnInfo, 0, len(tbl.Fields))
		for _, fieldName := range tbl.Fields {
			cols = append(cols, entities.ColumnInfo{Name: fieldName})
		}

		discovered, err := entities.NewDiscoveredSchema(ctx, conn.ID, tbl.Name, cols)
		if err != nil {
			skipped++

			if logger != nil {
				logger.With(
					libLog.Any("table", tbl.Name),
					libLog.Any("connectionID", conn.ID.String()),
					libLog.Any("error", err.Error()),
				).Log(ctx, libLog.LevelWarn, "skipping invalid table during schema sync")
			}

			continue
		}

		schemas = append(schemas, discovered)
		validTables = append(validTables, sharedPorts.FetcherTableSchema{
			Name:   tbl.Name,
			Fields: tbl.Fields,
		})
	}

	if len(schema.Tables) > 0 && len(schemas) == 0 {
		return fmt.Errorf("schema sync (%d tables): %w", skipped, ErrAllTablesFailed)
	}

	if len(schemas) == 0 {
		if err := cs.schemaRepo.DeleteByConnectionID(ctx, conn.ID); err != nil {
			return fmt.Errorf("delete schemas: %w", err)
		}
	} else {
		if err := cs.schemaRepo.UpsertBatch(ctx, schemas); err != nil {
			return fmt.Errorf("upsert schemas: %w", err)
		}
	}

	conn.MarkSchemaDiscovered()

	if err := cs.connRepo.Upsert(ctx, conn); err != nil {
		return fmt.Errorf("update connection schema flag: %w", err)
	}

	cs.refreshSchemaCache(ctx, logger, conn.ID, &sharedPorts.FetcherSchema{
		ID:           schema.ID,
		Tables:       validTables,
		DiscoveredAt: schema.DiscoveredAt,
	})

	return nil
}

func (cs *ConnectionSyncer) refreshSchemaCache(
	ctx context.Context,
	logger libLog.Logger,
	connectionID uuid.UUID,
	schema *sharedPorts.FetcherSchema,
) {
	if cs == nil || cs.schemaCache == nil || schema == nil {
		return
	}

	if err := cs.schemaCache.SetSchema(ctx, connectionID.String(), schema, cs.cacheTTL); err != nil {
		if logger != nil {
			logger.With(
				libLog.Any("connectionID", connectionID.String()),
				libLog.Any("error", err.Error()),
			).Log(ctx, libLog.LevelWarn, "failed to refresh schema cache after sync")
		}
	}
}

// MarkConnectionUnreachable marks a connection unreachable and clears stale schema state.
func (cs *ConnectionSyncer) MarkConnectionUnreachable(ctx context.Context, conn *entities.FetcherConnection) error {
	if cs == nil {
		return ErrNilSyncer
	}

	if conn == nil {
		return ErrNilConnection
	}

	conn.MarkUnreachable()

	if err := cs.connRepo.Upsert(ctx, conn); err != nil {
		return fmt.Errorf("mark connection unreachable: %w", err)
	}

	if err := cs.schemaRepo.DeleteByConnectionID(ctx, conn.ID); err != nil {
		return fmt.Errorf("delete stale schemas: %w", err)
	}

	if err := cs.invalidateSchemaCache(ctx, conn.ID); err != nil {
		return err
	}

	return nil
}

func (cs *ConnectionSyncer) invalidateSchemaCache(ctx context.Context, connectionID uuid.UUID) error {
	if cs == nil || cs.schemaCache == nil {
		return nil
	}

	if err := cs.schemaCache.InvalidateSchema(ctx, connectionID.String()); err != nil {
		return fmt.Errorf("invalidate schema cache: %w", err)
	}

	return nil
}
