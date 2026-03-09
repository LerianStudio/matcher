// Package syncer provides shared connection synchronization logic for the discovery context.
package syncer

import (
	"context"
	"errors"
	"fmt"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
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
	connRepo   repositories.ConnectionRepository
	schemaRepo repositories.SchemaRepository
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
		if logger != nil {
			logger.With(
				libLog.Any("fetcherConnID", fc.ID),
				libLog.Any("error", err.Error()),
			).Log(ctx, libLog.LevelWarn, "schema discovery failed for connection")
		}

		return nil
	}

	if err := cs.SyncSchema(ctx, conn, schema); err != nil && logger != nil {
		logger.With(
			libLog.Any("fetcherConnID", fc.ID),
			libLog.Any("error", err.Error()),
		).Log(ctx, libLog.LevelWarn, "schema sync failed for connection")
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
		existing.UpdateDetails(fc.Host, fc.Port, fc.DatabaseName, fc.ProductName)
		existing.ApplyFetcherStatus(fc.Status)

		if err := cs.connRepo.Upsert(ctx, existing); err != nil {
			return nil, fmt.Errorf("upsert existing connection: %w", err)
		}

		return existing, nil
	}

	newConn, err := entities.NewFetcherConnection(ctx, fc.ID, fc.ConfigName, fc.DatabaseType)
	if err != nil {
		return nil, fmt.Errorf("create connection entity: %w", err)
	}

	newConn.UpdateDetails(fc.Host, fc.Port, fc.DatabaseName, fc.ProductName)
	newConn.ApplyFetcherStatus(fc.Status)

	if err := cs.connRepo.Upsert(ctx, newConn); err != nil {
		return nil, fmt.Errorf("upsert new connection: %w", err)
	}

	return newConn, nil
}

// SyncSchema persists discovered table schemas and marks the connection as schema-discovered.
// It processes all tables best-effort: invalid individual tables are logged and skipped
// rather than aborting the entire batch.
func (cs *ConnectionSyncer) SyncSchema(ctx context.Context, conn *entities.FetcherConnection, schema *sharedPorts.FetcherSchema) error {
	if schema == nil || len(schema.Tables) == 0 {
		return nil
	}

	logger, _, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // only logger needed from tracking context

	schemas := make([]*entities.DiscoveredSchema, 0, len(schema.Tables))
	skipped := 0

	for _, tbl := range schema.Tables {
		cols := make([]entities.ColumnInfo, 0, len(tbl.Columns))

		for _, c := range tbl.Columns {
			cols = append(cols, entities.ColumnInfo{
				Name:     c.Name,
				Type:     c.Type,
				Nullable: c.Nullable,
			})
		}

		discovered, err := entities.NewDiscoveredSchema(ctx, conn.ID, tbl.TableName, cols)
		if err != nil {
			skipped++

			if logger != nil {
				logger.With(
					libLog.Any("table", tbl.TableName),
					libLog.Any("connectionID", conn.ID.String()),
					libLog.Any("error", err.Error()),
				).Log(ctx, libLog.LevelWarn, "skipping invalid table during schema sync")
			}

			continue
		}

		schemas = append(schemas, discovered)
	}

	if len(schemas) == 0 {
		return fmt.Errorf("schema sync (%d tables): %w", skipped, ErrAllTablesFailed)
	}

	if err := cs.schemaRepo.UpsertBatch(ctx, schemas); err != nil {
		return fmt.Errorf("upsert schemas: %w", err)
	}

	conn.MarkSchemaDiscovered()

	if err := cs.connRepo.Upsert(ctx, conn); err != nil {
		return fmt.Errorf("update connection schema flag: %w", err)
	}

	return nil
}
