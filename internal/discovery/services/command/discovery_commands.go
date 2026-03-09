package command

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// RefreshDiscovery forces an immediate discovery sync with Fetcher.
// It fetches all connections and their schemas, upserting into the database.
// Returns the number of successfully synced connections.
func (uc *UseCase) RefreshDiscovery(ctx context.Context) (int, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.discovery.refresh_discovery")
	defer span.End()

	if !uc.fetcherClient.IsHealthy(ctx) {
		libOpentelemetry.HandleSpanError(span, "fetcher unavailable", ErrFetcherUnavailable)

		return 0, ErrFetcherUnavailable
	}

	// List all connections from Fetcher.
	orgID := auth.GetTenantID(ctx)

	fetcherConns, err := uc.fetcherClient.ListConnections(ctx, orgID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "list connections from fetcher", err)

		return 0, fmt.Errorf("list fetcher connections: %w", err)
	}

	synced := 0

	for _, fc := range fetcherConns {
		if fc == nil {
			continue
		}

		if err := uc.syncConnection(ctx, logger, fc); err != nil {
			logger.With(
				libLog.Any("fetcherConnID", fc.ID),
				libLog.Any("error", err.Error()),
			).Log(ctx, libLog.LevelWarn, "failed to sync connection")

			continue
		}

		synced++
	}

	return synced, nil
}

// syncConnection upserts a single Fetcher connection and its schema.
// It looks up existing connections by Fetcher-assigned ID to preserve internal UUIDs,
// preventing FK mismatches in discovered_schemas after upsert.
func (uc *UseCase) syncConnection(ctx context.Context, logger libLog.Logger, fc *sharedPorts.FetcherConnection) error {
	if err := uc.syncer.SyncConnection(ctx, logger, fc, uc.fetcherClient.GetSchema); err != nil {
		return fmt.Errorf("sync connection: %w", err)
	}

	return nil
}

// ConnectionTestResult holds the result of proxying a connection test to Fetcher.
type ConnectionTestResult struct {
	ConnectionID  uuid.UUID
	FetcherConnID string
	Healthy       bool
	LatencyMs     int64
	ErrorMessage  string
}

// TestConnection verifies that the requested tenant-owned connection can still
// be reached through Fetcher.
func (uc *UseCase) TestConnection(ctx context.Context, connectionID uuid.UUID) (*ConnectionTestResult, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "command.discovery.test_connection")
	defer span.End()

	if !uc.fetcherClient.IsHealthy(ctx) {
		libOpentelemetry.HandleSpanError(span, "fetcher unavailable", ErrFetcherUnavailable)

		return nil, ErrFetcherUnavailable
	}

	conn, err := uc.connRepo.FindByID(ctx, connectionID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "find connection", err)

		if errors.Is(err, repositories.ErrConnectionNotFound) {
			return nil, ErrConnectionNotFound
		}

		return nil, fmt.Errorf("find connection: %w", err)
	}

	result, err := uc.fetcherClient.TestConnection(ctx, conn.FetcherConnID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "test connection", err)

		return nil, fmt.Errorf("test connection: %w", err)
	}

	return &ConnectionTestResult{
		ConnectionID:  conn.ID,
		FetcherConnID: conn.FetcherConnID,
		Healthy:       result.Healthy,
		LatencyMs:     result.LatencyMs,
		ErrorMessage:  result.ErrorMessage,
	}, nil
}
