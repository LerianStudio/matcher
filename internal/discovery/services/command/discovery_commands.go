package command

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

const discoveryRefreshLockKey = "matcher:discovery:sync"

const defaultDiscoveryRefreshLockTTL = 2 * time.Minute

// RefreshDiscovery forces an immediate discovery sync with Fetcher.
// It fetches all connections and their schemas, upserting into the database.
// Returns the number of successfully synced connections.
func (uc *UseCase) RefreshDiscovery(ctx context.Context) (int, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.discovery.refresh_discovery")
	defer span.End()

	lockToken, err := uc.acquireDiscoveryRefreshLock(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "acquire refresh lock", err)
		return 0, err
	}
	defer uc.releaseDiscoveryRefreshLock(ctx, lockToken)

	if !uc.fetcherClient.IsHealthy(ctx) {
		libOpentelemetry.HandleSpanError(span, "fetcher unavailable", ErrFetcherUnavailable)

		return 0, ErrFetcherUnavailable
	}

	// List all connections from Fetcher.
	orgID, tenantPresent := ctx.Value(auth.TenantIDKey).(string)

	orgID = strings.TrimSpace(orgID)
	if orgID == "" && tenantPresent {
		orgID = strings.TrimSpace(auth.GetTenantID(ctx))
	}

	if uc.requireTenantContext && orgID == "" {
		libOpentelemetry.HandleSpanError(span, "missing tenant context", ErrTenantContextRequired)

		return 0, ErrTenantContextRequired
	}

	if orgID == "" {
		orgID = auth.GetTenantID(ctx)
	}

	fetcherConns, err := uc.fetcherClient.ListConnections(ctx, orgID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "list connections from fetcher", err)

		return 0, fmt.Errorf("list fetcher connections: %w", err)
	}

	synced := 0
	seenFetcherIDs := make(map[string]bool, len(fetcherConns))

	for _, fc := range fetcherConns {
		if fc == nil {
			continue
		}

		seenFetcherIDs[fc.ID] = true

		if err := uc.syncConnection(ctx, logger, fc); err != nil {
			logger.With(
				libLog.Any("fetcherConnID", fc.ID),
				libLog.Any("error", err.Error()),
			).Log(ctx, libLog.LevelWarn, "failed to sync connection")

			continue
		}

		synced++
	}

	if err := uc.reconcileStaleConnections(ctx, logger, seenFetcherIDs); err != nil {
		return synced, err
	}

	return synced, nil
}

func (uc *UseCase) acquireDiscoveryRefreshLock(ctx context.Context) (string, error) {
	if uc == nil || uc.refreshLockProvider == nil {
		return "", nil
	}

	redisLease, err := uc.refreshLockProvider.GetRedisConnection(ctx)
	if err != nil {
		return "", fmt.Errorf("get redis connection for discovery refresh lock: %w", err)
	}

	defer redisLease.Release()

	redisConn := redisLease.Connection()
	if redisConn == nil {
		return "", nil
	}

	rdb, err := redisConn.GetClient(ctx)
	if err != nil {
		return "", fmt.Errorf("get redis client for discovery refresh lock: %w", err)
	}

	token := uuid.NewString()

	ttl := uc.refreshLockTTL
	if uc.refreshLockTTLGetter != nil {
		if runtimeTTL := uc.refreshLockTTLGetter(); runtimeTTL > 0 {
			ttl = runtimeTTL
		}
	}

	if ttl <= 0 {
		ttl = defaultDiscoveryRefreshLockTTL
	}

	ok, err := rdb.SetNX(ctx, discoveryRefreshLockKey, token, ttl).Result()
	if err != nil {
		return "", fmt.Errorf("acquire discovery refresh lock: %w", err)
	}

	if !ok {
		return "", ErrDiscoveryRefreshInProgress
	}

	return token, nil
}

func (uc *UseCase) releaseDiscoveryRefreshLock(ctx context.Context, token string) {
	if uc == nil || uc.refreshLockProvider == nil || token == "" {
		return
	}

	redisLease, err := uc.refreshLockProvider.GetRedisConnection(ctx)
	if err != nil || redisLease == nil {
		return
	}
	defer redisLease.Release()

	redisConn := redisLease.Connection()
	if redisConn == nil {
		return
	}

	rdb, err := redisConn.GetClient(ctx)
	if err != nil {
		return
	}

	script := `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
else
  return 0
end
`
	_, _ = rdb.Eval(ctx, script, []string{discoveryRefreshLockKey}, token).Result()
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

func (uc *UseCase) reconcileStaleConnections(ctx context.Context, logger libLog.Logger, seenFetcherIDs map[string]bool) error {
	allConns, err := uc.connRepo.FindAll(ctx)
	if err != nil {
		return fmt.Errorf("find all connections for stale reconciliation: %w", err)
	}

	for _, conn := range allConns {
		if conn == nil || seenFetcherIDs[conn.FetcherConnID] || conn.Status == vo.ConnectionStatusUnreachable {
			continue
		}

		if err := uc.syncer.MarkConnectionUnreachable(ctx, conn); err != nil {
			if logger != nil {
				logger.With(
					libLog.String("connection.id", conn.ID.String()),
					libLog.Any("error", err.Error()),
				).Log(ctx, libLog.LevelWarn, "failed to mark stale connection unreachable during manual refresh")
			}
		}
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

	if conn == nil {
		libOpentelemetry.HandleSpanError(span, "find connection", ErrConnectionNotFound)

		return nil, ErrConnectionNotFound
	}

	result, err := uc.fetcherClient.TestConnection(ctx, conn.FetcherConnID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "test connection", err)

		return nil, fmt.Errorf("test connection: %w", err)
	}

	if result == nil {
		libOpentelemetry.HandleSpanError(span, "test connection", ErrNilTestConnectionResult)

		return nil, ErrNilTestConnectionResult
	}

	return &ConnectionTestResult{
		ConnectionID:  conn.ID,
		FetcherConnID: conn.FetcherConnID,
		Healthy:       result.Healthy,
		LatencyMs:     result.LatencyMs,
		ErrorMessage:  result.ErrorMessage,
	}, nil
}
