// Package worker provides background workers for the discovery context.
package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v4/commons/runtime"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/discovery/services/syncer"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

const (
	// discoveryLockKey is the Redis distributed lock key for discovery sync.
	discoveryLockKey = "matcher:discovery:sync"

	// lockTTLMultiplier is applied to the worker interval to compute the lock TTL.
	lockTTLMultiplier = 2
)

// Sentinel errors for the discovery worker.
var (
	ErrNilFetcherClient        = errors.New("fetcher client is required")
	ErrNilConnectionRepository = errors.New("connection repository is required")
	ErrNilSchemaRepository     = errors.New("schema repository is required")
	ErrNilTenantLister         = errors.New("tenant lister is required")
	ErrNilInfraProvider        = errors.New("infrastructure provider is required")
	ErrWorkerAlreadyRunning    = errors.New("discovery worker already running")
	ErrWorkerNotRunning        = errors.New("discovery worker not running")
	ErrRedisClientNil          = errors.New("redis client is nil")
)

// DiscoveryWorkerConfig holds configuration for the discovery worker.
type DiscoveryWorkerConfig struct {
	// Interval between poll cycles.
	Interval time.Duration
}

// DiscoveryWorker polls Fetcher for connection discovery and schema sync.
type DiscoveryWorker struct {
	fetcherClient sharedPorts.FetcherClient
	connRepo      repositories.ConnectionRepository
	schemaRepo    repositories.SchemaRepository
	tenantLister  sharedPorts.TenantLister
	infraProvider sharedPorts.InfrastructureProvider
	syncer        *syncer.ConnectionSyncer
	cfg           DiscoveryWorkerConfig
	logger        libLog.Logger
	tracer        trace.Tracer

	running  atomic.Bool
	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// NewDiscoveryWorker creates a new discovery worker.
func NewDiscoveryWorker(
	fetcherClient sharedPorts.FetcherClient,
	connRepo repositories.ConnectionRepository,
	schemaRepo repositories.SchemaRepository,
	tenantLister sharedPorts.TenantLister,
	infraProvider sharedPorts.InfrastructureProvider,
	cfg DiscoveryWorkerConfig,
	logger libLog.Logger,
) (*DiscoveryWorker, error) {
	if fetcherClient == nil {
		return nil, ErrNilFetcherClient
	}

	if connRepo == nil {
		return nil, ErrNilConnectionRepository
	}

	if schemaRepo == nil {
		return nil, ErrNilSchemaRepository
	}

	if tenantLister == nil {
		return nil, ErrNilTenantLister
	}

	if infraProvider == nil {
		return nil, ErrNilInfraProvider
	}

	if cfg.Interval <= 0 {
		cfg.Interval = time.Minute
	}

	if logger == nil {
		logger = &libLog.NopLogger{}
	}

	cs, err := syncer.NewConnectionSyncer(connRepo, schemaRepo)
	if err != nil {
		return nil, fmt.Errorf("create connection syncer: %w", err)
	}

	return &DiscoveryWorker{
		fetcherClient: fetcherClient,
		connRepo:      connRepo,
		schemaRepo:    schemaRepo,
		tenantLister:  tenantLister,
		infraProvider: infraProvider,
		syncer:        cs,
		cfg:           cfg,
		logger:        logger,
		tracer:        otel.Tracer("discovery.worker"),
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}, nil
}

// Start begins the discovery worker.
func (dw *DiscoveryWorker) Start(ctx context.Context) error {
	if !dw.running.CompareAndSwap(false, true) {
		return ErrWorkerAlreadyRunning
	}

	runtime.SafeGoWithContextAndComponent(
		ctx,
		dw.logger,
		"discovery",
		"discovery_worker",
		runtime.KeepRunning,
		dw.run,
	)

	return nil
}

// Stop gracefully shuts down the worker.
func (dw *DiscoveryWorker) Stop() error {
	if !dw.running.CompareAndSwap(true, false) {
		return ErrWorkerNotRunning
	}

	dw.stopOnce.Do(func() {
		close(dw.stopCh)
	})
	<-dw.doneCh

	dw.logger.Log(context.Background(), libLog.LevelInfo, "discovery worker stopped")

	return nil
}

// Done returns a channel that is closed when the worker stops.
func (dw *DiscoveryWorker) Done() <-chan struct{} {
	return dw.doneCh
}

func (dw *DiscoveryWorker) run(ctx context.Context) {
	defer runtime.RecoverAndLogWithContext(ctx, dw.logger, "discovery", "discovery_worker.run")
	defer close(dw.doneCh)

	// Run one cycle immediately on start.
	dw.pollCycle(ctx)

	ticker := time.NewTicker(dw.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-dw.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			dw.pollCycle(ctx)
		}
	}
}

// pollCycle acquires a distributed lock, syncs connections and schemas from Fetcher,
// and marks stale connections as UNREACHABLE.
func (dw *DiscoveryWorker) pollCycle(ctx context.Context) {
	logger, tracer := dw.tracking(ctx)

	ctx, span := tracer.Start(ctx, "discovery.worker.poll_cycle")
	defer span.End()

	acquired, token, err := dw.acquireLock(ctx, discoveryLockKey)
	if err != nil {
		logger.With(libLog.Any("error", err.Error())).
			Log(ctx, libLog.LevelWarn, "discovery: lock error")

		return
	}

	if !acquired {
		return
	}

	defer dw.releaseLock(ctx, discoveryLockKey, token)

	// Check Fetcher health before proceeding.
	if !dw.fetcherClient.IsHealthy(ctx) {
		logger.Log(ctx, libLog.LevelWarn, "discovery: fetcher service is unhealthy, skipping cycle")

		return
	}

	dw.syncConnectionsAndSchemas(ctx)
}

// syncConnectionsAndSchemas fetches all connections from Fetcher, upserts them,
// discovers their schemas, and marks stale connections as unreachable.
func (dw *DiscoveryWorker) syncConnectionsAndSchemas(ctx context.Context) {
	logger, tracer := dw.tracking(ctx)

	ctx, span := tracer.Start(ctx, "discovery.worker.sync_connections")
	defer span.End()

	tenantIDs, err := dw.tenantLister.ListTenants(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list tenants for discovery", err)

		logger.With(libLog.Any("error", err.Error())).
			Log(ctx, libLog.LevelError, "discovery: failed to list tenants")

		return
	}

	span.SetAttributes(attribute.Int("discovery.tenants_found", len(tenantIDs)))

	for _, tenantID := range tenantIDs {
		dw.syncTenantConnections(ctx, tenantID)
	}
}

func (dw *DiscoveryWorker) syncTenantConnections(parentCtx context.Context, tenantID string) {
	ctx := context.WithValue(parentCtx, auth.TenantIDKey, tenantID)
	logger, tracer := dw.tracking(ctx)

	ctx, span := tracer.Start(ctx, "discovery.worker.sync_tenant_connections")
	defer span.End()

	span.SetAttributes(attribute.String("tenant.id", tenantID))

	fetcherConns, err := dw.fetcherClient.ListConnections(ctx, tenantID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list connections from fetcher", err)

		logger.With(
			libLog.String("tenant.id", tenantID),
			libLog.Any("error", err.Error()),
		).Log(ctx, libLog.LevelError, "discovery: failed to list tenant connections from fetcher")

		return
	}

	span.SetAttributes(attribute.Int("discovery.connections_found", len(fetcherConns)))

	seenFetcherIDs := make(map[string]bool, len(fetcherConns))
	for _, fc := range fetcherConns {
		if fc == nil {
			logger.Log(ctx, libLog.LevelWarn, "discovery: skipping nil fetcher connection entry")

			continue
		}

		seenFetcherIDs[fc.ID] = true
		dw.syncConnection(ctx, fc)
	}

	dw.markStaleConnections(ctx, seenFetcherIDs)
}

// syncConnection delegates connection/schema synchronization to the shared syncer.
func (dw *DiscoveryWorker) syncConnection(ctx context.Context, fc *sharedPorts.FetcherConnection) {
	logger, tracer := dw.tracking(ctx)

	ctx, span := tracer.Start(ctx, "discovery.worker.sync_connection")
	defer span.End()

	if fc == nil {
		return
	}

	span.SetAttributes(attribute.String("discovery.fetcher_conn_id", fc.ID))

	if err := dw.syncer.SyncConnection(ctx, logger, fc, dw.fetcherClient.GetSchema); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to sync connection", err)

		logger.With(
			libLog.String("fetcher_conn_id", fc.ID),
			libLog.Any("error", err.Error()),
		).Log(ctx, libLog.LevelError, "discovery: failed to sync connection")

		return
	}
}

// markStaleConnections marks connections not seen this cycle as UNREACHABLE.
func (dw *DiscoveryWorker) markStaleConnections(ctx context.Context, seenFetcherIDs map[string]bool) {
	logger, tracer := dw.tracking(ctx)

	ctx, span := tracer.Start(ctx, "discovery.worker.mark_stale")
	defer span.End()

	allConns, err := dw.connRepo.FindAll(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to find all connections", err)

		logger.With(libLog.Any("error", err.Error())).
			Log(ctx, libLog.LevelError, "discovery: failed to find all connections for stale check")

		return
	}

	staleCount := 0

	for _, conn := range allConns {
		if conn == nil {
			logger.Log(ctx, libLog.LevelWarn, "discovery: skipping nil connection entry from repository")

			continue
		}

		if seenFetcherIDs[conn.FetcherConnID] {
			continue
		}

		if conn.Status == vo.ConnectionStatusUnreachable {
			continue
		}

		conn.MarkUnreachable()

		if err := dw.connRepo.Upsert(ctx, conn); err != nil {
			logger.With(
				libLog.String("connection.id", conn.ID.String()),
				libLog.Any("error", err.Error()),
			).Log(ctx, libLog.LevelWarn, "discovery: failed to mark connection unreachable")

			continue
		}

		staleCount++
	}

	span.SetAttributes(attribute.Int("discovery.stale_count", staleCount))
}

// acquireLock attempts to acquire a Redis distributed lock.
func (dw *DiscoveryWorker) acquireLock(ctx context.Context, key string) (bool, string, error) {
	conn, err := dw.infraProvider.GetRedisConnection(ctx)
	if err != nil {
		return false, "", fmt.Errorf("get redis connection: %w", err)
	}

	if conn == nil {
		return false, "", ErrRedisClientNil
	}

	rdb, err := conn.GetClient(ctx)
	if err != nil {
		return false, "", fmt.Errorf("get redis client for lock acquire: %w", err)
	}

	lockTTL := lockTTLMultiplier * dw.cfg.Interval
	token := uuid.New().String()

	ok, err := rdb.SetNX(ctx, key, token, lockTTL).Result()
	if err != nil {
		return false, "", fmt.Errorf("redis setnx: %w", err)
	}

	return ok, token, nil
}

// releaseLock releases a Redis distributed lock using an atomic Lua script.
func (dw *DiscoveryWorker) releaseLock(ctx context.Context, key, token string) {
	conn, err := dw.infraProvider.GetRedisConnection(ctx)
	if err != nil {
		return
	}

	if conn == nil {
		return
	}

	rdb, rdbErr := conn.GetClient(ctx)
	if rdbErr != nil {
		dw.logger.With(
			libLog.String("lock.key", key),
			libLog.Any("error", rdbErr.Error()),
		).Log(ctx, libLog.LevelWarn, "discovery: failed to acquire redis client for lock release")

		return
	}

	script := `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
else
  return 0
end
`
	if _, err := rdb.Eval(ctx, script, []string{key}, token).Result(); err != nil {
		dw.logger.With(
			libLog.String("lock.key", key),
			libLog.Any("error", err.Error()),
		).Log(ctx, libLog.LevelWarn, "discovery: failed to release lock")
	}
}

// tracking extracts observability primitives from context.
func (dw *DiscoveryWorker) tracking(ctx context.Context) (libLog.Logger, trace.Tracer) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	if logger == nil {
		logger = dw.logger
	}

	if tracer == nil {
		tracer = dw.tracer
	}

	return logger, tracer
}
