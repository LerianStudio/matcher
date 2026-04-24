// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package worker

import (
	"context"
	"database/sql"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/governance/domain/repositories"
	"github.com/LerianStudio/matcher/internal/governance/services/command"
	"github.com/LerianStudio/matcher/internal/shared/objectstorage"
	workermetrics "github.com/LerianStudio/matcher/internal/shared/observability/workermetrics"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
	"github.com/LerianStudio/matcher/pkg/chanutil"
)

// archivalWorkerName is the stable label value emitted on matcher.worker.*
// metrics from this worker. Kept as a package constant so dashboards and
// alert rules can reference it textually.
const archivalWorkerName = "archival_worker"

const (
	// lockKey is the global distributed lock for archival.
	lockKey = "matcher:archival:lock"

	// uuidSchemaRegex matches tenant schema names (UUID format).
	uuidSchemaRegex = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"

	// archiveContentType is the content type for gzip-compressed JSON-lines.
	archiveContentType = "application/gzip"

	// lockTTLMultiplier is the multiplier applied to the archival interval to compute the lock TTL.
	lockTTLMultiplier = 2

	// defaultArchivalInterval is the default archival cycle interval (1 hour).
	defaultArchivalInterval = 1 * time.Hour

	// defaultArchivalBatchSize is the default number of rows per SELECT batch during export.
	defaultArchivalBatchSize = 1000

	// defaultPartitionLookahead is the default number of future monthly partitions to create.
	defaultPartitionLookahead = 3

	// partitionExportQueryOverhead is the static portion size of the export SELECT statement.
	partitionExportQueryOverhead = 160
)

var archivalPartitionNameRegex = regexp.MustCompile(`^audit_logs_\d{4}_\d{2}$`)

// PartitionManager manages audit log partition lifecycle operations for archival.
type PartitionManager interface {
	EnsurePartitionsExist(ctx context.Context, lookaheadMonths int) error
	ListPartitions(ctx context.Context) ([]command.PartitionInfo, error)
	DetachPartition(ctx context.Context, name string) error
	DropPartition(ctx context.Context, name string) error
}

// ArchivalWorker orchestrates the full partition lifecycle for audit log archival.
// It provisions future partitions, identifies eligible partitions, exports data,
// uploads to object storage, verifies integrity, and detaches/drops source partitions.
type ArchivalWorker struct {
	mu            sync.Mutex
	archiveRepo   repositories.ArchiveMetadataRepository
	partitionMgr  PartitionManager
	storage       objectstorage.Backend
	db            *sql.DB
	infraProvider sharedPorts.InfrastructureProvider
	cfg           ArchivalWorkerConfig
	logger        libLog.Logger
	tracer        trace.Tracer
	metrics       *workermetrics.Recorder

	running  atomic.Bool
	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// UpdateRuntimeConfig updates the worker runtime configuration used on the next start/restart.
// NOTE: This does NOT affect a currently running worker's ticker. The WorkerManager
// always performs a full stop→start cycle when config changes, ensuring the new
// config is picked up when the worker's run() loop creates a fresh ticker.
func (aw *ArchivalWorker) UpdateRuntimeConfig(cfg ArchivalWorkerConfig) error {
	aw.mu.Lock()
	defer aw.mu.Unlock()

	if aw.running.Load() {
		return ErrRuntimeConfigUpdateWhileRunning
	}

	aw.cfg = normalizeArchivalWorkerConfig(cfg)

	return nil
}

// UpdateRuntimeStorage swaps the storage client used on the next start/restart.
// It must only be called while the worker is stopped so the next archive cycle
// uses dependencies that match the pending runtime configuration.
func (aw *ArchivalWorker) UpdateRuntimeStorage(storage objectstorage.Backend) error {
	aw.mu.Lock()
	defer aw.mu.Unlock()

	if aw.running.Load() {
		return ErrRuntimeConfigUpdateWhileRunning
	}

	if sharedPorts.IsNilValue(storage) {
		return ErrNilStorageClient
	}

	aw.storage = storage

	return nil
}

// prepareRunState reinitialises the worker's stop/done channels and sync.Once for
// re-entrant Start→Stop→Start cycles. SAFETY: The caller (WorkerManager) MUST ensure
// Stop() has fully completed before calling Start(), which calls prepareRunState().
// The WorkerManager serialises all lifecycle transitions via its mutex.
func (aw *ArchivalWorker) prepareRunState() {
	aw.mu.Lock()
	defer aw.mu.Unlock()

	aw.stopOnce = sync.Once{}

	if chanutil.ClosedSignalChannel(aw.stopCh) {
		aw.stopCh = make(chan struct{})
	}

	if chanutil.ClosedSignalChannel(aw.doneCh) {
		aw.doneCh = make(chan struct{})
	}
}

func normalizeArchivalWorkerConfig(cfg ArchivalWorkerConfig) ArchivalWorkerConfig {
	if cfg.Interval <= 0 {
		cfg.Interval = defaultArchivalInterval
	}

	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultArchivalBatchSize
	}

	if cfg.PartitionLookahead <= 0 {
		cfg.PartitionLookahead = defaultPartitionLookahead
	}

	return cfg
}

// NewArchivalWorker creates a new ArchivalWorker with the given dependencies.
// All required dependencies must be non-nil.
func NewArchivalWorker(
	archiveRepo repositories.ArchiveMetadataRepository,
	partitionMgr PartitionManager,
	storage objectstorage.Backend,
	db *sql.DB,
	infraProvider sharedPorts.InfrastructureProvider,
	cfg ArchivalWorkerConfig,
	logger libLog.Logger,
) (*ArchivalWorker, error) {
	if archiveRepo == nil {
		return nil, ErrNilArchiveRepo
	}

	if partitionMgr == nil {
		return nil, ErrNilPartitionManager
	}

	if sharedPorts.IsNilValue(storage) {
		return nil, ErrNilStorageClient
	}

	if db == nil {
		return nil, command.ErrNilDB
	}

	if infraProvider == nil {
		return nil, ErrNilRedisClient
	}

	if logger == nil {
		logger = &libLog.NopLogger{}
	}

	cfg = normalizeArchivalWorkerConfig(cfg)

	return &ArchivalWorker{
		archiveRepo:   archiveRepo,
		partitionMgr:  partitionMgr,
		storage:       storage,
		db:            db,
		infraProvider: infraProvider,
		cfg:           cfg,
		logger:        logger,
		tracer:        otel.Tracer("governance.archival_worker"),
		metrics:       workermetrics.NewRecorder(archivalWorkerName),
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}, nil
}

// Start begins the archival worker.
func (aw *ArchivalWorker) Start(ctx context.Context) error {
	if !aw.running.CompareAndSwap(false, true) {
		return ErrWorkerAlreadyRunning
	}

	aw.prepareRunState()

	runtime.SafeGoWithContextAndComponent(
		ctx,
		aw.logger,
		"governance",
		"archival_worker",
		runtime.KeepRunning,
		aw.run,
	)

	return nil
}

// Stop gracefully shuts down the worker.
func (aw *ArchivalWorker) Stop() error {
	if !aw.running.Load() {
		return ErrWorkerNotRunning
	}

	aw.stopOnce.Do(func() {
		close(aw.stopCh)
	})
	<-aw.doneCh

	aw.running.Store(false)

	aw.logger.Log(context.Background(), libLog.LevelInfo, "archival worker stopped")

	return nil
}

// Done returns a channel that is closed when the worker stops.
func (aw *ArchivalWorker) Done() <-chan struct{} {
	return aw.doneCh
}

func (aw *ArchivalWorker) run(ctx context.Context) {
	defer runtime.RecoverAndLogWithContext(ctx, aw.logger, "governance", "archival_worker.run")
	defer close(aw.doneCh)

	// Run one cycle immediately on start.
	aw.archiveCycle(ctx)

	ticker := time.NewTicker(aw.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-aw.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			aw.archiveCycle(ctx)
		}
	}
}
