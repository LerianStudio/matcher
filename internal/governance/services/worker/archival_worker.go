package worker

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"
	libS3 "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/s3"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	"github.com/LerianStudio/matcher/internal/governance/domain/repositories"
	"github.com/LerianStudio/matcher/internal/governance/services/command"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
	"github.com/LerianStudio/matcher/pkg/chanutil"
	"github.com/LerianStudio/matcher/pkg/storageopt"
)

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
	storage       sharedPorts.ObjectStorageClient
	db            *sql.DB
	infraProvider sharedPorts.InfrastructureProvider
	cfg           ArchivalWorkerConfig
	logger        libLog.Logger
	tracer        trace.Tracer

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
func (aw *ArchivalWorker) UpdateRuntimeStorage(storage sharedPorts.ObjectStorageClient) error {
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
	storage sharedPorts.ObjectStorageClient,
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
	defer close(aw.doneCh)
	defer runtime.RecoverAndLogWithContext(ctx, aw.logger, "governance", "archival_worker.run")

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

// archiveCycle performs one complete archival cycle across all tenants.
func (aw *ArchivalWorker) archiveCycle(ctx context.Context) {
	logger, tracer := aw.tracking(ctx)

	ctx, span := tracer.Start(ctx, "governance.archival.cycle")
	defer span.End()

	ctx = libCommons.ContextWithLogger(ctx, logger)
	ctx = libCommons.ContextWithTracer(ctx, tracer)

	// 1. Acquire distributed lock.
	acquired, lockToken, err := aw.acquireLock(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to acquire archival lock", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to acquire archival lock")

		return
	}

	if !acquired {
		logger.Log(ctx, libLog.LevelInfo, "archival lock held by another instance, skipping cycle")

		return
	}

	defer aw.releaseLock(ctx, lockToken)

	// 2. Resume incomplete archives from previous cycles (crash recovery).
	aw.resumeIncomplete(ctx)

	// 3. List all tenants and process each.
	tenants, err := aw.listTenants(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list tenants", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to list tenants for archival")

		return
	}

	span.SetAttributes(attribute.Int("archival.tenant_count", len(tenants)))

	for _, tenantID := range tenants {
		if tenantID == "" {
			continue
		}

		tenantCtx := context.WithValue(ctx, auth.TenantIDKey, tenantID)

		tenantCtx, tenantSpan := tracer.Start(tenantCtx, "governance.archival.tenant")
		tenantSpan.SetAttributes(attribute.String("tenant.id", tenantID))

		aw.processTenant(tenantCtx, tenantID)

		tenantSpan.End()
	}
}

// processTenant handles partition provisioning and archival for a single tenant.
// Errors are logged but do not propagate; one tenant's failure does not block others.
func (aw *ArchivalWorker) processTenant(ctx context.Context, tenantID string) {
	logger, _ := aw.tracking(ctx)

	// Provision future partitions.
	if err := aw.provisionPartitions(ctx); err != nil {
		logger.With(
			libLog.String("tenant_id", tenantID),
			libLog.Any("error", err.Error()),
		).Log(ctx, libLog.LevelWarn, "failed to provision partitions for tenant")
	}

	// Archive eligible partitions.
	tid, parseErr := uuid.Parse(tenantID)
	if parseErr != nil {
		logger.With(
			libLog.String("tenant_id", tenantID),
			libLog.Any("error", parseErr.Error()),
		).Log(ctx, libLog.LevelWarn, "invalid tenant ID, skipping archival")

		return
	}

	if err := aw.archiveTenant(ctx, tid); err != nil {
		logger.With(
			libLog.String("tenant_id", tenantID),
			libLog.Any("error", err.Error()),
		).Log(ctx, libLog.LevelWarn, "archival failed for tenant")
	}
}

// provisionPartitions ensures future partitions exist for the tenant in context.
func (aw *ArchivalWorker) provisionPartitions(ctx context.Context) error {
	if err := aw.partitionMgr.EnsurePartitionsExist(ctx, aw.cfg.PartitionLookahead); err != nil {
		return fmt.Errorf("ensure partitions exist: %w", err)
	}

	return nil
}

// archiveTenant identifies and processes partitions eligible for cold archival.
func (aw *ArchivalWorker) archiveTenant(ctx context.Context, tenantID uuid.UUID) error {
	logger, tracer := aw.tracking(ctx)

	ctx, span := tracer.Start(ctx, "governance.archival.archive_tenant")
	defer span.End()

	partitions, err := aw.partitionMgr.ListPartitions(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list partitions", err)
		return fmt.Errorf("list partitions: %w", err)
	}

	warmCutoff := time.Now().UTC().AddDate(0, -aw.cfg.WarmRetentionMonths, 0)

	for i := range partitions {
		partition := &partitions[i]

		// A partition is eligible for cold archival when its entire date range
		// is older than the warm retention period.
		if !partition.RangeEnd.Before(warmCutoff) {
			continue
		}

		metadata, err := aw.getOrCreateMetadata(ctx, tenantID, partition)
		if err != nil {
			logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("failed to get/create archive metadata for partition %s: %v", partition.Name, err))

			continue
		}

		// Skip already completed partitions.
		if metadata.Status == entities.StatusComplete {
			continue
		}

		if err := aw.archivePartition(ctx, metadata); err != nil {
			logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("failed to archive partition %s: %v", partition.Name, err))
		}
	}

	return nil
}

// getOrCreateMetadata retrieves existing archive metadata or creates a new record.
func (aw *ArchivalWorker) getOrCreateMetadata(
	ctx context.Context,
	tenantID uuid.UUID,
	partition *command.PartitionInfo,
) (*entities.ArchiveMetadata, error) {
	existing, err := aw.archiveRepo.GetByPartition(ctx, tenantID, partition.Name)
	if err == nil && existing != nil {
		return existing, nil
	}

	metadata, err := entities.NewArchiveMetadata(ctx, tenantID, partition.Name, partition.RangeStart, partition.RangeEnd)
	if err != nil {
		return nil, fmt.Errorf("create archive metadata: %w", err)
	}

	if err := aw.archiveRepo.Create(ctx, metadata); err != nil {
		return nil, fmt.Errorf("persist archive metadata: %w", err)
	}

	return metadata, nil
}

// archivePartition processes a partition through the archival state machine,
// advancing from the current state forward. Each state transition is persisted
// before proceeding. On error, the metadata is marked with the error and the
// function returns so the next cycle can retry.
//
// DESIGN CONSTRAINT: handlePartitionError always returns non-nil error.
// All callers MUST return immediately after handlePartitionError to ensure
// local variables (exportBuf, checksum) are not reused with stale data
// after metadata has been reloaded from the database.
//
//nolint:cyclop,gocyclo // state machine requires sequential checks for each state
func (aw *ArchivalWorker) archivePartition(ctx context.Context, metadata *entities.ArchiveMetadata) error {
	_, tracer := aw.tracking(ctx)

	ctx, span := tracer.Start(ctx, "governance.archival.archive_partition")
	defer span.End()

	span.SetAttributes(
		attribute.String("partition.name", metadata.PartitionName),
		attribute.String("partition.status", string(metadata.Status)),
	)

	// PENDING -> EXPORTING
	if metadata.Status == entities.StatusPending {
		if err := aw.transitionTo(ctx, metadata, metadata.MarkExporting); err != nil {
			return aw.handlePartitionError(ctx, metadata, "mark exporting", err)
		}
	}

	// EXPORTING -> EXPORTED: export data from partition
	var exportBuf *bytes.Buffer

	var rowCount int64

	var checksum string

	if metadata.Status == entities.StatusExporting {
		var err error

		exportBuf, rowCount, checksum, err = aw.exportPartition(ctx, metadata)
		if err != nil {
			return aw.handlePartitionError(ctx, metadata, "export partition", err)
		}

		if err := aw.transitionToExported(ctx, metadata, rowCount); err != nil {
			return aw.handlePartitionError(ctx, metadata, "mark exported", err)
		}
	}

	// EXPORTED -> UPLOADING
	if metadata.Status == entities.StatusExported {
		if err := aw.transitionTo(ctx, metadata, metadata.MarkUploading); err != nil {
			return aw.handlePartitionError(ctx, metadata, "mark uploading", err)
		}
	}

	// UPLOADING -> UPLOADED: upload to object storage
	if metadata.Status == entities.StatusUploading {
		if err := aw.handleUploadingState(ctx, metadata, exportBuf, checksum); err != nil {
			return err
		}
	}

	// UPLOADED -> VERIFYING
	if metadata.Status == entities.StatusUploaded {
		if err := aw.transitionTo(ctx, metadata, metadata.MarkVerifying); err != nil {
			return aw.handlePartitionError(ctx, metadata, "mark verifying", err)
		}
	}

	// VERIFYING -> VERIFIED: checksum and row count verification
	if metadata.Status == entities.StatusVerifying {
		if err := aw.verifyArchive(ctx, metadata); err != nil {
			return aw.handlePartitionError(ctx, metadata, "verify archive", err)
		}

		if err := aw.transitionTo(ctx, metadata, metadata.MarkVerified); err != nil {
			return aw.handlePartitionError(ctx, metadata, "mark verified", err)
		}
	}

	// VERIFIED -> DETACHING: detach and drop partition
	if metadata.Status == entities.StatusVerified {
		if err := aw.transitionTo(ctx, metadata, metadata.MarkDetaching); err != nil {
			return aw.handlePartitionError(ctx, metadata, "mark detaching", err)
		}
	}

	// DETACHING -> COMPLETE: detach, drop, and mark complete
	if metadata.Status == entities.StatusDetaching {
		if err := aw.handleDetachingState(ctx, metadata); err != nil {
			return err
		}
	}

	return nil
}

// handleUploadingState handles the UPLOADING -> UPLOADED transition.
// If the export buffer is nil (crash recovery), it re-exports the partition data.
func (aw *ArchivalWorker) handleUploadingState(
	ctx context.Context,
	metadata *entities.ArchiveMetadata,
	exportBuf *bytes.Buffer,
	checksum string,
) error {
	// If we don't have the buffer (resuming from crash), re-export.
	if exportBuf == nil {
		var err error

		exportBuf, _, checksum, err = aw.exportPartition(ctx, metadata)
		if err != nil {
			return aw.handlePartitionError(ctx, metadata, "re-export partition", err)
		}
	}

	archiveKey, err := aw.archiveKey(metadata)
	if err != nil {
		return aw.handlePartitionError(ctx, metadata, "build archive key", err)
	}

	compressedSize := int64(exportBuf.Len())

	_, err = aw.storage.UploadWithOptions(
		ctx,
		archiveKey,
		exportBuf,
		archiveContentType,
		storageopt.WithStorageClass(aw.cfg.StorageClass),
	)
	if err != nil {
		return aw.handlePartitionError(ctx, metadata, "upload archive", err)
	}

	if err := aw.transitionToUploaded(ctx, metadata, archiveKey, checksum, compressedSize); err != nil {
		return aw.handlePartitionError(ctx, metadata, "mark uploaded", err)
	}

	logger, _ := aw.tracking(ctx)

	logger.With(
		libLog.String("partition_name", metadata.PartitionName),
		libLog.String("archive_key", archiveKey),
		libLog.Any("compressed_size", compressedSize),
	).Log(ctx, libLog.LevelInfo, "uploaded archive for partition")

	return nil
}

// handleDetachingState handles the DETACHING -> COMPLETE transition.
// It detaches the partition, drops it, and marks the archive as complete.
func (aw *ArchivalWorker) handleDetachingState(
	ctx context.Context,
	metadata *entities.ArchiveMetadata,
) error {
	if err := aw.detachAndDrop(ctx, metadata); err != nil {
		return aw.handlePartitionError(ctx, metadata, "detach and drop partition", err)
	}

	if err := aw.transitionTo(ctx, metadata, metadata.MarkComplete); err != nil {
		return aw.handlePartitionError(ctx, metadata, "mark complete", err)
	}

	logger, _ := aw.tracking(ctx)

	logger.With(
		libLog.String("partition_name", metadata.PartitionName),
		libLog.Any("row_count", metadata.RowCount),
	).Log(ctx, libLog.LevelInfo, "partition archived")

	return nil
}

// transitionTo applies a state transition function and persists the result.
func (aw *ArchivalWorker) transitionTo(
	ctx context.Context,
	metadata *entities.ArchiveMetadata,
	transition func() error,
) error {
	if err := transition(); err != nil {
		return fmt.Errorf("state transition: %w", err)
	}

	if err := aw.archiveRepo.Update(ctx, metadata); err != nil {
		return fmt.Errorf("persist state transition: %w", err)
	}

	return nil
}

// transitionToExported applies the MarkExported transition with row count.
func (aw *ArchivalWorker) transitionToExported(
	ctx context.Context,
	metadata *entities.ArchiveMetadata,
	rowCount int64,
) error {
	if err := metadata.MarkExported(rowCount); err != nil {
		return fmt.Errorf("mark exported: %w", err)
	}

	if err := aw.archiveRepo.Update(ctx, metadata); err != nil {
		return fmt.Errorf("persist exported state: %w", err)
	}

	return nil
}

// transitionToUploaded applies the MarkUploaded transition with archive details.
func (aw *ArchivalWorker) transitionToUploaded(
	ctx context.Context,
	metadata *entities.ArchiveMetadata,
	archiveKey, checksum string,
	compressedSize int64,
) error {
	if err := metadata.MarkUploaded(archiveKey, checksum, compressedSize, aw.cfg.StorageClass); err != nil {
		return fmt.Errorf("mark uploaded: %w", err)
	}

	if err := aw.archiveRepo.Update(ctx, metadata); err != nil {
		return fmt.Errorf("persist uploaded state: %w", err)
	}

	return nil
}

// exportPartition streams all rows from the partition into a gzip-compressed
// JSON-lines buffer, computing a SHA-256 checksum as it writes.
func (aw *ArchivalWorker) exportPartition(
	ctx context.Context,
	metadata *entities.ArchiveMetadata,
) (*bytes.Buffer, int64, string, error) {
	_, tracer := aw.tracking(ctx)

	ctx, span := tracer.Start(ctx, "governance.archival.export")
	defer span.End()

	span.SetAttributes(attribute.String("partition.name", metadata.PartitionName))

	result, err := withArchivalCurrentDBResult(ctx, aw, func(currentDB *sql.DB) (struct {
		buf      *bytes.Buffer
		rowCount int64
		checksum string
	}, error,
	) {
		tx, err := currentDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			return struct {
				buf      *bytes.Buffer
				rowCount int64
				checksum string
			}{}, fmt.Errorf("begin read transaction: %w", err)
		}

		defer func() { _ = tx.Rollback() }()

		if err := auth.ApplyTenantSchema(ctx, tx); err != nil {
			return struct {
				buf      *bytes.Buffer
				rowCount int64
				checksum string
			}{}, fmt.Errorf("apply tenant schema: %w", err)
		}

		query, err := buildPartitionExportQuery(metadata.PartitionName)
		if err != nil {
			return struct {
				buf      *bytes.Buffer
				rowCount int64
				checksum string
			}{}, err
		}

		rows, err := tx.QueryContext(ctx, query)
		if err != nil {
			return struct {
				buf      *bytes.Buffer
				rowCount int64
				checksum string
			}{}, fmt.Errorf("query partition %s: %w", metadata.PartitionName, err)
		}
		defer rows.Close()

		var buf bytes.Buffer

		hasher := sha256.New()

		gzWriter, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
		if err != nil {
			return struct {
				buf      *bytes.Buffer
				rowCount int64
				checksum string
			}{}, fmt.Errorf("create gzip writer: %w", err)
		}

		rowCount, err := encodePartitionRows(rows, gzWriter, hasher)
		if err != nil {
			return struct {
				buf      *bytes.Buffer
				rowCount int64
				checksum string
			}{}, err
		}

		if err := gzWriter.Close(); err != nil {
			return struct {
				buf      *bytes.Buffer
				rowCount int64
				checksum string
			}{}, fmt.Errorf("close gzip writer: %w", err)
		}

		if err := tx.Commit(); err != nil {
			return struct {
				buf      *bytes.Buffer
				rowCount int64
				checksum string
			}{}, fmt.Errorf("commit read transaction: %w", err)
		}

		return struct {
			buf      *bytes.Buffer
			rowCount int64
			checksum string
		}{buf: &buf, rowCount: rowCount, checksum: hex.EncodeToString(hasher.Sum(nil))}, nil
	})
	if err != nil {
		return nil, 0, "", err
	}

	span.SetAttributes(attribute.Int64("archival.row_count", result.rowCount), attribute.Int("archival.compressed_bytes", result.buf.Len()))

	return result.buf, result.rowCount, result.checksum, nil
}

// encodePartitionRows iterates over all rows from a partition query, encodes each
// as a JSON line into the gzip writer, and computes a running checksum.
func encodePartitionRows(rows *sql.Rows, gzWriter *gzip.Writer, hasher hash.Hash) (int64, error) {
	encoder := json.NewEncoder(gzWriter)

	var rowCount int64

	for rows.Next() {
		row, err := scanAuditLogRow(rows)
		if err != nil {
			return 0, fmt.Errorf("scan audit log row: %w", err)
		}

		// Encode row as JSON and compute checksum on the uncompressed JSON line.
		jsonBytes, err := json.Marshal(row)
		if err != nil {
			return 0, fmt.Errorf("marshal audit log row: %w", err)
		}

		if _, err := hasher.Write(jsonBytes); err != nil {
			return 0, fmt.Errorf("hash audit log row: %w", err)
		}

		if _, err := hasher.Write([]byte("\n")); err != nil {
			return 0, fmt.Errorf("hash newline: %w", err)
		}

		if err := encoder.Encode(row); err != nil {
			return 0, fmt.Errorf("encode audit log row to gzip: %w", err)
		}

		rowCount++
	}

	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate partition rows: %w", err)
	}

	return rowCount, nil
}

// verifyArchive downloads the archive from object storage, decompresses it,
// re-computes the SHA-256 checksum while counting JSONL lines, and compares
// both values against metadata. This ensures corrupted or partial uploads are
// caught before the source partition is detached.
func (aw *ArchivalWorker) verifyArchive(
	ctx context.Context,
	metadata *entities.ArchiveMetadata,
) error {
	if metadata.Checksum == "" {
		return fmt.Errorf("%w: checksum is empty", ErrChecksumMismatch)
	}

	if metadata.RowCount <= 0 {
		return fmt.Errorf("%w: row count is %d", ErrRowCountMismatch, metadata.RowCount)
	}

	// Download the archive from object storage.
	reader, err := aw.storage.Download(ctx, metadata.ArchiveKey)
	if err != nil {
		return fmt.Errorf("download archive for verification: %w", err)
	}

	defer reader.Close()

	// Decompress the gzip stream.
	gzReader, err := gzip.NewReader(reader)
	if err != nil {
		return fmt.Errorf("open gzip reader for verification: %w", err)
	}

	defer gzReader.Close()

	// Stream through SHA-256 hasher while counting JSONL lines.
	hasher := sha256.New()

	var lineCount int64

	scanner := bufio.NewScanner(gzReader)
	// Audit log JSONL lines may exceed the default 64KB token size when changes
	// payloads contain large diffs. Use a 1MB buffer to match practical limits.
	const maxVerificationLineSize = 1 << 20 // 1MB
	scanner.Buffer(make([]byte, 0, maxVerificationLineSize), maxVerificationLineSize)

	for scanner.Scan() {
		line := scanner.Bytes()

		// Write the JSON line + newline to the hasher, matching the
		// encoding pattern used during export (json bytes + "\n").
		if _, err := hasher.Write(line); err != nil {
			return fmt.Errorf("hash verification line: %w", err)
		}

		if _, err := hasher.Write([]byte("\n")); err != nil {
			return fmt.Errorf("hash verification newline: %w", err)
		}

		lineCount++
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan archive for verification: %w", err)
	}

	// Compare computed checksum against stored value.
	computedChecksum := hex.EncodeToString(hasher.Sum(nil))
	if computedChecksum != metadata.Checksum {
		return fmt.Errorf("%w: expected %s, got %s", ErrChecksumMismatch, metadata.Checksum, computedChecksum)
	}

	// Compare line count against stored row count.
	if lineCount != metadata.RowCount {
		return fmt.Errorf("%w: expected %d rows, got %d", ErrRowCountMismatch, metadata.RowCount, lineCount)
	}

	return nil
}

// detachAndDrop detaches the partition from the parent table and drops it.
func (aw *ArchivalWorker) detachAndDrop(ctx context.Context, metadata *entities.ArchiveMetadata) error {
	if err := aw.partitionMgr.DetachPartition(ctx, metadata.PartitionName); err != nil {
		return fmt.Errorf("detach partition: %w", err)
	}

	if err := aw.partitionMgr.DropPartition(ctx, metadata.PartitionName); err != nil {
		return fmt.Errorf("drop partition: %w", err)
	}

	return nil
}

// handlePartitionError marks the metadata with the error, persists the state,
// and reloads the metadata from the database to prevent in-memory state
// corruption (e.g., when a transition mutated in-memory state but the
// subsequent DB persist failed).
func (aw *ArchivalWorker) handlePartitionError(
	ctx context.Context,
	metadata *entities.ArchiveMetadata,
	operation string,
	err error,
) error {
	_, tracer := aw.tracking(ctx)

	_, span := tracer.Start(ctx, "governance.archival.handle_error")
	defer span.End()

	wrappedErr := fmt.Errorf("%s: %w", operation, err)

	libOpentelemetry.HandleSpanError(span, "archival partition error", wrappedErr)

	metadata.MarkError(wrappedErr.Error())

	if updateErr := aw.archiveRepo.Update(ctx, metadata); updateErr != nil {
		aw.logger.With(
			libLog.String("partition_name", metadata.PartitionName),
			libLog.Any("error", updateErr.Error()),
		).Log(ctx, libLog.LevelError, "failed to persist error state for partition")
	}

	// Reload metadata from DB to ensure in-memory state matches persisted state.
	// This guards against scenarios where a state transition mutated the in-memory
	// object but the DB persist failed, leaving the pointer in an inconsistent state.
	if metadata.ID != uuid.Nil {
		reloaded, reloadErr := aw.archiveRepo.GetByID(ctx, metadata.ID)

		switch {
		case reloadErr == nil && reloaded != nil:
			*metadata = *reloaded
		case reloadErr != nil && aw.logger != nil:
			aw.logger.With(
				libLog.String("partition_name", metadata.PartitionName),
				libLog.Any("error", reloadErr.Error()),
			).Log(ctx, libLog.LevelWarn, "failed to reload metadata for partition after error")
		}
	}

	return wrappedErr
}

// archiveKey generates the object storage key for an archive.
// Format: {tenant_id}/{prefix}/{year}/{month}/{archive_id}/audit_logs_{YYYY_MM}.jsonl.gz
//
// The archive_id UUID segment prevents path enumeration attacks: even if a
// presigned URL for one archive leaks, an attacker cannot guess paths for
// other tenants' or months' archives without knowing the per-archive UUID.
func (aw *ArchivalWorker) archiveKey(metadata *entities.ArchiveMetadata) (string, error) {
	year, month, _ := metadata.DateRangeStart.Date()

	originalKey := fmt.Sprintf("%s/%04d/%02d/%s/audit_logs_%04d_%02d.jsonl.gz",
		aw.cfg.StoragePrefix,
		year,
		month,
		metadata.ID.String(),
		year,
		month,
	)

	key, err := libS3.GetObjectStorageKey(metadata.TenantID.String(), originalKey)
	if err != nil {
		return "", fmt.Errorf("build scoped archive storage key: %w", err)
	}

	return key, nil
}

// resumeIncomplete queries for archives not yet COMPLETE and processes each.
func (aw *ArchivalWorker) resumeIncomplete(ctx context.Context) {
	logger, _ := aw.tracking(ctx)

	incomplete, err := aw.archiveRepo.ListIncomplete(ctx)
	if err != nil {
		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to list incomplete archives")

		return
	}

	for _, metadata := range incomplete {
		// Set tenant context for each incomplete archive.
		tenantCtx := context.WithValue(ctx, auth.TenantIDKey, metadata.TenantID.String())

		if err := aw.archivePartition(tenantCtx, metadata); err != nil {
			logger.With(
				libLog.String("partition_name", metadata.PartitionName),
				libLog.Any("error", err.Error()),
			).Log(ctx, libLog.LevelWarn, "failed to resume incomplete archive")
		}
	}
}

// listTenants queries pg_namespace to find all tenant schemas (UUID-named).
func (aw *ArchivalWorker) listTenants(ctx context.Context) ([]string, error) {
	tenants, err := withArchivalCurrentDBResult(ctx, aw, func(currentDB *sql.DB) ([]string, error) {
		rows, err := currentDB.QueryContext(
			ctx,
			"SELECT nspname FROM pg_namespace WHERE nspname ~* $1",
			uuidSchemaRegex,
		)
		if err != nil {
			return nil, fmt.Errorf("query tenant schemas: %w", err)
		}
		defer rows.Close()

		var tenants []string

		for rows.Next() {
			var tenant string
			if err := rows.Scan(&tenant); err != nil {
				return nil, fmt.Errorf("scan tenant schema: %w", err)
			}

			tenants = append(tenants, tenant)
		}

		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterate tenant schemas: %w", err)
		}

		return tenants, nil
	})
	if err != nil {
		return nil, err
	}

	// The default tenant uses the public schema (no UUID-named schema
	// in pg_namespace), so the query above will never discover it.
	// Always ensure it is included so its audit logs are archived.
	defaultTenantID := auth.GetDefaultTenantID()
	if defaultTenantID != "" && !slices.Contains(tenants, defaultTenantID) {
		tenants = append(tenants, defaultTenantID)
	}

	return tenants, nil
}

func withArchivalCurrentDBResult[T any](ctx context.Context, aw *ArchivalWorker, fn func(*sql.DB) (T, error)) (T, error) {
	var zero T
	if aw == nil {
		return zero, command.ErrNilDB
	}

	if _, hasExplicitTenant := auth.LookupTenantID(ctx); !hasExplicitTenant {
		if aw.db == nil {
			return zero, command.ErrNilDB
		}

		return fn(aw.db)
	}

	if aw.infraProvider != nil {
		return withArchivalProviderDBResult(ctx, aw.infraProvider, fn)
	}

	if aw.db == nil {
		return zero, command.ErrNilDB
	}

	return fn(aw.db)
}

func withArchivalProviderDBResult[T any](
	ctx context.Context,
	infraProvider sharedPorts.InfrastructureProvider,
	fn func(*sql.DB) (T, error),
) (T, error) {
	var zero T

	lease, err := infraProvider.GetPrimaryDB(ctx)
	if err != nil {
		return zero, fmt.Errorf("resolve primary postgres db: %w", err)
	}

	if lease == nil {
		return zero, command.ErrNilDB
	}
	defer lease.Release()

	db := lease.DB()
	if db == nil {
		return zero, command.ErrNilDB
	}

	return fn(db)
}

func buildPartitionExportQuery(partitionName string) (string, error) {
	if !archivalPartitionNameRegex.MatchString(partitionName) {
		return "", fmt.Errorf("build partition export query: %w: %s", command.ErrInvalidPartitionName, partitionName)
	}

	var builder strings.Builder
	builder.Grow(len(partitionName) + partitionExportQueryOverhead)
	builder.WriteString("SELECT id, tenant_id, entity_type, entity_id, action, actor_id, changes, created_at, tenant_seq, prev_hash, record_hash, hash_version FROM ")
	builder.WriteString(auth.QuoteIdentifier(partitionName))
	builder.WriteString(" ORDER BY created_at")

	return builder.String(), nil
}

// acquireLock attempts to acquire the distributed archival lock via Redis SET NX EX.
// Returns (acquired, token, error).
func (aw *ArchivalWorker) acquireLock(ctx context.Context) (bool, string, error) {
	connLease, err := aw.infraProvider.GetRedisConnection(ctx)
	if err != nil {
		return false, "", fmt.Errorf("get redis connection: %w", err)
	}
	defer connLease.Release()

	conn := connLease.Connection()
	if conn == nil {
		return false, "", ErrNilRedisClient
	}

	rdb, err := conn.GetClient(ctx)
	if err != nil {
		return false, "", fmt.Errorf("get redis client for archival lock: %w", err)
	}

	lockTTL := lockTTLMultiplier * aw.cfg.Interval

	token := uuid.New().String()

	ok, err := rdb.SetNX(ctx, lockKey, token, lockTTL).Result()
	if err != nil {
		return false, "", fmt.Errorf("redis setnx for archival lock: %w", err)
	}

	return ok, token, nil
}

// releaseLock releases the distributed archival lock using a Lua script
// that only deletes the key if the token matches (safe release).
func (aw *ArchivalWorker) releaseLock(ctx context.Context, token string) {
	connLease, err := aw.infraProvider.GetRedisConnection(ctx)
	if err != nil {
		aw.logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelWarn, "failed to get redis connection for lock release")

		return
	}
	defer connLease.Release()

	conn := connLease.Connection()
	if conn == nil {
		return
	}

	rdb, rdbErr := conn.GetClient(ctx)
	if rdbErr != nil {
		aw.logger.With(libLog.Any("error", rdbErr.Error())).Log(ctx, libLog.LevelWarn, "failed to get redis client for lock release")

		return
	}

	script := `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
else
  return 0
end
`

	if _, err := rdb.Eval(ctx, script, []string{lockKey}, token).Result(); err != nil {
		aw.logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelWarn, "failed to release archival lock")
	}
}

// tracking extracts observability primitives from context, falling back to instance-level values.
func (aw *ArchivalWorker) tracking(ctx context.Context) (libLog.Logger, trace.Tracer) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	if logger == nil {
		logger = aw.logger
	}

	if tracer == nil {
		tracer = aw.tracer
	}

	return logger, tracer
}

// auditLogRow represents a single audit log record for JSON-lines export.
type auditLogRow struct {
	ID          string  `json:"id"`
	TenantID    string  `json:"tenant_id"`
	EntityType  string  `json:"entity_type"`
	EntityID    string  `json:"entity_id"`
	Action      string  `json:"action"`
	ActorID     *string `json:"actor_id,omitempty"`
	Changes     *string `json:"changes,omitempty"`
	CreatedAt   string  `json:"created_at"`
	TenantSeq   *int64  `json:"tenant_seq,omitempty"`
	PrevHash    *string `json:"prev_hash,omitempty"`
	RecordHash  *string `json:"record_hash,omitempty"`
	HashVersion *int    `json:"hash_version,omitempty"`
}

// scanAuditLogRow scans a single row from the audit_logs partition query.
func scanAuditLogRow(rows *sql.Rows) (*auditLogRow, error) {
	var row auditLogRow

	var createdAt time.Time

	if err := rows.Scan(
		&row.ID,
		&row.TenantID,
		&row.EntityType,
		&row.EntityID,
		&row.Action,
		&row.ActorID,
		&row.Changes,
		&createdAt,
		&row.TenantSeq,
		&row.PrevHash,
		&row.RecordHash,
		&row.HashVersion,
	); err != nil {
		return nil, fmt.Errorf("scan row: %w", err)
	}

	row.CreatedAt = createdAt.UTC().Format(time.RFC3339Nano)

	return &row, nil
}
