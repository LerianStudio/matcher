// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package worker

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/auth"
	workermetrics "github.com/LerianStudio/matcher/internal/shared/observability/workermetrics"
)

// archiveCycle performs one complete archival cycle across all tenants.
func (aw *ArchivalWorker) archiveCycle(ctx context.Context) {
	logger, tracer := aw.tracking(ctx)

	ctx, span := tracer.Start(ctx, "governance.archival.cycle")
	defer span.End()

	startedAt := time.Now()
	outcome := workermetrics.OutcomeSuccess

	var processed, failed int

	defer func() {
		aw.metrics.RecordCycle(ctx, startedAt, outcome)
		aw.metrics.RecordItems(ctx, processed, failed)
	}()

	ctx = libCommons.ContextWithLogger(ctx, logger)
	ctx = libCommons.ContextWithTracer(ctx, tracer)

	// 1. Acquire distributed lock.
	acquired, lockToken, err := aw.acquireLock(ctx)
	if err != nil {
		outcome = workermetrics.OutcomeFailure

		libOpentelemetry.HandleSpanError(span, "failed to acquire archival lock", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to acquire archival lock")

		return
	}

	if !acquired {
		outcome = workermetrics.OutcomeSkipped

		logger.Log(ctx, libLog.LevelInfo, "archival lock held by another instance, skipping cycle")

		return
	}

	defer aw.releaseLock(ctx, lockToken)

	// 2. Resume incomplete archives from previous cycles (crash recovery).
	aw.resumeIncomplete(ctx)

	// 3. List all tenants and process each.
	tenants, err := aw.listTenants(ctx)
	if err != nil {
		outcome = workermetrics.OutcomeFailure

		libOpentelemetry.HandleSpanError(span, "failed to list tenants", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to list tenants for archival")

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

		tenantProcessed, tenantFailed := aw.processTenant(tenantCtx, tenantID)
		processed += tenantProcessed
		failed += tenantFailed

		tenantSpan.End()
	}
}

// processTenant handles partition provisioning and archival for a single tenant.
// Errors are logged but do not propagate; one tenant's failure does not block others.
// Returns (processed, failed) partition counts so the cycle can aggregate into
// matcher.worker.items_processed_total / items_failed_total.
func (aw *ArchivalWorker) processTenant(ctx context.Context, tenantID string) (int, int) {
	logger, _ := aw.tracking(ctx)

	// Provision future partitions.
	if err := aw.provisionPartitions(ctx); err != nil {
		logger.With(
			libLog.String("tenant_id", tenantID),
			libLog.Err(err),
		).Log(ctx, libLog.LevelWarn, "failed to provision partitions for tenant")
	}

	// Archive eligible partitions.
	tid, parseErr := uuid.Parse(tenantID)
	if parseErr != nil {
		logger.With(
			libLog.String("tenant_id", tenantID),
			libLog.Err(parseErr),
		).Log(ctx, libLog.LevelWarn, "invalid tenant ID, skipping archival")

		return 0, 0
	}

	processed, failed, err := aw.archiveTenant(ctx, tid)
	if err != nil {
		logger.With(
			libLog.String("tenant_id", tenantID),
			libLog.Err(err),
		).Log(ctx, libLog.LevelWarn, "archival failed for tenant")
	}

	return processed, failed
}

// provisionPartitions ensures future partitions exist for the tenant in context.
func (aw *ArchivalWorker) provisionPartitions(ctx context.Context) error {
	if err := aw.partitionMgr.EnsurePartitionsExist(ctx, aw.cfg.PartitionLookahead); err != nil {
		return fmt.Errorf("ensure partitions exist: %w", err)
	}

	return nil
}

// resumeIncomplete queries for archives not yet COMPLETE and processes each.
func (aw *ArchivalWorker) resumeIncomplete(ctx context.Context) {
	logger, _ := aw.tracking(ctx)

	incomplete, err := aw.archiveRepo.ListIncomplete(ctx)
	if err != nil {
		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to list incomplete archives")

		return
	}

	for _, metadata := range incomplete {
		if metadata == nil {
			continue
		}

		// Set tenant context for each incomplete archive.
		tenantCtx := context.WithValue(ctx, auth.TenantIDKey, metadata.TenantID.String())

		if err := aw.archivePartition(tenantCtx, metadata); err != nil {
			logger.With(
				libLog.String("partition_name", metadata.PartitionName),
				libLog.Err(err),
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
		aw.logger.With(libLog.Err(err)).Log(ctx, libLog.LevelWarn, "failed to get redis connection for lock release")

		return
	}
	defer connLease.Release()

	conn := connLease.Connection()
	if conn == nil {
		return
	}

	rdb, rdbErr := conn.GetClient(ctx)
	if rdbErr != nil {
		aw.logger.With(libLog.Err(rdbErr)).Log(ctx, libLog.LevelWarn, "failed to get redis client for lock release")

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
		aw.logger.With(libLog.Err(err)).Log(ctx, libLog.LevelWarn, "failed to release archival lock")
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
