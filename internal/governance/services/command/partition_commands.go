// Package command provides governance command use cases.
package command

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/auth"
)

// Sentinel errors for partition manager operations.
var (
	ErrNilDB                 = errors.New("database connection is required")
	ErrNowFuncRequired       = errors.New("clock function is required")
	ErrInvalidLookahead      = errors.New("lookahead months must be positive")
	ErrInvalidPartitionName  = errors.New("invalid partition name format")
	ErrParseDateFromBound    = errors.New("could not parse dates from partition bound expression")
	ErrMissingFromClause     = errors.New("missing FROM clause in partition bound expression")
	ErrMissingToClause       = errors.New("missing TO clause in partition bound expression")
	ErrMissingEndDelimiter   = errors.New("missing end delimiter in partition bound expression")
	ErrRetentionPeriodActive = errors.New("partition within retention period")
)

// partitionNameRegex validates partition names to prevent SQL injection.
// Only allows names like audit_logs_2026_02.
var partitionNameRegex = regexp.MustCompile(`^audit_logs_\d{4}_\d{2}$`)

// PartitionInfo describes a single partition of the audit_logs table.
type PartitionInfo struct {
	Name           string
	RangeStart     time.Time
	RangeEnd       time.Time
	ApproxRowCount int64
}

// PartitionManager manages audit_logs partition lifecycle (create, list, detach, drop).
type PartitionManager struct {
	db     *sql.DB
	logger libLog.Logger
	tracer trace.Tracer
	nowFn  func() time.Time
}

// NewPartitionManager creates a new PartitionManager with the given dependencies.
func NewPartitionManager(db *sql.DB, logger libLog.Logger, tracer trace.Tracer) (*PartitionManager, error) {
	return NewPartitionManagerWithClock(db, logger, tracer, func() time.Time {
		return time.Now().UTC()
	})
}

// NewPartitionManagerWithClock creates a new PartitionManager with a custom clock.
// Use this constructor in tests for deterministic time-dependent behavior.
func NewPartitionManagerWithClock(
	db *sql.DB,
	logger libLog.Logger,
	tracer trace.Tracer,
	nowFn func() time.Time,
) (*PartitionManager, error) {
	if db == nil {
		return nil, ErrNilDB
	}

	if nowFn == nil {
		return nil, ErrNowFuncRequired
	}

	return &PartitionManager{
		db:     db,
		logger: logger,
		tracer: tracer,
		nowFn:  nowFn,
	}, nil
}

// EnsurePartitionsExist creates monthly partitions from now to now + lookaheadMonths
// for the tenant in context. Uses CREATE TABLE IF NOT EXISTS for idempotency.
func (pm *PartitionManager) EnsurePartitionsExist(ctx context.Context, lookaheadMonths int) error {
	logger, tracer := pm.tracking(ctx)

	ctx, span := tracer.Start(ctx, "governance.partition_manager.ensure_partitions")
	defer span.End()

	if lookaheadMonths <= 0 {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "invalid lookahead", ErrInvalidLookahead)
		return ErrInvalidLookahead
	}

	span.SetAttributes(attribute.Int("partition.lookahead_months", lookaheadMonths))

	tx, err := pm.db.BeginTx(ctx, nil)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to begin transaction", err)
		return fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		_ = tx.Rollback()
	}()

	if err := auth.ApplyTenantSchema(ctx, tx); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to apply tenant schema", err)
		return fmt.Errorf("apply tenant schema: %w", err)
	}

	now := pm.nowFn()

	for i := range lookaheadMonths + 1 {
		start := monthStart(now, i)
		end := monthStart(now, i+1)
		name := partitionName(start)

		ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s PARTITION OF audit_logs FOR VALUES FROM ('%s') TO ('%s')", name, start.Format("2006-01-02"), end.Format("2006-01-02")) // #nosec G201 -- partition name is generated from time values via partitionName(), not user input

		if _, err := tx.ExecContext(ctx, ddl); err != nil {
			libOpentelemetry.HandleSpanError(span, "failed to create partition", err)
			return fmt.Errorf("create partition %s: %w", name, err)
		}

		logger.Log(ctx, libLog.LevelInfo, fmt.Sprintf("ensured partition exists: %s [%s, %s)",
			name, start.Format("2006-01-02"), end.Format("2006-01-02")))
	}

	if err := tx.Commit(); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to commit transaction", err)
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

// ListPartitions queries pg_catalog to list all partitions of the audit_logs table
// in the current tenant schema.
func (pm *PartitionManager) ListPartitions(ctx context.Context) ([]PartitionInfo, error) {
	_, tracer := pm.tracking(ctx)

	ctx, span := tracer.Start(ctx, "governance.partition_manager.list_partitions")
	defer span.End()

	tx, err := pm.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to begin transaction", err)
		return nil, fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		_ = tx.Rollback()
	}()

	if err := auth.ApplyTenantSchema(ctx, tx); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to apply tenant schema", err)
		return nil, fmt.Errorf("apply tenant schema: %w", err)
	}

	query := `
		SELECT
			child.relname AS partition_name,
			pg_get_expr(child.relpartbound, child.oid) AS partition_bound,
			child.reltuples::bigint AS approx_row_count
		FROM pg_catalog.pg_inherits inh
		JOIN pg_catalog.pg_class parent ON inh.inhparent = parent.oid
		JOIN pg_catalog.pg_class child ON inh.inhrelid = child.oid
		JOIN pg_catalog.pg_namespace ns ON parent.relnamespace = ns.oid
		WHERE parent.relname = 'audit_logs'
			AND ns.nspname = current_schema()
		ORDER BY child.relname
	`

	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to query partitions", err)
		return nil, fmt.Errorf("query partitions: %w", err)
	}

	defer rows.Close()

	partitions := make([]PartitionInfo, 0)

	for rows.Next() {
		var (
			name, boundExpr string
			approxRows      int64
		)

		if err := rows.Scan(&name, &boundExpr, &approxRows); err != nil {
			libOpentelemetry.HandleSpanError(span, "failed to scan partition row", err)
			return nil, fmt.Errorf("scan partition: %w", err)
		}

		rangeStart, rangeEnd, parseErr := parsePartitionBound(boundExpr)
		if parseErr != nil {
			libOpentelemetry.HandleSpanError(span, "failed to parse partition bound", parseErr)
			return nil, fmt.Errorf("parse partition bound for %s: %w", name, parseErr)
		}

		partitions = append(partitions, PartitionInfo{
			Name:           name,
			RangeStart:     rangeStart,
			RangeEnd:       rangeEnd,
			ApproxRowCount: approxRows,
		})
	}

	if err := rows.Err(); err != nil {
		libOpentelemetry.HandleSpanError(span, "rows iteration error", err)
		return nil, fmt.Errorf("iterate partitions: %w", err)
	}

	if err := tx.Commit(); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to commit read transaction", err)
		return nil, fmt.Errorf("commit transaction: %w", err)
	}

	span.SetAttributes(attribute.Int("partition.count", len(partitions)))

	return partitions, nil
}

// Retention period constants for regulatory compliance.
const (
	minRetentionYears = 7
	daysPerYear       = 365
	hoursPerDay       = 24
)

// DetachPartition detaches a partition from the audit_logs parent table.
// The partition name must match the expected format to prevent SQL injection.
// Partitions whose data falls within the 7-year retention period cannot be detached.
func (pm *PartitionManager) DetachPartition(ctx context.Context, partitionName string) error {
	logger, tracer := pm.tracking(ctx)

	ctx, span := tracer.Start(ctx, "governance.partition_manager.detach_partition")
	defer span.End()

	if err := validatePartitionName(partitionName); err != nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "invalid partition name", err)
		return err
	}

	span.SetAttributes(attribute.String("partition.name", partitionName))

	if err := pm.validateRetentionPeriod(partitionName); err != nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "retention period active", err)

		return err
	}

	tx, err := pm.db.BeginTx(ctx, nil)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to begin transaction", err)
		return fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		_ = tx.Rollback()
	}()

	if err := auth.ApplyTenantSchema(ctx, tx); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to apply tenant schema", err)
		return fmt.Errorf("apply tenant schema: %w", err)
	}

	ddl := "ALTER TABLE audit_logs DETACH PARTITION " + partitionName // #nosec G202 -- partition name validated by partitionNameRegex, not from user input

	if _, err := tx.ExecContext(ctx, ddl); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to detach partition", err)
		return fmt.Errorf("detach partition %s: %w", partitionName, err)
	}

	if err := tx.Commit(); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to commit transaction", err)
		return fmt.Errorf("commit transaction: %w", err)
	}

	logger.Log(ctx, libLog.LevelInfo, "detached partition: "+partitionName)

	return nil
}

// DropPartition drops a previously detached partition table.
// The partition name must match the expected format to prevent SQL injection.
func (pm *PartitionManager) DropPartition(ctx context.Context, partitionName string) error {
	logger, tracer := pm.tracking(ctx)

	ctx, span := tracer.Start(ctx, "governance.partition_manager.drop_partition")
	defer span.End()

	if err := validatePartitionName(partitionName); err != nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "invalid partition name", err)
		return err
	}

	if err := pm.validateRetentionPeriod(partitionName); err != nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "retention period active", err)

		return err
	}

	span.SetAttributes(attribute.String("partition.name", partitionName))

	tx, err := pm.db.BeginTx(ctx, nil)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to begin transaction", err)
		return fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		_ = tx.Rollback()
	}()

	if err := auth.ApplyTenantSchema(ctx, tx); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to apply tenant schema", err)
		return fmt.Errorf("apply tenant schema: %w", err)
	}

	ddl := "DROP TABLE IF EXISTS " + partitionName // #nosec G202 -- partition name validated by partitionNameRegex, not from user input

	if _, err := tx.ExecContext(ctx, ddl); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to drop partition", err)
		return fmt.Errorf("drop partition %s: %w", partitionName, err)
	}

	if err := tx.Commit(); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to commit transaction", err)
		return fmt.Errorf("commit transaction: %w", err)
	}

	logger.Log(ctx, libLog.LevelInfo, "dropped partition: "+partitionName)

	return nil
}

// validatePartitionName checks that the partition name matches the expected format.
func validatePartitionName(name string) error {
	if !partitionNameRegex.MatchString(name) {
		return fmt.Errorf("%w: %s", ErrInvalidPartitionName, name)
	}

	return nil
}

// partitionName generates the partition table name for a given month start time.
func partitionName(t time.Time) string {
	return fmt.Sprintf("audit_logs_%04d_%02d", t.Year(), t.Month())
}

// monthStart returns the first day of the month offset by the given number of months from t.
func monthStart(t time.Time, monthsOffset int) time.Time {
	y, m, _ := t.Date()
	return time.Date(y, m+time.Month(monthsOffset), 1, 0, 0, 0, 0, time.UTC)
}

// parsePartitionBoundFromName derives the range [start, end) from a partition name like "audit_logs_2026_02".
func parsePartitionBoundFromName(name string) (time.Time, time.Time, error) {
	if !partitionNameRegex.MatchString(name) {
		return time.Time{}, time.Time{}, fmt.Errorf("%w: %s", ErrInvalidPartitionName, name)
	}

	var year, month int

	_, err := fmt.Sscanf(name, "audit_logs_%d_%d", &year, &month)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("%w: %s", ErrInvalidPartitionName, name)
	}

	start := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 1, 0)

	return start, end, nil
}

// parsePartitionBound parses PostgreSQL partition bound expressions like:
// "FOR VALUES FROM ('2026-02-01 00:00:00+00') TO ('2026-03-01 00:00:00+00')".
func parsePartitionBound(expr string) (time.Time, time.Time, error) {
	// Extract the two quoted values from the expression
	startStr, endStr, err := extractBoundValues(expr)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}

	// Try multiple timestamp formats PostgreSQL may use
	formats := []string{
		"2006-01-02 15:04:05+00",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}

	rangeStart := tryParseTime(startStr, formats)
	rangeEnd := tryParseTime(endStr, formats)

	if rangeStart.IsZero() || rangeEnd.IsZero() {
		return time.Time{}, time.Time{}, fmt.Errorf("%w: %q", ErrParseDateFromBound, expr)
	}

	return rangeStart, rangeEnd, nil
}

// extractBoundValues pulls the FROM and TO values from a partition bound expression.
func extractBoundValues(expr string) (string, string, error) {
	const (
		fromMarker = "FROM ('"
		toMarker   = "') TO ('"
		endMarker  = "')"
	)

	_, after, ok := strings.Cut(expr, fromMarker)
	if !ok {
		return "", "", fmt.Errorf("%w: %q", ErrMissingFromClause, expr)
	}

	afterFrom := after

	before, after, ok := strings.Cut(afterFrom, toMarker)
	if !ok {
		return "", "", fmt.Errorf("%w: %q", ErrMissingToClause, expr)
	}

	startStr := before
	afterTo := after

	before, _, ok = strings.Cut(afterTo, endMarker)
	if !ok {
		return "", "", fmt.Errorf("%w: %q", ErrMissingEndDelimiter, expr)
	}

	endStr := before

	return startStr, endStr, nil
}

// tryParseTime attempts to parse a time string with multiple formats.
func tryParseTime(s string, formats []string) time.Time {
	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return t.UTC()
		}
	}

	return time.Time{}
}

func (pm *PartitionManager) validateRetentionPeriod(partitionName string) error {
	_, endDate, err := parsePartitionBoundFromName(partitionName)
	if err != nil {
		return fmt.Errorf("parse partition bound: %w", err)
	}

	retentionDuration := time.Duration(minRetentionYears) * daysPerYear * hoursPerDay * time.Hour
	if pm.nowFn().Sub(endDate) < retentionDuration {
		return fmt.Errorf("partition %s data is within %d-year retention period: %w",
			partitionName, minRetentionYears, ErrRetentionPeriodActive)
	}

	return nil
}

// tracking extracts observability primitives from context, falling back to instance-level values.
func (pm *PartitionManager) tracking(ctx context.Context) (libLog.Logger, trace.Tracer) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	if logger == nil {
		logger = pm.logger
	}

	if tracer == nil {
		tracer = pm.tracer
	}

	if tracer == nil {
		tracer = noop.NewTracerProvider().Tracer("governance.partition_manager")
	}

	return logger, tracer
}
