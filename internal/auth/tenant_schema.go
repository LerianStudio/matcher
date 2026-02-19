package auth

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel/attribute"

	libCommons "github.com/LerianStudio/lib-uncommons/v2/uncommons"
	"github.com/LerianStudio/lib-uncommons/v2/uncommons/assert"
	libOpentelemetry "github.com/LerianStudio/lib-uncommons/v2/uncommons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// SQLExecutor is an interface for executing SQL queries, implemented by *sql.Tx and *sql.Conn.
type SQLExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

var errNilTransaction = errors.New("transaction is nil")

// QuoteIdentifier escapes and quotes a PostgreSQL identifier to prevent SQL injection.
func QuoteIdentifier(identifier string) string {
	return "\"" + strings.ReplaceAll(identifier, "\"", "\"\"") + "\""
}

// ApplyTenantSchema sets the PostgreSQL search_path to the tenant's schema within a transaction.
// It only supports *sql.Tx executors to ensure schema isolation is properly scoped.
//
//nolint:dogsled // NewTrackingFromContext returns 4 values; we only need tracer
func ApplyTenantSchema(ctx context.Context, executor SQLExecutor) error {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "auth.apply_tenant_schema")

	defer span.End()

	asserter := assert.New(ctx, nil, constants.ApplicationName, "auth.apply_tenant_schema")
	if err := asserter.NotNil(ctx, executor, "executor is required"); err != nil {
		libOpentelemetry.HandleSpanError(span, "executor is nil", err)

		return fmt.Errorf("apply tenant schema: %w", err)
	}

	tenantID := GetTenantID(ctx)
	span.SetAttributes(attribute.String("tenant.id", tenantID))

	// Default tenant uses the connection's default search_path (typically 'public').
	// This avoids SET LOCAL overhead in single-tenant deployments.
	// If the default tenant is migrated to its own dedicated schema, this short-circuit must be removed.
	if tenantID == getDefaultTenantID() {
		span.SetAttributes(attribute.Bool("schema.skipped", true))

		return nil
	}

	if err := asserter.That(ctx, libCommons.IsUUID(tenantID), "invalid tenant id for schema", "tenant_id", tenantID); err != nil {
		libOpentelemetry.HandleSpanError(span, "invalid tenant id format", err)

		return fmt.Errorf("apply tenant schema: %w", err)
	}

	if tx, ok := executor.(*sql.Tx); ok {
		if tx == nil {
			libOpentelemetry.HandleSpanError(span, "nil transaction", errNilTransaction)

			return fmt.Errorf("apply tenant schema: %w", errNilTransaction)
		}

		_, err := tx.ExecContext(
			ctx,
			"SET LOCAL search_path TO "+QuoteIdentifier(tenantID)+", public",
		)
		if err != nil {
			libOpentelemetry.HandleSpanError(span, "set search_path failed", err)

			return fmt.Errorf("set search_path: %w", err)
		}

		span.SetAttributes(
			attribute.String("schema.name", tenantID),
			attribute.Bool("schema.applied", true),
		)

		return nil
	}

	if _, ok := executor.(*sql.Conn); ok {
		err := asserter.Never(
			ctx,
			"executor must be transaction to set tenant schema safely",
			"executor_type",
			"*sql.Conn",
		)
		libOpentelemetry.HandleSpanError(span, "invalid executor type", err)

		return fmt.Errorf("apply tenant schema: %w", err)
	}

	err := asserter.Never(
		ctx,
		"executor must be *sql.Tx",
		"executor_type",
		fmt.Sprintf("%T", executor),
	)
	libOpentelemetry.HandleSpanError(span, "unsupported executor type", err)

	return fmt.Errorf("apply tenant schema: %w", err)
}
