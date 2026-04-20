// Package exception_creator provides PostgreSQL persistence for exception records.
package exception_creator

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/matching/domain/enums"
	"github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	matchingPorts "github.com/LerianStudio/matcher/internal/matching/ports"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	sharedException "github.com/LerianStudio/matcher/internal/shared/domain/exception"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

// Repository persists exceptions in Postgres.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new exception creator repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

// CreateExceptions inserts or updates exception rows for each unmatched transaction.
// Uses UPSERT to update reasons on reruns, but never downgrades a specific reason back to UNMATCHED.
// Severity is calculated using PRD AC-002 thresholds based on amount, age, and source type.
func (repo *Repository) CreateExceptions(
	ctx context.Context,
	contextID, runID uuid.UUID,
	inputs []matchingPorts.ExceptionTransactionInput,
	regulatorySourceTypes []string,
) error {
	return repo.createExceptions(ctx, nil, contextID, runID, inputs, regulatorySourceTypes)
}

// CreateExceptionsWithTx inserts exception rows using the provided transaction.
func (repo *Repository) CreateExceptionsWithTx(
	ctx context.Context,
	tx repositories.Tx,
	contextID, runID uuid.UUID,
	inputs []matchingPorts.ExceptionTransactionInput,
	regulatorySourceTypes []string,
) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if tx == nil {
		return ErrInvalidTx
	}

	return repo.createExceptions(ctx, tx, contextID, runID, inputs, regulatorySourceTypes)
}

func (repo *Repository) createExceptions(
	ctx context.Context,
	tx *sql.Tx,
	_, _ uuid.UUID,
	inputs []matchingPorts.ExceptionTransactionInput,
	regulatorySourceTypes []string,
) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if len(inputs) == 0 {
		return nil
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.create_exceptions_batch")

	defer span.End()

	now := time.Now().UTC()
	severityRules := sharedException.DefaultSeverityRules(regulatorySourceTypes)

	_, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(execTx *sql.Tx) (struct{}, error) {
			stmt, err := execTx.PrepareContext(
				ctx,
				`INSERT INTO exceptions (id, transaction_id, severity, status, reason, created_at, updated_at)
			 VALUES ($1,$2,$3,'OPEN',$4,$5,$6)
			 ON CONFLICT (transaction_id) DO UPDATE SET
			   severity = CASE
			     WHEN EXCLUDED.severity = 'CRITICAL' THEN EXCLUDED.severity
			     WHEN EXCLUDED.severity = 'HIGH' AND exceptions.severity NOT IN ('CRITICAL') THEN EXCLUDED.severity
			     WHEN EXCLUDED.severity = 'MEDIUM' AND exceptions.severity NOT IN ('CRITICAL', 'HIGH') THEN EXCLUDED.severity
			     ELSE exceptions.severity
			   END,
			   reason = CASE
			     WHEN EXCLUDED.reason = 'UNMATCHED' AND exceptions.reason <> 'UNMATCHED'
			       THEN exceptions.reason
			     ELSE EXCLUDED.reason
			   END,
			   updated_at = EXCLUDED.updated_at`,
			)
			if err != nil {
				return struct{}{}, fmt.Errorf("prepare insert exception: %w", err)
			}

			defer func() { _ = stmt.Close() }()

			for _, input := range inputs {
				if input.TransactionID == uuid.Nil {
					continue
				}

				severity := classifySeverity(ctx, input, now, severityRules)
				reason := sanitizeInputReason(input.Reason)

				if _, err := stmt.ExecContext(ctx, uuid.New().String(), input.TransactionID.String(), severity.String(), reason, now, now); err != nil {
					return struct{}{}, fmt.Errorf("insert exception: %w", err)
				}
			}

			return struct{}{}, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("failed to create exceptions: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create exceptions", wrappedErr)

		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to create exceptions")

		return wrappedErr
	}

	return nil
}

// classifySeverity calculates the severity for an exception based on PRD AC-002 rules.
func classifySeverity(
	ctx context.Context,
	input matchingPorts.ExceptionTransactionInput,
	now time.Time,
	rules []sharedException.SeverityRule,
) sharedException.ExceptionSeverity {
	ageHours := 0

	if !input.TransactionDate.IsZero() {
		duration := now.Sub(input.TransactionDate)
		if duration > 0 {
			ageHours = int(duration.Hours())
		}
	}

	classificationInput := sharedException.SeverityClassificationInput{
		AmountAbsBase: input.AmountAbsBase,
		AgeHours:      ageHours,
		SourceType:    input.SourceType,
		FXMissing:     input.FXMissing,
	}

	result, err := sharedException.ClassifyExceptionSeverity(classificationInput, rules)
	if err != nil {
		logger, _, _, _ := libCommons.NewTrackingFromContext(ctx)
		logger.With(
			libLog.Err(err),
			libLog.String("amount", classificationInput.AmountAbsBase.String()),
			libLog.Any("age_hours", classificationInput.AgeHours),
			libLog.String("source_type", classificationInput.SourceType),
			libLog.Any("fx_missing", classificationInput.FXMissing),
			libLog.Any("rules_count", len(rules)),
		).Log(ctx, libLog.LevelWarn, "severity classification failed, defaulting to MEDIUM")

		return sharedException.ExceptionSeverityMedium
	}

	return result.Severity
}

// sanitizeInputReason validates and sanitizes the reason from input.
func sanitizeInputReason(reason string) string {
	if strings.TrimSpace(reason) == "" {
		return enums.ReasonUnmatched
	}

	return enums.SanitizeReason(reason)
}

var _ matchingPorts.ExceptionCreator = (*Repository)(nil)
