//go:build unit

package cross

import (
	"context"
	"database/sql"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"

	exceptionPorts "github.com/LerianStudio/matcher/internal/exception/ports"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
)

// ExceptionMatchingGateway end-to-end behaviour (CreateForceMatch,
// CreateAdjustment, context resolution with source fallback) is covered by
// tests/integration/exception/* and tests/integration/matching/* which
// exercise the real Postgres path. Only nil-receiver guards and pure helpers
// (mapReasonToAdjustmentType, mapSourceLookupError) stay at unit level.

func TestExceptionMatchingGateway_CreateForceMatch_NilReceiverGuard(t *testing.T) {
	t.Parallel()

	var gateway *ExceptionMatchingGateway

	err := gateway.CreateForceMatch(
		context.Background(),
		exceptionPorts.ForceMatchInput{TransactionID: uuid.New()},
	)
	require.ErrorIs(t, err, ErrNilTransactionRepository)
}

func TestExceptionMatchingGateway_CreateAdjustment_NilReceiverGuard(t *testing.T) {
	t.Parallel()

	var gateway *ExceptionMatchingGateway

	err := gateway.CreateAdjustment(
		context.Background(),
		exceptionPorts.CreateAdjustmentInput{TransactionID: uuid.New()},
	)
	require.ErrorIs(t, err, ErrNilAdjustmentRepository)
}

func TestExceptionMatchingGateway_ImplementsInterface(t *testing.T) {
	t.Parallel()
	var _ exceptionPorts.MatchingGateway = (*ExceptionMatchingGateway)(nil)
}

func TestMapSourceLookupError_NotFoundNormalization(t *testing.T) {
	t.Parallel()

	err := mapSourceLookupError(ErrIngestionJobNotFound, sql.ErrNoRows)
	require.ErrorIs(t, err, ErrSourceNotFound)
}

func TestMapReasonToAdjustmentType(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	assert.Equal(t, matchingEntities.AdjustmentTypeFXDifference,
		mapReasonToAdjustmentType(ctx, nil, "CURRENCY_CORRECTION"))
	// Nil logger must not panic on the default arm.
	assert.Equal(t, matchingEntities.AdjustmentTypeMiscellaneous,
		mapReasonToAdjustmentType(ctx, nil, "MANUAL_CORRECTION"))
}

func TestMapReasonToAdjustmentType_LogsOnDefault(t *testing.T) {
	t.Parallel()

	logger := &recordingLogger{}

	ctx := context.Background()

	// Known reason does not log.
	mapReasonToAdjustmentType(ctx, logger, "CURRENCY_CORRECTION")
	assert.Equal(t, 0, logger.warnCount, "known reasons must not emit warn logs")

	// Unknown reason logs a WARN and returns Miscellaneous.
	result := mapReasonToAdjustmentType(ctx, logger, "UNKNOWN_REASON_XYZ")
	assert.Equal(t, matchingEntities.AdjustmentTypeMiscellaneous, result)
	assert.Equal(t, 1, logger.warnCount, "unknown reasons must emit exactly one warn log")
}

// recordingLogger is a minimal libLog.Logger stub used to assert on the WARN
// emitted by mapReasonToAdjustmentType's default arm without taking a
// dependency on a full logger implementation.
type recordingLogger struct {
	warnCount int
}

func (r *recordingLogger) Log(_ context.Context, level libLog.Level, _ string, _ ...libLog.Field) {
	if level == libLog.LevelWarn {
		r.warnCount++
	}
}

func (r *recordingLogger) With(_ ...libLog.Field) libLog.Logger { return r }
func (r *recordingLogger) WithGroup(_ string) libLog.Logger     { return r }
func (r *recordingLogger) Enabled(_ libLog.Level) bool          { return true }
func (r *recordingLogger) Sync(_ context.Context) error         { return nil }
