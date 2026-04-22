// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package command

import (
	"context"
	"errors"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// Compile-time interface satisfaction check: the matching UseCase IS the
// MatchTrigger. Ingestion and scheduler workers hold this as the sharedPorts
// interface and Go's structural satisfaction handles the assignment at
// bootstrap — no wrapper adapter required.
var _ sharedPorts.MatchTrigger = (*UseCase)(nil)

// TriggerMatchForContext kicks off an asynchronous RunMatch for the given
// tenant/context pair. Fire-and-forget: errors are logged but do not affect
// the caller. This is the concrete behind sharedPorts.MatchTrigger; ingestion
// and the scheduler worker depend on the port interface.
//
// Errors that indicate a configuration gap (fee-rule wiring) are logged at
// ERROR with failure_kind="configuration" so operators can distinguish them
// from transient runtime failures which log at WARN with
// failure_kind="transient".
func (uc *UseCase) TriggerMatchForContext(ctx context.Context, tenantID, contextID uuid.UUID) {
	if uc == nil {
		return
	}

	logger, _, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // only logger needed

	runtime.SafeGoWithContextAndComponent(
		ctx,
		logger,
		"ingestion",
		"auto_match_trigger",
		runtime.KeepRunning,
		func(innerCtx context.Context) {
			input := RunMatchInput{
				TenantID:  tenantID,
				ContextID: contextID,
				Mode:      matchingVO.MatchRunModeCommit,
			}

			_, _, err := uc.RunMatch(innerCtx, input)
			if err != nil {
				innerLogger, _, _, _ := libCommons.NewTrackingFromContext(innerCtx)
				level := libLog.LevelWarn
				failureKind := "transient"

				if isAutoMatchConfigurationError(err) {
					level = libLog.LevelError
					failureKind = "configuration"
				}

				innerLogger.With(
					libLog.String("failure_kind", failureKind),
					libLog.String("context_id", contextID.String()),
				).Log(innerCtx, level, "auto-match trigger failed")
			}
		},
	)
}

// isAutoMatchConfigurationError returns true when the error signals a wiring
// gap rather than a transient runtime failure. Used by TriggerMatchForContext
// to pick log level + failure_kind label.
func isAutoMatchConfigurationError(err error) bool {
	return errors.Is(err, ErrFeeRulesReferenceMissingSchedules) ||
		errors.Is(err, ErrFeeRulesRequiredForNormalization) ||
		errors.Is(err, ErrNilFeeRuleProvider) ||
		errors.Is(err, ErrNilFeeScheduleRepository) ||
		errors.Is(err, ErrNilFeeVarianceRepository)
}
