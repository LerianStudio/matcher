// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package ports

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
)

//go:generate mockgen -destination=mocks/resolution_executor_mock.go -package=mocks . ResolutionExecutor

// AdjustmentInput contains parameters for entry adjustment.
type AdjustmentInput struct {
	Amount      decimal.Decimal
	Currency    string
	EffectiveAt time.Time
	Reason      value_objects.AdjustmentReasonCode
	Notes       string
}

// ResolutionExecutor executes exception resolution actions.
type ResolutionExecutor interface {
	ForceMatch(
		ctx context.Context,
		exceptionID uuid.UUID,
		notes string,
		overrideReason value_objects.OverrideReason,
	) error
	AdjustEntry(ctx context.Context, exceptionID uuid.UUID, input AdjustmentInput) error
}
