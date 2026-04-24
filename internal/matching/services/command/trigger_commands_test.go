// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"testing"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// Compile-time assertion: the matching UseCase satisfies the MatchTrigger
// port. T-004 absorbed the former MatchTriggerAdapter; this test locks
// the signature so the adapter cannot quietly return.
func TestUseCase_ImplementsMatchTrigger(t *testing.T) {
	t.Parallel()

	var _ sharedPorts.MatchTrigger = (*UseCase)(nil)
}
