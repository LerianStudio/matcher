//go:build unit

package ports

import (
	postgres "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/fee_schedule"
)

// Compile-time interface compliance check.
var _ FeeScheduleRepository = (*postgres.Repository)(nil)
