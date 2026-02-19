//go:build unit

package repositories

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFeeScheduleRepository_InterfaceDefined(t *testing.T) {
	t.Parallel()

	// Verify the interface is defined and can be referenced as a type.
	// The compile-time satisfaction check is in the adapter package:
	//   var _ matchingRepos.FeeScheduleRepository = (*Repository)(nil)
	// located in fee_schedule.postgresql.go
	var repo FeeScheduleRepository
	assert.Nil(t, repo)
}
