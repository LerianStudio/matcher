//go:build e2e

package factories

import (
	"github.com/LerianStudio/matcher/tests/e2e"
)

// Factories provides access to all test data factories.
type Factories struct {
	Context     *ContextFactory
	Source      *SourceFactory
	Rule        *RuleFactory
	FeeSchedule *FeeScheduleFactory
	FeeRule     *FeeRuleFactory
}

// New creates all factories for a test context.
func New(tc *e2e.TestContext, client *e2e.Client) *Factories {
	return &Factories{
		Context:     NewContextFactory(tc, client),
		Source:      NewSourceFactory(tc, client),
		Rule:        NewRuleFactory(tc, client),
		FeeSchedule: NewFeeScheduleFactory(tc, client),
		FeeRule:     NewFeeRuleFactory(tc, client),
	}
}
