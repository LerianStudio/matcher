//go:build e2e

package factories

import (
	"context"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/e2e/client"
)

// RuleFactory creates match rules for tests.
type RuleFactory struct {
	tc     *e2e.TestContext
	client *e2e.Client
}

// NewRuleFactory creates a new rule factory.
func NewRuleFactory(tc *e2e.TestContext, c *e2e.Client) *RuleFactory {
	return &RuleFactory{tc: tc, client: c}
}

// RuleBuilder builds match rule creation requests.
type RuleBuilder struct {
	factory   *RuleFactory
	contextID string
	req       client.CreateMatchRuleRequest
}

// NewRule starts building a new match rule.
func (f *RuleFactory) NewRule(contextID string) *RuleBuilder {
	return &RuleBuilder{
		factory:   f,
		contextID: contextID,
		req: client.CreateMatchRuleRequest{
			Priority: 1,
			Type:     "EXACT",
			Config:   map[string]any{},
		},
	}
}

// WithPriority sets the rule priority.
func (b *RuleBuilder) WithPriority(priority int) *RuleBuilder {
	b.req.Priority = priority
	return b
}

// WithType sets the rule type.
func (b *RuleBuilder) WithType(ruleType string) *RuleBuilder {
	b.req.Type = ruleType
	return b
}

// Exact sets the rule type to EXACT matching.
func (b *RuleBuilder) Exact() *RuleBuilder {
	return b.WithType("EXACT")
}

// Tolerance sets the rule type to TOLERANCE matching.
func (b *RuleBuilder) Tolerance() *RuleBuilder {
	return b.WithType("TOLERANCE")
}

// DateLag sets the rule type to DATE_LAG matching.
func (b *RuleBuilder) DateLag() *RuleBuilder {
	return b.WithType("DATE_LAG")
}

// WithConfig sets the rule configuration.
func (b *RuleBuilder) WithConfig(config map[string]any) *RuleBuilder {
	b.req.Config = config
	return b
}

// WithExactConfig sets configuration for exact matching.
func (b *RuleBuilder) WithExactConfig(matchCurrency, matchAmount bool) *RuleBuilder {
	b.req.Config = map[string]any{
		"matchCurrency": matchCurrency,
		"matchAmount":   matchAmount,
	}
	return b
}

// WithToleranceConfig sets configuration for tolerance matching.
func (b *RuleBuilder) WithToleranceConfig(absTolerance string) *RuleBuilder {
	b.req.Config = map[string]any{
		"absTolerance": absTolerance,
	}
	return b
}

// WithDateLagConfig sets configuration for date lag matching.
func (b *RuleBuilder) WithDateLagConfig(
	minDays, maxDays int,
	direction string,
	inclusive bool,
) *RuleBuilder {
	b.req.Config = map[string]any{
		"minDays":   minDays,
		"maxDays":   maxDays,
		"direction": direction,
		"inclusive": inclusive,
	}
	return b
}

// WithPercentToleranceConfig sets configuration for percentage tolerance matching.
func (b *RuleBuilder) WithPercentToleranceConfig(percentTolerance float64) *RuleBuilder {
	b.req.Config = map[string]any{
		"percentTolerance": percentTolerance,
	}
	return b
}

// Create creates the rule and registers cleanup.
func (b *RuleBuilder) Create(ctx context.Context) (*client.MatchRule, error) {
	created, err := b.factory.client.Configuration.CreateMatchRule(ctx, b.contextID, b.req)
	if err != nil {
		return nil, err
	}

	b.factory.tc.RegisterCleanup(func() error {
		return b.factory.client.Configuration.DeleteMatchRule(
			context.Background(),
			b.contextID,
			created.ID,
		)
	})

	b.factory.tc.Logf(
		"Created rule: %s (priority=%d, type=%s)",
		created.ID,
		created.Priority,
		created.Type,
	)
	return created, nil
}

// MustCreate creates the rule and panics on error.
func (b *RuleBuilder) MustCreate(ctx context.Context) *client.MatchRule {
	created, err := b.Create(ctx)
	if err != nil {
		panic(err)
	}
	return created
}
