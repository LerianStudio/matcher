//go:build e2e

package factories

import (
	"context"
	"errors"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/e2e/client"
)

// FeeRuleFactory creates fee rules for tests.
type FeeRuleFactory struct {
	tc     *e2e.TestContext
	client *e2e.Client
}

// NewFeeRuleFactory creates a new fee rule factory.
func NewFeeRuleFactory(tc *e2e.TestContext, c *e2e.Client) *FeeRuleFactory {
	return &FeeRuleFactory{tc: tc, client: c}
}

// FeeRuleBuilder builds fee rule creation requests.
type FeeRuleBuilder struct {
	factory   *FeeRuleFactory
	contextID string
	req       client.CreateFeeRuleRequest
}

// NewFeeRule starts building a new fee rule.
func (f *FeeRuleFactory) NewFeeRule(contextID string) *FeeRuleBuilder {
	return &FeeRuleBuilder{
		factory:   f,
		contextID: contextID,
		req: client.CreateFeeRuleRequest{
			Side:       "ANY",
			Name:       f.tc.UniqueName("fee-rule"),
			Priority:   0,
			Predicates: []client.CreateFeeRulePredicateRequest{},
		},
	}
}

// WithName sets the fee rule name with test prefixing.
func (b *FeeRuleBuilder) WithName(name string) *FeeRuleBuilder {
	b.req.Name = b.factory.tc.UniqueName(name)
	return b
}

// WithRawName sets the fee rule name without prefixing.
func (b *FeeRuleBuilder) WithRawName(name string) *FeeRuleBuilder {
	b.req.Name = name
	return b
}

// WithSide sets the fee rule side.
func (b *FeeRuleBuilder) WithSide(side string) *FeeRuleBuilder {
	b.req.Side = side
	return b
}

// Any applies the fee rule to both sides.
func (b *FeeRuleBuilder) Any() *FeeRuleBuilder {
	return b.WithSide("ANY")
}

// Left applies the fee rule to the left side.
func (b *FeeRuleBuilder) Left() *FeeRuleBuilder {
	return b.WithSide("LEFT")
}

// Right applies the fee rule to the right side.
func (b *FeeRuleBuilder) Right() *FeeRuleBuilder {
	return b.WithSide("RIGHT")
}

// WithFeeScheduleID sets the referenced fee schedule ID.
func (b *FeeRuleBuilder) WithFeeScheduleID(feeScheduleID string) *FeeRuleBuilder {
	b.req.FeeScheduleID = feeScheduleID
	return b
}

// WithPriority sets the fee rule priority.
func (b *FeeRuleBuilder) WithPriority(priority int) *FeeRuleBuilder {
	b.req.Priority = priority
	return b
}

// WithEqualsPredicate adds an EQUALS predicate.
func (b *FeeRuleBuilder) WithEqualsPredicate(field, value string) *FeeRuleBuilder {
	b.req.Predicates = append(b.req.Predicates, client.CreateFeeRulePredicateRequest{
		Field:    field,
		Operator: "EQUALS",
		Value:    value,
	})
	return b
}

// WithInPredicate adds an IN predicate.
func (b *FeeRuleBuilder) WithInPredicate(field string, values ...string) *FeeRuleBuilder {
	b.req.Predicates = append(b.req.Predicates, client.CreateFeeRulePredicateRequest{
		Field:    field,
		Operator: "IN",
		Values:   values,
	})
	return b
}

// Create creates the fee rule and registers cleanup.
func (b *FeeRuleBuilder) Create(ctx context.Context) (*client.FeeRuleResponse, error) {
	created, err := b.factory.client.Configuration.CreateFeeRule(ctx, b.contextID, b.req)
	if err != nil {
		return nil, err
	}

	b.factory.tc.RegisterCleanup(func() error {
		err := b.factory.client.Configuration.DeleteFeeRule(context.Background(), created.ID)
		var apiErr *client.APIError
		if errors.As(err, &apiErr) && apiErr.IsNotFound() {
			return nil
		}

		return err
	})

	b.factory.tc.Logf("Created fee rule: %s (%s)", created.Name, created.ID)
	return created, nil
}

// MustCreate creates the fee rule and panics on error.
func (b *FeeRuleBuilder) MustCreate(ctx context.Context) *client.FeeRuleResponse {
	created, err := b.Create(ctx)
	if err != nil {
		panic(err)
	}

	return created
}
