//go:build e2e

package factories

import (
	"context"
	"errors"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/client"
)

// FeeScheduleFactory creates fee schedules for tests.
type FeeScheduleFactory struct {
	tc     *e2e.TestContext
	client *e2e.Client
}

// NewFeeScheduleFactory creates a new fee schedule factory.
func NewFeeScheduleFactory(tc *e2e.TestContext, c *e2e.Client) *FeeScheduleFactory {
	return &FeeScheduleFactory{tc: tc, client: c}
}

// FeeScheduleBuilder builds fee schedule creation requests.
type FeeScheduleBuilder struct {
	factory *FeeScheduleFactory
	req     client.CreateFeeScheduleRequest
}

// NewFeeSchedule starts building a new fee schedule.
func (f *FeeScheduleFactory) NewFeeSchedule() *FeeScheduleBuilder {
	return &FeeScheduleBuilder{
		factory: f,
		req: client.CreateFeeScheduleRequest{
			Name:             f.tc.UniqueName("fee-schedule"),
			Currency:         "USD",
			ApplicationOrder: "PARALLEL",
			RoundingScale:    2,
			RoundingMode:     "HALF_UP",
			Items:            []client.CreateFeeScheduleItemRequest{},
		},
	}
}

// WithName sets the schedule name (auto-prefixed with test context).
func (b *FeeScheduleBuilder) WithName(name string) *FeeScheduleBuilder {
	b.req.Name = b.factory.tc.UniqueName(name)
	return b
}

// WithRawName sets the schedule name without prefix.
func (b *FeeScheduleBuilder) WithRawName(name string) *FeeScheduleBuilder {
	b.req.Name = name
	return b
}

// WithCurrency sets the schedule currency.
func (b *FeeScheduleBuilder) WithCurrency(currency string) *FeeScheduleBuilder {
	b.req.Currency = currency
	return b
}

// Parallel sets the application order to PARALLEL.
func (b *FeeScheduleBuilder) Parallel() *FeeScheduleBuilder {
	b.req.ApplicationOrder = "PARALLEL"
	return b
}

// Cascading sets the application order to CASCADING.
func (b *FeeScheduleBuilder) Cascading() *FeeScheduleBuilder {
	b.req.ApplicationOrder = "CASCADING"
	return b
}

// WithRoundingScale sets the rounding scale.
func (b *FeeScheduleBuilder) WithRoundingScale(scale int) *FeeScheduleBuilder {
	b.req.RoundingScale = scale
	return b
}

// WithRoundingMode sets the rounding mode.
func (b *FeeScheduleBuilder) WithRoundingMode(mode string) *FeeScheduleBuilder {
	b.req.RoundingMode = mode
	return b
}

// WithFlatFee adds a flat fee item.
func (b *FeeScheduleBuilder) WithFlatFee(name string, priority int, amount string) *FeeScheduleBuilder {
	b.req.Items = append(b.req.Items, client.CreateFeeScheduleItemRequest{
		Name:          name,
		Priority:      priority,
		StructureType: "FLAT",
		Structure:     map[string]any{"amount": amount},
	})
	return b
}

// WithPercentageFee adds a percentage fee item.
func (b *FeeScheduleBuilder) WithPercentageFee(name string, priority int, rate string) *FeeScheduleBuilder {
	b.req.Items = append(b.req.Items, client.CreateFeeScheduleItemRequest{
		Name:          name,
		Priority:      priority,
		StructureType: "PERCENTAGE",
		Structure:     map[string]any{"rate": rate},
	})
	return b
}

// WithItem adds a custom fee schedule item.
func (b *FeeScheduleBuilder) WithItem(item client.CreateFeeScheduleItemRequest) *FeeScheduleBuilder {
	b.req.Items = append(b.req.Items, item)
	return b
}

// GetRequest returns the underlying request for inspection.
func (b *FeeScheduleBuilder) GetRequest() client.CreateFeeScheduleRequest {
	return b.req
}

// Create creates the fee schedule and registers cleanup.
func (b *FeeScheduleBuilder) Create(ctx context.Context) (*client.FeeScheduleResponse, error) {
	created, err := b.factory.client.FeeSchedule.CreateFeeSchedule(ctx, b.req)
	if err != nil {
		return nil, err
	}

	b.factory.tc.RegisterCleanup(func() error {
		err := b.factory.client.FeeSchedule.DeleteFeeSchedule(context.Background(), created.ID)
		var apiErr *client.APIError
		if errors.As(err, &apiErr) && apiErr.IsNotFound() {
			return nil
		}

		return err
	})

	b.factory.tc.Logf("Created fee schedule: %s (%s)", created.Name, created.ID)
	return created, nil
}

// MustCreate creates the fee schedule and panics on error.
func (b *FeeScheduleBuilder) MustCreate(ctx context.Context) *client.FeeScheduleResponse {
	created, err := b.Create(ctx)
	if err != nil {
		panic(err)
	}
	return created
}
