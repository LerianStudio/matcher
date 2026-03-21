//go:build e2e

package journeys

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/e2e/client"
	"github.com/LerianStudio/matcher/tests/e2e/factories"
)

func TestFeeRule_CRUD(t *testing.T) {
	e2e.RunE2EWithTimeout(t, 2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, c *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, c)

			reconciliationContext := f.Context.NewContext().OneToOne().MustCreate(ctx)
			scheduleA := f.FeeSchedule.NewFeeSchedule().
				WithName("fee-rule-schedule-a").
				WithCurrency("USD").
				Parallel().
				WithFlatFee("flat-a", 1, "1.25").
				MustCreate(ctx)
			scheduleB := f.FeeSchedule.NewFeeSchedule().
				WithName("fee-rule-schedule-b").
				WithCurrency("USD").
				Parallel().
				WithFlatFee("flat-b", 1, "2.50").
				MustCreate(ctx)

			tc.Logf("Step 1: create fee rule")
			created := f.FeeRule.NewFeeRule(reconciliationContext.ID).
				WithName("visa-right").
				Right().
				WithFeeScheduleID(scheduleA.ID).
				WithPriority(5).
				WithEqualsPredicate("brand", "visa").
				MustCreate(ctx)

			require.NotEmpty(t, created.ID)
			assert.Equal(t, reconciliationContext.ID, created.ContextID)
			assert.Equal(t, "RIGHT", created.Side)
			assert.Equal(t, scheduleA.ID, created.FeeScheduleID)
			assert.Equal(t, 5, created.Priority)
			require.Len(t, created.Predicates, 1)

			tc.Logf("Step 2: get fee rule")
			fetched, err := c.Configuration.GetFeeRule(ctx, created.ID)
			require.NoError(t, err)
			assert.Equal(t, created.ID, fetched.ID)
			assert.Equal(t, created.Name, fetched.Name)

			tc.Logf("Step 3: list fee rules for context")
			listed, err := c.Configuration.ListFeeRules(ctx, reconciliationContext.ID)
			require.NoError(t, err)
			found := false
			for _, rule := range listed {
				if rule.ID == created.ID {
					found = true
					assert.Equal(t, 5, rule.Priority)
					break
				}
			}
			assert.True(t, found, "created fee rule should appear in list")

			tc.Logf("Step 4: update fee rule")
			newName := tc.UniqueName("visa-right-updated")
			newSide := "LEFT"
			newPriority := 3
			newPredicates := []client.CreateFeeRulePredicateRequest{{
				Field:    "channel",
				Operator: "IN",
				Values:   []string{"pos", "ecommerce"},
			}}
			updated, err := c.Configuration.UpdateFeeRule(ctx, created.ID, client.UpdateFeeRuleRequest{
				Name:          &newName,
				Side:          &newSide,
				FeeScheduleID: &scheduleB.ID,
				Priority:      &newPriority,
				Predicates:    &newPredicates,
			})
			require.NoError(t, err)
			assert.Equal(t, newName, updated.Name)
			assert.Equal(t, "LEFT", updated.Side)
			assert.Equal(t, scheduleB.ID, updated.FeeScheduleID)
			assert.Equal(t, 3, updated.Priority)
			require.Len(t, updated.Predicates, 1)

			tc.Logf("Step 5: delete fee rule")
			err = c.Configuration.DeleteFeeRule(ctx, created.ID)
			require.NoError(t, err)

			_, err = c.Configuration.GetFeeRule(ctx, created.ID)
			require.Error(t, err)
			var apiErr *client.APIError
			require.True(t, errors.As(err, &apiErr))
			assert.True(t, apiErr.IsNotFound(), "deleted fee rule should return 404")
		})
}

func TestFeeRule_ValidationAndConflicts(t *testing.T) {
	e2e.RunE2EWithTimeout(t, 2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, c *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, c)

			reconciliationContext := f.Context.NewContext().OneToOne().MustCreate(ctx)
			schedule := f.FeeSchedule.NewFeeSchedule().
				WithName("fee-rule-validation-schedule").
				WithCurrency("USD").
				Parallel().
				WithFlatFee("flat", 1, "1.00").
				MustCreate(ctx)

			tc.Logf("Validation case 1: invalid side should be rejected")
			_, err := c.Configuration.CreateFeeRule(ctx, reconciliationContext.ID, client.CreateFeeRuleRequest{
				Side:          "BOTH",
				FeeScheduleID: schedule.ID,
				Name:          tc.UniqueName("invalid-side"),
				Priority:      1,
				Predicates: []client.CreateFeeRulePredicateRequest{{
					Field:    "brand",
					Operator: "EQUALS",
					Value:    "visa",
				}},
			})
			require.Error(t, err)
			var apiErr *client.APIError
			require.True(t, errors.As(err, &apiErr))
			assert.True(t, apiErr.IsBadRequest(), "invalid side should return 400")

			tc.Logf("Validation case 2: invalid predicate operator should be rejected")
			_, err = c.Configuration.CreateFeeRule(ctx, reconciliationContext.ID, client.CreateFeeRuleRequest{
				Side:          "ANY",
				FeeScheduleID: schedule.ID,
				Name:          tc.UniqueName("invalid-operator"),
				Priority:      2,
				Predicates: []client.CreateFeeRulePredicateRequest{{
					Field:    "brand",
					Operator: "LIKE",
					Value:    "visa",
				}},
			})
			require.Error(t, err)
			require.True(t, errors.As(err, &apiErr))
			assert.True(t, apiErr.IsBadRequest(), "invalid operator should return 400")

			tc.Logf("Validation case 3: duplicate priority across sides should conflict")
			first := f.FeeRule.NewFeeRule(reconciliationContext.ID).
				WithName("priority-anchor").
				Left().
				WithFeeScheduleID(schedule.ID).
				WithPriority(7).
				WithEqualsPredicate("brand", "visa").
				MustCreate(ctx)
			require.NotEmpty(t, first.ID)

			_, err = c.Configuration.CreateFeeRule(ctx, reconciliationContext.ID, client.CreateFeeRuleRequest{
				Side:          "RIGHT",
				FeeScheduleID: schedule.ID,
				Name:          tc.UniqueName("priority-conflict"),
				Priority:      7,
				Predicates: []client.CreateFeeRulePredicateRequest{{
					Field:    "brand",
					Operator: "EQUALS",
					Value:    "mastercard",
				}},
			})
			require.Error(t, err)
			require.True(t, errors.As(err, &apiErr))
			assert.True(t, apiErr.IsConflict(), "duplicate priority should return 409 even across different sides")

			tc.Logf("Validation case 4: too many predicates should be rejected")
			predicates := make([]client.CreateFeeRulePredicateRequest, 0, 51)
			for i := 0; i < 51; i++ {
				predicates = append(predicates, client.CreateFeeRulePredicateRequest{
					Field:    "field",
					Operator: "EQUALS",
					Value:    "value",
				})
			}

			_, err = c.Configuration.CreateFeeRule(ctx, reconciliationContext.ID, client.CreateFeeRuleRequest{
				Side:          "ANY",
				FeeScheduleID: schedule.ID,
				Name:          tc.UniqueName("too-many-predicates"),
				Priority:      9,
				Predicates:    predicates,
			})
			require.Error(t, err)
			require.True(t, errors.As(err, &apiErr))
			assert.True(t, apiErr.IsBadRequest(), "too many predicates should return 400")
		})
}
