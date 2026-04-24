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
	"github.com/LerianStudio/matcher/tests/client"
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

// TestFeeRule_PredicateRoundTrip verifies that fee rule predicates survive
// a create → get → list round-trip with all fields intact (M13).
func TestFeeRule_PredicateRoundTrip(t *testing.T) {
	e2e.RunE2EWithTimeout(t, 2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, c *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, c)

			reconciliationContext := f.Context.NewContext().OneToOne().MustCreate(ctx)
			schedule := f.FeeSchedule.NewFeeSchedule().
				WithName("predicate-roundtrip-schedule").
				WithCurrency("USD").
				Parallel().
				WithFlatFee("flat", 1, "1.00").
				MustCreate(ctx)

			// Create a fee rule with both EQUALS and IN predicates.
			tc.Logf("Step 1: Create fee rule with EQUALS + IN predicates")
			created := f.FeeRule.NewFeeRule(reconciliationContext.ID).
				WithName("predicate-roundtrip").
				Left().
				WithFeeScheduleID(schedule.ID).
				WithPriority(1).
				WithEqualsPredicate("brand", "visa").
				WithInPredicate("channel", "pos", "ecommerce", "online").
				MustCreate(ctx)

			require.NotEmpty(t, created.ID)
			require.Len(t, created.Predicates, 2, "create response should contain both predicates")

			// Verify EQUALS predicate from create response.
			equalsPred := created.Predicates[0]
			assert.Equal(t, "brand", equalsPred.Field, "EQUALS predicate field")
			assert.Equal(t, "EQUALS", equalsPred.Operator, "EQUALS predicate operator")
			assert.Equal(t, "visa", equalsPred.Value, "EQUALS predicate value")

			// Verify IN predicate from create response.
			inPred := created.Predicates[1]
			assert.Equal(t, "channel", inPred.Field, "IN predicate field")
			assert.Equal(t, "IN", inPred.Operator, "IN predicate operator")
			assert.Equal(t, []string{"pos", "ecommerce", "online"}, inPred.Values, "IN predicate values")

			// Step 2: GET the fee rule and verify predicates survive the round-trip.
			tc.Logf("Step 2: GET fee rule and verify predicates round-trip")
			fetched, err := c.Configuration.GetFeeRule(ctx, created.ID)
			require.NoError(t, err)
			require.Len(t, fetched.Predicates, 2, "GET response should contain both predicates")

			// Find predicates by operator (order not guaranteed after persistence).
			var fetchedEquals, fetchedIn *client.FeeRulePredicateResponse
			for i := range fetched.Predicates {
				switch fetched.Predicates[i].Operator {
				case "EQUALS":
					fetchedEquals = &fetched.Predicates[i]
				case "IN":
					fetchedIn = &fetched.Predicates[i]
				}
			}

			require.NotNil(t, fetchedEquals, "GET should return EQUALS predicate")
			assert.Equal(t, "brand", fetchedEquals.Field)
			assert.Equal(t, "visa", fetchedEquals.Value)

			require.NotNil(t, fetchedIn, "GET should return IN predicate")
			assert.Equal(t, "channel", fetchedIn.Field)
			assert.ElementsMatch(t, []string{"pos", "ecommerce", "online"}, fetchedIn.Values)

			// Step 3: LIST fee rules and verify predicates in the list response.
			tc.Logf("Step 3: LIST fee rules and verify predicates in list response")
			listed, err := c.Configuration.ListFeeRules(ctx, reconciliationContext.ID)
			require.NoError(t, err)

			var listedRule *client.FeeRuleResponse
			for i := range listed {
				if listed[i].ID == created.ID {
					listedRule = &listed[i]
					break
				}
			}
			require.NotNil(t, listedRule, "created fee rule should appear in list")
			require.Len(t, listedRule.Predicates, 2, "list response should contain both predicates")

			// Step 4: UPDATE with new predicates and verify replacement.
			tc.Logf("Step 4: UPDATE predicates and verify replacement")
			newPredicates := []client.CreateFeeRulePredicateRequest{
				{Field: "country", Operator: "EQUALS", Value: "BR"},
				{Field: "mcc", Operator: "IN", Values: []string{"5411", "5541", "5812"}},
				{Field: "brand", Operator: "EQUALS", Value: "mastercard"},
			}
			updated, err := c.Configuration.UpdateFeeRule(ctx, created.ID, client.UpdateFeeRuleRequest{
				Predicates: &newPredicates,
			})
			require.NoError(t, err)
			require.Len(t, updated.Predicates, 3, "update should replace with 3 new predicates")

			// Verify the updated predicates via GET.
			refetched, err := c.Configuration.GetFeeRule(ctx, created.ID)
			require.NoError(t, err)
			require.Len(t, refetched.Predicates, 3, "refetched should have 3 predicates after update")

			// Count operators to verify structure.
			equalsCount := 0
			inCount := 0
			for _, p := range refetched.Predicates {
				switch p.Operator {
				case "EQUALS":
					equalsCount++
				case "IN":
					inCount++
				}
			}
			assert.Equal(t, 2, equalsCount, "should have 2 EQUALS predicates after update")
			assert.Equal(t, 1, inCount, "should have 1 IN predicate after update")

			tc.Logf("✓ M13: Fee rule predicate round-trip verified (create→get→list→update→get)")
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
