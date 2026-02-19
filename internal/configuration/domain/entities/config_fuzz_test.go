//go:build unit && go1.18

package entities

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func FuzzMatchRuleConfigJSON(f *testing.F) {
	f.Add(`{"matchCurrency":true}`)
	f.Add(`{"matchScore":75}`)
	f.Add(`{"nested":{"key":"value"}}`)

	ctx := context.Background()
	contextID := uuid.New()

	f.Fuzz(func(t *testing.T, raw string) {
		if strings.ContainsRune(raw, 0) {
			return
		}

		var config map[string]any
		if err := json.Unmarshal([]byte(raw), &config); err != nil {
			return
		}

		if len(config) == 0 {
			return
		}

		ruleType := shared.RuleTypeExact
		if len(config)%3 == 1 {
			ruleType = shared.RuleTypeTolerance
		}

		if len(config)%3 == 2 {
			ruleType = shared.RuleTypeDateLag
		}

		rule, err := NewMatchRule(ctx, contextID, CreateMatchRuleInput{
			Priority: 1,
			Type:     ruleType,
			Config:   config,
		})
		if err != nil {
			if errors.Is(err, ErrRuleConfigRequired) {
				return
			}

			require.NoError(t, err)
		}

		payload, err := rule.ConfigJSON()
		require.NoError(t, err)

		var decoded map[string]any
		require.NoError(t, json.Unmarshal(payload, &decoded))
	})
}

func FuzzFieldMapMappingJSON(f *testing.F) {
	f.Add(`{"amount":"txn_amount"}`)
	f.Add(`{"currency":"txn_currency"}`)

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	f.Fuzz(func(t *testing.T, raw string) {
		if strings.ContainsRune(raw, 0) {
			return
		}

		var mapping map[string]any
		if err := json.Unmarshal([]byte(raw), &mapping); err != nil {
			return
		}

		if len(mapping) == 0 {
			return
		}

		fieldMap, err := NewFieldMap(
			ctx,
			contextID,
			sourceID,
			CreateFieldMapInput{Mapping: mapping},
		)
		require.NoError(t, err)

		payload, err := fieldMap.MappingJSON()
		require.NoError(t, err)

		var decoded map[string]any
		require.NoError(t, json.Unmarshal(payload, &decoded))
	})
}
