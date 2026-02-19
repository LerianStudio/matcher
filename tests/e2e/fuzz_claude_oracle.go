//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
)

// Claude API token limits.
const (
	claudeGenerateMaxTokens = 16384
	claudeComputeMaxTokens  = 4096
)

// Sentinel errors for Claude oracle operations.
var (
	errClaudeEmptyResponse = errors.New("claude returned empty response")
	errNoOpenBrace         = errors.New("no '{' found in text")
	errNoMatchingBrace     = errors.New("no matching '}' found")
	errNoOpenBracket       = errors.New("no '[' found in text")
	errNoMatchingBracket   = errors.New("no matching ']' found")
)

// claudeSystemPrompt describes the exact fee calculation rules so Claude can both
// generate adversarial scenarios and independently compute expected results.
const claudeSystemPrompt = `You are a fee-calculation engine oracle. You understand EXACTLY how fee schedules work and can both generate adversarial test scenarios and compute fee results with perfect precision.

## Fee Structure Types

### FLAT
A fixed monetary amount. The structure contains:
  {"amount": "2.50"}
The fee is always the amount, regardless of the base.

### PERCENTAGE
A fraction of the base amount. The structure contains:
  {"rate": "0.015"}
IMPORTANT: The rate is already a fraction (0.015 means 1.5%). Do NOT multiply by 100.
Fee = base * rate.

### TIERED
Marginal (graduated) tiers. The structure contains:
  {"tiers": [{"min": "0", "max": "1000", "rate": "0.01"}, {"min": "1000", "max": "5000", "rate": "0.02"}, ...]}
Calculation uses MARGINAL algorithm: each tier's rate applies only to the portion of the base that falls within that tier's [min, max) range. The last tier may omit "max" or set it to "" meaning unbounded.

Algorithm:
  fee = 0
  remaining = base
  for each tier sorted by min ascending:
    tierWidth = tier.max - tier.min  (or remaining if unbounded)
    applicable = min(remaining, tierWidth)
    fee += applicable * tier.rate
    remaining -= applicable
    if remaining <= 0: break

## Application Orders

### PARALLEL
All fee items use the SAME base (the original gross amount). Items are sorted by priority ascending, each computes its fee from the gross.
  totalFee = sum of all item fees
  netAmount = grossAmount - totalFee

### CASCADING (Waterfall)
Items are sorted by priority ascending. The first item's base is the gross amount. Each subsequent item's base is the PREVIOUS remaining amount (previous base minus previous fee).
  remaining = grossAmount
  for each item:
    item.baseUsed = remaining
    item.fee = calculate(item, remaining)
    remaining = remaining - item.fee
  totalFee = grossAmount - remaining
  netAmount = remaining

EDGE CASE: In cascading mode, if cumulative fees exceed the gross amount, the net amount is clamped to 0 (never negative). Each fee still computes on its base even if the base goes to 0.

## Rounding

After computing each item's raw fee, round according to:
- roundingScale: number of decimal places (0 to 10)
- roundingMode:
  - HALF_UP: standard rounding. 0.5 rounds away from zero. 2.5 -> 3, -2.5 -> -3, 2.45 -> 2 (scale=0)
  - BANKERS: round half to even (banker's rounding). 0.5 rounds to nearest even digit. 2.5 -> 2, 3.5 -> 4, 2.25 -> 2.2 (scale=1)
  - FLOOR: always round toward negative infinity. 2.9 -> 2 (scale=0), -2.1 -> -3 (scale=0)
  - CEIL: always round toward positive infinity. 2.1 -> 3 (scale=0), -2.9 -> -2 (scale=0)
  - TRUNCATE: drop digits beyond the scale. 2.99 -> 2 (scale=0), 2.99 -> 2.9 (scale=1)

Rounding is applied to each fee item AFTER calculation, BEFORE summing.

## Important Rules
- Items are ALWAYS sorted by priority ascending before processing.
- All amounts are strings representing exact decimal values.
- Results must be strings with exact decimal precision.
- Percentages are already fractions (0.015 = 1.5%, NOT 15%).
- The totalFee is the sum of all rounded individual fees.
- The netAmount = grossAmount - totalFee (clamped to 0 minimum).`

// ClaudeOracle uses the Anthropic API to generate adversarial fee schedule
// scenarios and compute expected results independently.
type ClaudeOracle struct {
	client *anthropic.Client
	model  string
}

// NewClaudeOracle creates a new Claude oracle with the given API key and model.
// If apiKey is empty, it falls back to ANTHROPIC_API_KEY env var (SDK default).
func NewClaudeOracle(apiKey, model string) *ClaudeOracle {
	var client anthropic.Client

	if apiKey != "" {
		client = anthropic.NewClient(option.WithAPIKey(apiKey))
	} else {
		client = anthropic.NewClient()
	}

	return &ClaudeOracle{
		client: &client,
		model:  model,
	}
}

// GenerateAdversarialScenarios asks Claude to generate adversarial fee schedule
// test scenarios designed to expose bugs in a fee calculation engine.
func (co *ClaudeOracle) GenerateAdversarialScenarios(ctx context.Context, count int) ([]FuzzScenario, error) {
	userPrompt := fmt.Sprintf(`Generate exactly %d adversarial fee schedule test scenarios designed to expose bugs in a fee calculation engine. Each scenario should target a specific edge case or potential implementation flaw.

Vary across ALL of these dimensions:
- Structure types: FLAT, PERCENTAGE, TIERED (and combinations in multi-item schedules)
- Application orders: PARALLEL and CASCADING
- Rounding modes: HALF_UP, BANKERS, FLOOR, CEIL, TRUNCATE
- Rounding scales: 0, 1, 2, 3, and higher
- Gross amounts that hit tie-breaking boundaries (e.g., amounts ending in .5, .25, .005)
- Tier boundaries: amounts exactly at tier min/max, amounts spanning many tiers
- Cascading chains where fees exceed gross (net should clamp to 0)
- Very small amounts (0.01) and very large amounts (999999999.99)
- Multiple items where priority ordering matters
- Percentage rates that produce repeating decimals (1/3, 1/7, etc.)
- Zero-amount fees, zero-rate percentages
- Single-tier "tiered" structures

For each scenario, explain what bug it targets in the "attackVector" field.

Respond with ONLY a valid JSON array in this exact format (no surrounding text, no markdown fences):
[
  {
    "id": "adv_001",
    "attackVector": "Description of what implementation bug this scenario targets",
    "difficulty": 4,
    "schedule": {
      "name": "Descriptive schedule name",
      "currency": "USD",
      "applicationOrder": "PARALLEL",
      "roundingScale": 2,
      "roundingMode": "HALF_UP",
      "items": [
        {
          "name": "Item name",
          "priority": 1,
          "structureType": "PERCENTAGE",
          "structure": {"rate": "0.015"}
        }
      ]
    },
    "grossAmount": "1000.00"
  }
]

Generate exactly %d scenarios with IDs from adv_001 to adv_%03d.`, count, count, count)

	message, err := co.client.Messages.New(ctx, anthropic.MessageNewParams{
		Model:       anthropic.Model(co.model),
		MaxTokens:   claudeGenerateMaxTokens,
		Temperature: anthropic.Float(1.0),
		System: []anthropic.TextBlockParam{
			{Text: claudeSystemPrompt},
		},
		Messages: []anthropic.MessageParam{
			anthropic.NewUserMessage(anthropic.NewTextBlock(userPrompt)),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("claude generate scenarios: %w", err)
	}

	text := extractTextFromMessage(message)
	if text == "" {
		return nil, fmt.Errorf("scenario generation: %w", errClaudeEmptyResponse)
	}

	jsonStr, err := extractJSONArray(text)
	if err != nil {
		return nil, fmt.Errorf("extract JSON array from claude response: %w", err)
	}

	var scenarios []FuzzScenario
	if err := json.Unmarshal([]byte(jsonStr), &scenarios); err != nil {
		return nil, fmt.Errorf("unmarshal claude scenarios: %w", err)
	}

	for i := range scenarios {
		scenarios[i].Source = "claude_adversarial"
		scenarios[i].Category = "adversarial"
	}

	return scenarios, nil
}

// ComputeExpected asks Claude to independently compute the expected result
// for a given fee schedule and gross amount. Returns step-by-step reasoning
// along with the computed values.
func (co *ClaudeOracle) ComputeExpected(ctx context.Context, spec FuzzScheduleSpec, grossAmount string) (*FuzzExpectedResult, error) {
	scheduleJSON, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal schedule spec: %w", err)
	}

	userPrompt := fmt.Sprintf(`Compute the exact fee calculation result for the following schedule and gross amount.

Fee Schedule:
%s

Gross Amount: %s

Instructions:
1. Sort items by priority ascending.
2. For application order "%s":
   - If PARALLEL: each item's base is the gross amount (%s).
   - If CASCADING: first item's base is the gross amount, subsequent items use the remaining amount after previous fees.
3. For each item, compute the fee based on its structureType and structure.
4. Round each fee to %d decimal places using %s rounding.
5. Sum all rounded fees to get totalFee.
6. netAmount = grossAmount - totalFee (clamped to 0 if negative).

Show your step-by-step reasoning, then provide the final answer.

Respond with ONLY a valid JSON object (no surrounding text, no markdown fences):
{
  "totalFee": "exact decimal string",
  "netAmount": "exact decimal string",
  "itemFees": [
    {"name": "item name", "fee": "exact decimal string", "baseUsed": "exact decimal string"}
  ],
  "reasoning": "Step 1: ... Step 2: ..."
}`,
		string(scheduleJSON),
		grossAmount,
		spec.ApplicationOrder,
		grossAmount,
		spec.RoundingScale,
		spec.RoundingMode,
	)

	message, err := co.client.Messages.New(ctx, anthropic.MessageNewParams{
		Model:       anthropic.Model(co.model),
		MaxTokens:   claudeComputeMaxTokens,
		Temperature: anthropic.Float(0),
		System: []anthropic.TextBlockParam{
			{Text: claudeSystemPrompt},
		},
		Messages: []anthropic.MessageParam{
			anthropic.NewUserMessage(anthropic.NewTextBlock(userPrompt)),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("claude compute expected: %w", err)
	}

	text := extractTextFromMessage(message)
	if text == "" {
		return nil, fmt.Errorf("compute expected: %w", errClaudeEmptyResponse)
	}

	jsonStr, err := extractJSONObject(text)
	if err != nil {
		return nil, fmt.Errorf("extract JSON object from claude response: %w", err)
	}

	var result FuzzExpectedResult
	if err := json.Unmarshal([]byte(jsonStr), &result); err != nil {
		return nil, fmt.Errorf("unmarshal claude expected result: %w", err)
	}

	return &result, nil
}

// GenerateVariations asks Claude to generate variations of a scenario that
// exposed a discrepancy, probing the same bug from different angles.
func (co *ClaudeOracle) GenerateVariations(ctx context.Context, original FuzzScenario, apiResult *FuzzExpectedResult, count int) ([]FuzzScenario, error) {
	originalJSON, err := json.MarshalIndent(original, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal original scenario: %w", err)
	}

	apiResultJSON, err := json.MarshalIndent(apiResult, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal api result: %w", err)
	}

	userPrompt := fmt.Sprintf(`The following fee schedule scenario exposed a discrepancy between the expected and actual API result. Generate %d variations that probe the same class of bug from different angles.

Original scenario:
%s

API returned this result (which may differ from the expected):
%s

Create variations by:
- Changing the gross amount to nearby values that stress the same edge case
- Adjusting rounding scale or mode while keeping the same structure type
- Adding or removing fee items while preserving the attack vector
- Scaling amounts up or down by orders of magnitude
- Testing the boundary from both sides (just above and just below)

Respond with ONLY a valid JSON array (no surrounding text, no markdown fences):
[
  {
    "id": "var_001",
    "attackVector": "Variation of: <original attack> - <what this variation changes>",
    "difficulty": 4,
    "schedule": { ... full FuzzScheduleSpec ... },
    "grossAmount": "..."
  }
]

Generate exactly %d variations with IDs from var_001 to var_%03d.`, count, string(originalJSON), string(apiResultJSON), count, count)

	message, err := co.client.Messages.New(ctx, anthropic.MessageNewParams{
		Model:       anthropic.Model(co.model),
		MaxTokens:   claudeGenerateMaxTokens,
		Temperature: anthropic.Float(1.0),
		System: []anthropic.TextBlockParam{
			{Text: claudeSystemPrompt},
		},
		Messages: []anthropic.MessageParam{
			anthropic.NewUserMessage(anthropic.NewTextBlock(userPrompt)),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("claude generate variations: %w", err)
	}

	text := extractTextFromMessage(message)
	if text == "" {
		return nil, fmt.Errorf("generate variations: %w", errClaudeEmptyResponse)
	}

	jsonStr, err := extractJSONArray(text)
	if err != nil {
		return nil, fmt.Errorf("extract JSON array from claude variations response: %w", err)
	}

	var scenarios []FuzzScenario
	if err := json.Unmarshal([]byte(jsonStr), &scenarios); err != nil {
		return nil, fmt.Errorf("unmarshal claude variations: %w", err)
	}

	for i := range scenarios {
		scenarios[i].Source = "claude_variation"
		scenarios[i].Category = "adversarial"
	}

	return scenarios, nil
}

// extractTextFromMessage concatenates all text blocks from a Claude response message.
func extractTextFromMessage(message *anthropic.Message) string {
	var sb strings.Builder

	for _, block := range message.Content {
		if tb, ok := block.AsAny().(anthropic.TextBlock); ok {
			sb.WriteString(tb.Text)
		}
	}

	return sb.String()
}

// extractJSONObject extracts the first top-level JSON object from text,
// handling nested braces correctly.
func extractJSONObject(text string) (string, error) {
	start := strings.IndexByte(text, '{')
	if start == -1 {
		return "", errNoOpenBrace
	}

	depth := 0
	inString := false
	escaped := false

	for i := start; i < len(text); i++ {
		ch := text[i]

		if escaped {
			escaped = false
			continue
		}

		if ch == '\\' && inString {
			escaped = true
			continue
		}

		if ch == '"' {
			inString = !inString
			continue
		}

		if inString {
			continue
		}

		switch ch {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return text[start : i+1], nil
			}
		}
	}

	return "", fmt.Errorf("%w: '{' at position %d", errNoMatchingBrace, start)
}

// extractJSONArray extracts the first top-level JSON array from text,
// handling nested brackets and braces correctly.
func extractJSONArray(text string) (string, error) {
	start := strings.IndexByte(text, '[')
	if start == -1 {
		return "", errNoOpenBracket
	}

	depth := 0
	inString := false
	escaped := false

	for i := start; i < len(text); i++ {
		ch := text[i]

		if escaped {
			escaped = false
			continue
		}

		if ch == '\\' && inString {
			escaped = true
			continue
		}

		if ch == '"' {
			inString = !inString
			continue
		}

		if inString {
			continue
		}

		switch ch {
		case '[':
			depth++
		case ']':
			depth--
			if depth == 0 {
				return text[start : i+1], nil
			}
		}
	}

	return "", fmt.Errorf("%w: '[' at position %d", errNoMatchingBracket, start)
}
