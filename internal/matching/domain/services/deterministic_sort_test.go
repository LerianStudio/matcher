//go:build unit

package services

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestDeterministicSort_Rules(t *testing.T) {
	t.Parallel()

	rules := []RuleDefinition{
		{ID: uuid.MustParse("00000000-0000-0000-0000-000000000201"), Priority: 2, Type: "B"},
		{ID: uuid.MustParse("00000000-0000-0000-0000-000000000202"), Priority: 1, Type: "B"},
		{ID: uuid.MustParse("00000000-0000-0000-0000-000000000203"), Priority: 1, Type: "A"},
		{ID: uuid.MustParse("00000000-0000-0000-0000-000000000204"), Priority: 1, Type: "A"},
	}

	SortRules(rules)

	require.Equal(t, uuid.MustParse("00000000-0000-0000-0000-000000000203"), rules[0].ID)
	require.Equal(t, uuid.MustParse("00000000-0000-0000-0000-000000000204"), rules[1].ID)
	require.Equal(t, uuid.MustParse("00000000-0000-0000-0000-000000000202"), rules[2].ID)
	require.Equal(t, uuid.MustParse("00000000-0000-0000-0000-000000000201"), rules[3].ID)
}

func TestDeterministicSort_Rules_Empty(t *testing.T) {
	t.Parallel()

	rules := []RuleDefinition{}
	SortRules(rules)
	require.Empty(t, rules)
}

func TestDeterministicSort_Rules_Tied(t *testing.T) {
	t.Parallel()

	rules := []RuleDefinition{
		{ID: uuid.MustParse("00000000-0000-0000-0000-000000000211"), Priority: 1, Type: "A"},
		{ID: uuid.MustParse("00000000-0000-0000-0000-000000000210"), Priority: 1, Type: "A"},
	}

	SortRules(rules)

	require.Equal(t, uuid.MustParse("00000000-0000-0000-0000-000000000210"), rules[0].ID)
	require.Equal(t, uuid.MustParse("00000000-0000-0000-0000-000000000211"), rules[1].ID)
}

func TestDeterministicSort_Transactions(t *testing.T) {
	t.Parallel()

	txs := []CandidateTransaction{
		{
			ID:             uuid.MustParse("00000000-0000-0000-0000-000000000302"),
			Amount:         decimal.RequireFromString("10.00"),
			OriginalAmount: decimal.RequireFromString("10.00"),
			Currency:       "USD",
			Date:           time.Date(2026, 1, 2, 10, 0, 0, 0, time.UTC),
			Reference:      "B",
		},
		{
			ID:             uuid.MustParse("00000000-0000-0000-0000-000000000301"),
			Amount:         decimal.RequireFromString("9.00"),
			OriginalAmount: decimal.RequireFromString("9.00"),
			Currency:       "USD",
			Date:           time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
			Reference:      "A",
		},
		{
			ID:             uuid.MustParse("00000000-0000-0000-0000-000000000303"),
			Amount:         decimal.RequireFromString("9.00"),
			OriginalAmount: decimal.RequireFromString("9.00"),
			Currency:       "EUR",
			Date:           time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
			Reference:      "A",
		},
		{
			ID:             uuid.MustParse("00000000-0000-0000-0000-000000000304"),
			Amount:         decimal.RequireFromString("9.00"),
			OriginalAmount: decimal.RequireFromString("9.00"),
			Currency:       "EUR",
			Date:           time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
			Reference:      "B",
		},
	}

	SortTransactions(txs)

	require.Equal(t, uuid.MustParse("00000000-0000-0000-0000-000000000303"), txs[0].ID)
	require.Equal(t, uuid.MustParse("00000000-0000-0000-0000-000000000304"), txs[1].ID)
	require.Equal(t, uuid.MustParse("00000000-0000-0000-0000-000000000301"), txs[2].ID)
	require.Equal(t, uuid.MustParse("00000000-0000-0000-0000-000000000302"), txs[3].ID)
}

func TestDeterministicSort_Transactions_Empty(t *testing.T) {
	t.Parallel()

	txs := []CandidateTransaction{}
	SortTransactions(txs)
	require.Empty(t, txs)
}

func TestDeterministicSort_Transactions_Tied(t *testing.T) {
	t.Parallel()

	date := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	txs := []CandidateTransaction{
		{
			ID:             uuid.MustParse("00000000-0000-0000-0000-000000000310"),
			Amount:         decimal.RequireFromString("9.00"),
			OriginalAmount: decimal.RequireFromString("9.00"),
			Currency:       "USD",
			Date:           date,
			Reference:      "A",
		},
		{
			ID:             uuid.MustParse("00000000-0000-0000-0000-000000000309"),
			Amount:         decimal.RequireFromString("9.00"),
			OriginalAmount: decimal.RequireFromString("9.00"),
			Currency:       "USD",
			Date:           date,
			Reference:      "A",
		},
	}

	SortTransactions(txs)

	require.Equal(t, uuid.MustParse("00000000-0000-0000-0000-000000000309"), txs[0].ID)
	require.Equal(t, uuid.MustParse("00000000-0000-0000-0000-000000000310"), txs[1].ID)
}
