// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package services

import "sort"

// SortRules sorts rules deterministically by priority, type, and ID.
func SortRules(rules []RuleDefinition) {
	sort.SliceStable(rules, func(i, j int) bool {
		if rules[i].Priority != rules[j].Priority {
			return rules[i].Priority < rules[j].Priority
		}

		if rules[i].Type != rules[j].Type {
			return rules[i].Type < rules[j].Type
		}

		return rules[i].ID.String() < rules[j].ID.String()
	})
}

// SortTransactions sorts transactions deterministically by date, amount, currency, and ID.
// Dates are normalized to UTC days via DayUTC.
func SortTransactions(txs []CandidateTransaction) {
	sort.SliceStable(txs, func(i, j int) bool {
		di := DayUTC(txs[i].Date)
		dj := DayUTC(txs[j].Date)

		if !di.Equal(dj) {
			return di.Before(dj)
		}

		if !txs[i].Amount.Equal(txs[j].Amount) {
			return txs[i].Amount.LessThan(txs[j].Amount)
		}

		if txs[i].Currency != txs[j].Currency {
			return txs[i].Currency < txs[j].Currency
		}

		if txs[i].AmountBase != nil && txs[j].AmountBase != nil {
			if !txs[i].AmountBase.Equal(*txs[j].AmountBase) {
				return txs[i].AmountBase.LessThan(*txs[j].AmountBase)
			}
		} else if (txs[i].AmountBase == nil) != (txs[j].AmountBase == nil) {
			return txs[i].AmountBase == nil
		}

		if txs[i].CurrencyBase != txs[j].CurrencyBase {
			return txs[i].CurrencyBase < txs[j].CurrencyBase
		}

		if txs[i].Reference != txs[j].Reference {
			return txs[i].Reference < txs[j].Reference
		}

		return txs[i].ID.String() < txs[j].ID.String()
	})
}
