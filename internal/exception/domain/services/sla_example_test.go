// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package services

import (
	"fmt"
	"time"

	"github.com/shopspring/decimal"
)

// ExampleDefaultSLARules demonstrates how to obtain the PRD-defined SLA rules
// and inspect their severity levels and due durations.
func ExampleDefaultSLARules() {
	rules := DefaultSLARules()

	for _, rule := range rules {
		fmt.Printf("%-8s due_in=%3dh", rule.Name, int(rule.DueIn.Hours()))

		if rule.MinAmountAbsBase != nil {
			fmt.Printf("  min_amount=%s", rule.MinAmountAbsBase.String())
		}

		if rule.MinAgeHours != nil {
			fmt.Printf("  min_age=%dh", *rule.MinAgeHours)
		}

		fmt.Println()
	}

	// Output:
	// CRITICAL due_in= 24h  min_amount=100000  min_age=120h
	// HIGH     due_in= 72h  min_amount=10000  min_age=72h
	// MEDIUM   due_in=120h  min_amount=1000  min_age=24h
	// LOW      due_in=168h
}

// ExampleComputeSLADueAt_severityClassification shows how different amounts
// and ages map to SLA severity levels using OR logic.
//
// Key insight: a rule matches if EITHER amount OR age meets the threshold.
// A $500 exception (normally LOW) becomes CRITICAL if it has been open for
// 120+ hours, because stale exceptions indicate process failures.
func ExampleComputeSLADueAt_severityClassification() {
	rules := DefaultSLARules()
	reference := time.Date(2026, 1, 15, 9, 0, 0, 0, time.UTC)

	cases := []struct {
		label  string
		amount int64
		age    int
	}{
		{"Large amount, fresh", 200000, 1},
		{"Small amount, very old", 500, 120},
		{"Medium amount, fresh", 15000, 1},
		{"Small amount, fresh", 100, 1},
	}

	for _, c := range cases {
		input := SLAInput{
			AmountAbsBase: decimal.NewFromInt(c.amount),
			AgeHours:      c.age,
			ReferenceTime: reference,
		}

		result, err := ComputeSLADueAt(input, rules)
		if err != nil {
			fmt.Printf("%-30s -> error: %v\n", c.label, err)
			continue
		}

		fmt.Printf("%-30s -> %s\n", c.label, result.RuleName)
	}

	// Output:
	// Large amount, fresh            -> CRITICAL
	// Small amount, very old         -> CRITICAL
	// Medium amount, fresh           -> HIGH
	// Small amount, fresh            -> LOW
}

// ExampleComputeSLADueAt_dueDateCalculation shows how the due date is
// computed by adding the matched rule's DueIn duration to the reference time.
func ExampleComputeSLADueAt_dueDateCalculation() {
	rules := DefaultSLARules()

	// An exception created on January 15 at 09:00 UTC with a $50,000
	// amount will match HIGH (due in 72 hours).
	reference := time.Date(2026, 1, 15, 9, 0, 0, 0, time.UTC)
	input := SLAInput{
		AmountAbsBase: decimal.NewFromInt(50000),
		AgeHours:      1,
		ReferenceTime: reference,
	}

	result, err := ComputeSLADueAt(input, rules)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		return
	}

	fmt.Printf("Severity:  %s\n", result.RuleName)
	fmt.Printf("Reference: %s\n", reference.Format(time.RFC3339))
	fmt.Printf("Due at:    %s\n", result.DueAt.Format(time.RFC3339))
	fmt.Printf("Hours:     %.0f\n", result.DueAt.Sub(reference).Hours())

	// Output:
	// Severity:  HIGH
	// Reference: 2026-01-15T09:00:00Z
	// Due at:    2026-01-18T09:00:00Z
	// Hours:     72
}
