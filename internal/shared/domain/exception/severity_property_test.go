// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package exception

import (
	"math/rand"
	"testing"
	"testing/quick"

	"github.com/stretchr/testify/require"
)

func TestProperty_Severity_ParseRoundTrip(t *testing.T) {
	t.Parallel()

	cfg := quick.Config{
		MaxCount: 100,
		Rand:     rand.New(rand.NewSource(42)),
	}

	validSeverities := []ExceptionSeverity{
		ExceptionSeverityLow,
		ExceptionSeverityMedium,
		ExceptionSeverityHigh,
		ExceptionSeverityCritical,
	}

	property := func(index uint8) bool {
		sev := validSeverities[int(index)%len(validSeverities)]

		parsed, err := ParseExceptionSeverity(sev.String())
		if err != nil {
			return false
		}

		return parsed == sev
	}

	require.NoError(t, quick.Check(property, &cfg))
}

func TestProperty_Severity_OrderingTransitive(t *testing.T) {
	t.Parallel()

	cfg := quick.Config{
		MaxCount: 100,
		Rand:     rand.New(rand.NewSource(7)),
	}

	severityOrder := map[ExceptionSeverity]int{
		ExceptionSeverityLow:      0,
		ExceptionSeverityMedium:   1,
		ExceptionSeverityHigh:     2,
		ExceptionSeverityCritical: 3,
	}

	validSeverities := []ExceptionSeverity{
		ExceptionSeverityLow,
		ExceptionSeverityMedium,
		ExceptionSeverityHigh,
		ExceptionSeverityCritical,
	}

	property := func(indexA, indexB, indexC uint8) bool {
		a := validSeverities[int(indexA)%len(validSeverities)]
		b := validSeverities[int(indexB)%len(validSeverities)]
		c := validSeverities[int(indexC)%len(validSeverities)]

		orderA := severityOrder[a]
		orderB := severityOrder[b]
		orderC := severityOrder[c]

		if orderA > orderB && orderB > orderC {
			return orderA > orderC
		}

		return true
	}

	require.NoError(t, quick.Check(property, &cfg))
}

func TestProperty_Severity_IsValidConsistent(t *testing.T) {
	t.Parallel()

	cfg := quick.Config{
		MaxCount: 100,
		Rand:     rand.New(rand.NewSource(99)),
	}

	validSeverities := []ExceptionSeverity{
		ExceptionSeverityLow,
		ExceptionSeverityMedium,
		ExceptionSeverityHigh,
		ExceptionSeverityCritical,
	}

	property := func(index uint8) bool {
		sev := validSeverities[int(index)%len(validSeverities)]

		first := sev.IsValid()
		second := sev.IsValid()
		third := sev.IsValid()

		return first == second && second == third && first
	}

	require.NoError(t, quick.Check(property, &cfg))
}
