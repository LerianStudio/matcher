// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package services

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDayUTC(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		input    time.Time
		expected time.Time
	}{
		{
			name:     "already UTC",
			input:    time.Date(2024, time.April, 2, 15, 4, 5, 0, time.UTC),
			expected: time.Date(2024, time.April, 2, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "positive offset +0530",
			input: time.Date(
				2024,
				time.March,
				10,
				0,
				30,
				0,
				0,
				time.FixedZone("+0530", 5*60*60+30*60),
			),
			expected: time.Date(2024, time.March, 9, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "exactly at midnight",
			input: time.Date(
				2024,
				time.March,
				10,
				0,
				0,
				0,
				0,
				time.FixedZone("-0300", -3*60*60),
			),
			expected: time.Date(2024, time.March, 10, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "zero time",
			input:    time.Time{},
			expected: time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "negative offset -0300",
			input: time.Date(
				2024,
				time.March,
				10,
				23,
				45,
				10,
				999,
				time.FixedZone("-0300", -3*60*60),
			),
			expected: time.Date(2024, time.March, 11, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			result := DayUTC(testCase.input)

			require.Equal(t, testCase.expected, result)
		})
	}
}

func TestSignedDayDiff(t *testing.T) {
	t.Parallel()

	left := time.Date(2024, time.January, 1, 23, 0, 0, 0, time.UTC)
	right := time.Date(2024, time.January, 3, 1, 0, 0, 0, time.UTC)
	sameDayLeft := time.Date(2024, time.January, 10, 0, 1, 0, 0, time.UTC)
	sameDayRight := time.Date(2024, time.January, 10, 23, 59, 59, 0, time.UTC)
	consecutiveLeft := time.Date(2024, time.January, 15, 12, 0, 0, 0, time.UTC)
	consecutiveRight := time.Date(2024, time.January, 16, 1, 0, 0, 0, time.UTC)
	yearBoundaryLeft := time.Date(2023, time.December, 31, 23, 0, 0, 0, time.UTC)
	yearBoundaryRight := time.Date(2024, time.January, 1, 0, 30, 0, 0, time.UTC)
	leapYearLeft := time.Date(2024, time.February, 28, 12, 0, 0, 0, time.UTC)
	leapYearRight := time.Date(2024, time.February, 29, 12, 0, 0, 0, time.UTC)
	nonLeapLeft := time.Date(2023, time.February, 28, 12, 0, 0, 0, time.UTC)
	nonLeapRight := time.Date(2023, time.March, 1, 12, 0, 0, 0, time.UTC)

	require.Equal(t, 2, SignedDayDiff(left, right))
	require.Equal(t, -2, SignedDayDiff(right, left))
	require.Equal(t, 0, SignedDayDiff(sameDayLeft, sameDayRight))
	require.Equal(t, 1, SignedDayDiff(consecutiveLeft, consecutiveRight))
	require.Equal(t, -1, SignedDayDiff(consecutiveRight, consecutiveLeft))
	require.Equal(t, 1, SignedDayDiff(yearBoundaryLeft, yearBoundaryRight))
	require.Equal(t, -1, SignedDayDiff(yearBoundaryRight, yearBoundaryLeft))
	require.Equal(t, 1, SignedDayDiff(leapYearLeft, leapYearRight))
	require.Equal(t, -1, SignedDayDiff(leapYearRight, leapYearLeft))
	require.Equal(t, 1, SignedDayDiff(nonLeapLeft, nonLeapRight))
	require.Equal(t, -1, SignedDayDiff(nonLeapRight, nonLeapLeft))
}

func TestAbsDayDiff(t *testing.T) {
	t.Parallel()

	left := time.Date(2024, time.January, 5, 10, 0, 0, 0, time.UTC)
	right := time.Date(2024, time.January, 2, 11, 0, 0, 0, time.UTC)
	sameDayLeft := time.Date(2024, time.January, 8, 1, 0, 0, 0, time.UTC)
	sameDayRight := time.Date(2024, time.January, 8, 23, 59, 0, 0, time.UTC)

	require.Equal(t, 3, AbsDayDiff(left, right))
	require.Equal(t, AbsDayDiff(left, right), AbsDayDiff(right, left))
	require.Equal(t, 0, AbsDayDiff(sameDayLeft, sameDayRight))
}
