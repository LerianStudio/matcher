package services

import "time"

// hoursPerDay is the number of hours in a day.
const hoursPerDay = 24

// DayUTC returns the given time normalized to midnight (00:00:00) UTC.
func DayUTC(t time.Time) time.Time {
	u := t.UTC()
	return time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC)
}

// SignedDayDiff returns (rightDay - leftDay) in whole days.
func SignedDayDiff(left, right time.Time) int {
	l := DayUTC(left)
	r := DayUTC(right)

	return int(r.Sub(l) / (hoursPerDay * time.Hour))
}

// AbsDayDiff returns the absolute difference in whole days between left and right.
func AbsDayDiff(left, right time.Time) int {
	d := SignedDayDiff(left, right)
	if d < 0 {
		return -d
	}

	return d
}
