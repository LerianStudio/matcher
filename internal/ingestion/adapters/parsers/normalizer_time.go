// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package parsers

import (
	"fmt"
	"strings"
	"time"

	"github.com/LerianStudio/matcher/internal/ingestion/ports"
)

const (
	unixTimestampSecondsLen = 10
	unixTimestampMillisLen  = 13
	base10                  = 10
)

// dateLayouts contains supported date formats in order of parsing priority.
// Formats are ordered from most specific to least specific to avoid ambiguity.
// Note: Ambiguous formats like MM/DD/YYYY and DD/MM/YYYY are intentionally excluded
// as they cannot be reliably distinguished without explicit configuration.
var dateLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05",
	"2006-01-02",
	"2006/01/02 15:04:05",
	"2006/01/02",
	"20060102150405",
	"20060102",
	"02-Jan-2006 15:04:05",
	"02-Jan-2006",
	"Jan 2, 2006 15:04:05",
	"Jan 2, 2006",
	"January 2, 2006 15:04:05",
	"January 2, 2006",
	"2 Jan 2006 15:04:05",
	"2 Jan 2006",
}

func parseTime(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, errDateEmpty
	}

	for _, layout := range dateLayouts {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed.UTC(), nil
		}
	}

	if parsed, ok := parseUnixTimestamp(value); ok {
		return parsed, nil
	}

	return time.Time{}, fmt.Errorf("%w: %s", errUnsupportedDateFormat, value)
}

// parseUnixTimestamp attempts to parse a string as a Unix timestamp.
// Supports exactly 10 digits (seconds) or 13 digits (milliseconds).
func parseUnixTimestamp(value string) (time.Time, bool) {
	if len(value) != unixTimestampSecondsLen && len(value) != unixTimestampMillisLen {
		return time.Time{}, false
	}

	for _, c := range value {
		if c < '0' || c > '9' {
			return time.Time{}, false
		}
	}

	var timestamp int64
	for _, c := range value {
		timestamp = timestamp*base10 + int64(c-'0')
	}

	if len(value) == unixTimestampSecondsLen {
		return time.Unix(timestamp, 0).UTC(), true
	}

	return time.UnixMilli(timestamp).UTC(), true
}

func updateDateRange(dateRange *ports.DateRange, date time.Time) *ports.DateRange {
	if dateRange == nil {
		return &ports.DateRange{Start: date, End: date}
	}

	if date.Before(dateRange.Start) {
		dateRange.Start = date
	}

	if date.After(dateRange.End) {
		dateRange.End = date
	}

	return dateRange
}
