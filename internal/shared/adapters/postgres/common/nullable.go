// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package common provides shared utilities for postgres adapters.
package common

import (
	"database/sql"
	"time"
)

// StringToNullString converts a string value to sql.NullString.
// Empty strings are treated as SQL NULL (Valid=false), meaning this function
// does not distinguish between "not set" and "explicitly empty". Callers who
// need to preserve empty strings as valid, non-NULL values should use
// [StringPtrToNullString] with a non-nil pointer instead.
func StringToNullString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}

	return sql.NullString{String: s, Valid: true}
}

// StringPtrToNullString converts a *string to sql.NullString.
// Nil pointers result in invalid (NULL) NullString.
func StringPtrToNullString(s *string) sql.NullString {
	if s == nil {
		return sql.NullString{}
	}

	return sql.NullString{String: *s, Valid: true}
}

// NullStringToStringPtr converts sql.NullString to *string.
// Invalid NullStrings return nil.
func NullStringToStringPtr(ns sql.NullString) *string {
	if !ns.Valid {
		return nil
	}

	return &ns.String
}

// TimePtrToNullTime converts a *time.Time to sql.NullTime.
// Nil pointers result in invalid (NULL) NullTime.
func TimePtrToNullTime(t *time.Time) sql.NullTime {
	if t == nil {
		return sql.NullTime{}
	}

	return sql.NullTime{Time: *t, Valid: true}
}

// NullTimeToTimePtr converts sql.NullTime to *time.Time.
// Invalid NullTimes return nil.
func NullTimeToTimePtr(nt sql.NullTime) *time.Time {
	if !nt.Valid {
		return nil
	}

	return &nt.Time
}
