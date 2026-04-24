// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package fee

import "testing"

func TestMatchingSide_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		side  MatchingSide
		valid bool
	}{
		{"LEFT is valid", MatchingSideLeft, true},
		{"RIGHT is valid", MatchingSideRight, true},
		{"ANY is valid", MatchingSideAny, true},
		{"empty is invalid", MatchingSide(""), false},
		{"lowercase is invalid", MatchingSide("left"), false},
		{"unknown is invalid", MatchingSide("BOTH"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.side.IsValid(); got != tt.valid {
				t.Errorf("MatchingSide(%q).IsValid() = %v, want %v", tt.side, got, tt.valid)
			}
		})
	}
}

func TestMatchingSide_AppliesToLeft(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		side    MatchingSide
		applies bool
	}{
		{"LEFT applies to left", MatchingSideLeft, true},
		{"RIGHT does not apply to left", MatchingSideRight, false},
		{"ANY applies to left", MatchingSideAny, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.side.AppliesToLeft(); got != tt.applies {
				t.Errorf("MatchingSide(%q).AppliesToLeft() = %v, want %v", tt.side, got, tt.applies)
			}
		})
	}
}

func TestMatchingSide_IsExclusive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		side      MatchingSide
		exclusive bool
	}{
		{"LEFT is exclusive", MatchingSideLeft, true},
		{"RIGHT is exclusive", MatchingSideRight, true},
		{"ANY is not exclusive", MatchingSideAny, false},
		{"empty is not exclusive", MatchingSide(""), false},
		{"INVALID is not exclusive", MatchingSide("INVALID"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.side.IsExclusive(); got != tt.exclusive {
				t.Errorf("MatchingSide(%q).IsExclusive() = %v, want %v", tt.side, got, tt.exclusive)
			}
		})
	}
}

func TestMatchingSide_AppliesToRight(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		side    MatchingSide
		applies bool
	}{
		{"LEFT does not apply to right", MatchingSideLeft, false},
		{"RIGHT applies to right", MatchingSideRight, true},
		{"ANY applies to right", MatchingSideAny, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.side.AppliesToRight(); got != tt.applies {
				t.Errorf("MatchingSide(%q).AppliesToRight() = %v, want %v", tt.side, got, tt.applies)
			}
		})
	}
}
