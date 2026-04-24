// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package parsers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsXMLRecordElement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		// Recognized elements — lowercase
		{name: "transaction lowercase", input: "transaction", expected: true},
		{name: "row lowercase", input: "row", expected: true},
		{name: "record lowercase", input: "record", expected: true},
		{name: "item lowercase", input: "item", expected: true},
		{name: "entry lowercase", input: "entry", expected: true},

		// Recognized elements — uppercase
		{name: "TRANSACTION uppercase", input: "TRANSACTION", expected: true},
		{name: "ROW uppercase", input: "ROW", expected: true},
		{name: "RECORD uppercase", input: "RECORD", expected: true},
		{name: "ITEM uppercase", input: "ITEM", expected: true},
		{name: "ENTRY uppercase", input: "ENTRY", expected: true},

		// Recognized elements — mixed case
		{name: "Transaction title case", input: "Transaction", expected: true},
		{name: "Row title case", input: "Row", expected: true},
		{name: "Record title case", input: "Record", expected: true},
		{name: "Item title case", input: "Item", expected: true},
		{name: "Entry title case", input: "Entry", expected: true},
		{name: "tRaNsAcTiOn mixed case", input: "tRaNsAcTiOn", expected: true},

		// Not recognized
		{name: "empty string", input: "", expected: false},
		{name: "data element", input: "data", expected: false},
		{name: "transactions plural", input: "transactions", expected: false},
		{name: "rows plural", input: "rows", expected: false},
		{name: "records plural", input: "records", expected: false},
		{name: "items plural", input: "items", expected: false},
		{name: "entries plural", input: "entries", expected: false},
		{name: "payment unrecognized", input: "payment", expected: false},
		{name: "root element", input: "root", expected: false},
		{name: "whitespace", input: " transaction ", expected: false},
		{name: "partial match prefix", input: "transact", expected: false},
		{name: "partial match suffix", input: "action", expected: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := IsXMLRecordElement(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}
