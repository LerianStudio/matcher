// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package value_objects

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
)

var xmlRecordElements = []string{
	"transaction",
	"row",
	"record",
	"item",
	"entry",
}

var utf8BOM = []byte{0xEF, 0xBB, 0xBF}

// IsXMLRecordElement checks if a given XML element represents a transaction record.
func IsXMLRecordElement(name string) bool {
	return slices.Contains(xmlRecordElements, strings.ToLower(name))
}

// StripUTF8BOM strips a UTF-8 BOM prefix from a reader when present.
func StripUTF8BOM(reader io.Reader) (io.Reader, error) {
	buf := make([]byte, len(utf8BOM))

	bytesRead, err := io.ReadFull(reader, buf)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return reader, fmt.Errorf("reading BOM prefix: %w", err)
	}

	if bytesRead >= len(utf8BOM) && bytes.Equal(buf[:len(utf8BOM)], utf8BOM) {
		return reader, nil
	}

	return io.MultiReader(bytes.NewReader(buf[:bytesRead]), reader), nil
}
