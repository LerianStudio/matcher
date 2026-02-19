// Package parsers provides file format parsers for ingestion.
package parsers

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

// utf8BOM contains the byte sequence for a UTF-8 Byte Order Mark.
// Files exported from Excel on Windows commonly include this prefix,
// which can cause header mismatches if not stripped (e.g., "\xEF\xBB\xBFid" vs "id").
var utf8BOM = []byte{0xEF, 0xBB, 0xBF}

// StripBOM reads the first 3 bytes from reader and checks for a UTF-8 BOM.
// If a BOM is found, it returns a reader positioned after the BOM.
// If no BOM is found, it returns a reader that replays the consumed bytes
// followed by the remaining content, so nothing is lost.
func StripBOM(reader io.Reader) (io.Reader, error) {
	buf := make([]byte, len(utf8BOM))

	bytesRead, err := io.ReadFull(reader, buf)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return reader, fmt.Errorf("reading BOM prefix: %w", err)
	}

	// Check for UTF-8 BOM
	if bytesRead >= len(utf8BOM) && bytes.Equal(buf[:len(utf8BOM)], utf8BOM) {
		return reader, nil // BOM found and consumed
	}

	// No BOM found; put back the bytes we read
	return io.MultiReader(bytes.NewReader(buf[:bytesRead]), reader), nil
}
