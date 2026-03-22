// Package parsers provides file format parsers for ingestion.
package parsers

import (
	"io"

	valueObjects "github.com/LerianStudio/matcher/internal/ingestion/domain/value_objects"
)

// StripBOM reads the first 3 bytes from reader and checks for a UTF-8 BOM.
// If a BOM is found, it returns a reader positioned after the BOM.
// If no BOM is found, it returns a reader that replays the consumed bytes
// followed by the remaining content, so nothing is lost.
func StripBOM(reader io.Reader) (io.Reader, error) {
	return valueObjects.StripUTF8BOM(reader)
}
