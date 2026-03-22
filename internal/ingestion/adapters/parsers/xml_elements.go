package parsers

import (
	valueObjects "github.com/LerianStudio/matcher/internal/ingestion/domain/value_objects"
)

// IsXMLRecordElement checks whether the given element name represents a
// transaction record in XML files. The comparison is case-insensitive.
// This function is the single source of truth used by both the XML parser
// and the file preview service, preventing silent data loss from mismatched
// element recognition.
func IsXMLRecordElement(name string) bool {
	return valueObjects.IsXMLRecordElement(name)
}
