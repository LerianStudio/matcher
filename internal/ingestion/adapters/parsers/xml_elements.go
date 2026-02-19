package parsers

import (
	"slices"
	"strings"
)

// xmlRecordElements lists the element names recognized as transaction records
// in XML files. Both the parser and preview service share this list to ensure
// consistent behavior: if preview shows data, the parser will parse it.
var xmlRecordElements = []string{
	"transaction",
	"row",
	"record",
	"item",
	"entry",
}

// IsXMLRecordElement checks whether the given element name represents a
// transaction record in XML files. The comparison is case-insensitive.
// This function is the single source of truth used by both the XML parser
// and the file preview service, preventing silent data loss from mismatched
// element recognition.
func IsXMLRecordElement(name string) bool {
	lower := strings.ToLower(name)
	return slices.Contains(xmlRecordElements, lower)
}
