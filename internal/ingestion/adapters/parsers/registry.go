// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package parsers

import (
	"fmt"
	"strings"

	"github.com/LerianStudio/matcher/internal/ingestion/ports"
)

// Registry manages available parsers for different file formats.
type Registry struct {
	parsers map[string]ports.Parser
}

// NewParserRegistry creates a new parser registry.
func NewParserRegistry() *Registry {
	return &Registry{parsers: make(map[string]ports.Parser)}
}

// Register adds a parser to the registry.
func (registry *Registry) Register(parser ports.Parser) {
	if registry == nil || parser == nil {
		return
	}

	format := strings.ToLower(strings.TrimSpace(parser.SupportedFormat()))
	if format == "" {
		return
	}

	registry.parsers[format] = parser
}

// GetParser returns a parser for the given format.
func (registry *Registry) GetParser(format string) (ports.Parser, error) {
	if registry == nil {
		return nil, errRegistryNotInitialized
	}

	key := strings.ToLower(strings.TrimSpace(format))

	parser, ok := registry.parsers[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnsupportedFormat, format)
	}

	return parser, nil
}
