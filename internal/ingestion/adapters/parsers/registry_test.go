// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package parsers

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

type stubParser struct {
	format string
}

func (s stubParser) Parse(
	_ context.Context,
	_ io.Reader,
	_ *entities.IngestionJob,
	_ *shared.FieldMap,
) (*ports.ParseResult, error) {
	return nil, nil
}

func (s stubParser) SupportedFormat() string {
	return s.format
}

func TestRegistryRegisterAndGet(t *testing.T) {
	t.Parallel()

	reg := NewParserRegistry()
	parser := stubParser{format: "CSV"}

	reg.Register(parser)

	resolved, err := reg.GetParser("csv")
	require.NoError(t, err)
	require.Equal(t, "CSV", resolved.SupportedFormat())
}

func TestRegistryRegisterIgnoresInvalid(t *testing.T) {
	t.Parallel()

	var reg *Registry

	require.NotPanics(t, func() {
		reg.Register(stubParser{format: "csv"})
	})

	reg = NewParserRegistry()
	reg.Register(stubParser{format: ""})
	_, err := reg.GetParser("csv")
	require.Error(t, err)
}

func TestRegistryGetParserErrors(t *testing.T) {
	t.Parallel()

	var reg *Registry

	_, err := reg.GetParser("csv")
	require.Error(t, err)

	reg = NewParserRegistry()
	_, err = reg.GetParser("csv")
	require.Error(t, err)
}

func TestParserSupportedFormats(t *testing.T) {
	t.Parallel()

	require.Equal(t, "csv", NewCSVParser().SupportedFormat())
	require.Equal(t, "json", NewJSONParser().SupportedFormat())
	require.Equal(t, "xml", NewXMLParser().SupportedFormat())
}
