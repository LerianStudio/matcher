// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package command

import (
	"fmt"
	"strings"
	"time"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

const extractionDateLayout = "2006-01-02"

func validateExtractionRequest(tables map[string]any, params sharedPorts.ExtractionParams) error {
	if len(tables) == 0 {
		return fmt.Errorf("%w: at least one table is required", ErrInvalidExtractionRequest)
	}

	startDate, err := parseExtractionDate("start date", params.StartDate)
	if err != nil {
		return err
	}

	endDate, err := parseExtractionDate("end date", params.EndDate)
	if err != nil {
		return err
	}

	if !startDate.IsZero() && !endDate.IsZero() && endDate.Before(startDate) {
		return fmt.Errorf("%w: end date must be on or after start date", ErrInvalidExtractionRequest)
	}

	for tableName, cfg := range tables {
		if strings.TrimSpace(tableName) == "" {
			return fmt.Errorf("%w: table name is required", ErrInvalidExtractionRequest)
		}

		if _, err := extractRequestedColumns(cfg); err != nil {
			return err
		}
	}

	return nil
}

func parseExtractionDate(label, raw string) (time.Time, error) {
	if strings.TrimSpace(raw) == "" {
		return time.Time{}, nil
	}

	parsed, err := time.Parse(extractionDateLayout, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: %s must use YYYY-MM-DD format", ErrInvalidExtractionRequest, label)
	}

	return parsed, nil
}

func validateExtractionScope(tables map[string]any, schemas []*entities.DiscoveredSchema) error {
	if len(schemas) == 0 {
		return fmt.Errorf("%w: schema has not been discovered for this connection", ErrInvalidExtractionRequest)
	}

	allowedTables := make(map[string]map[string]struct{}, len(schemas))
	for _, schema := range schemas {
		if schema == nil || strings.TrimSpace(schema.TableName) == "" {
			continue
		}

		columns := make(map[string]struct{}, len(schema.Columns))
		for _, column := range schema.Columns {
			if strings.TrimSpace(column.Name) == "" {
				continue
			}

			columns[column.Name] = struct{}{}
		}

		allowedTables[schema.TableName] = columns
	}

	for tableName, cfg := range tables {
		allowedColumns, ok := allowedTables[tableName]
		if !ok {
			return fmt.Errorf("%w: unknown table %q", ErrInvalidExtractionRequest, tableName)
		}

		columns, err := extractRequestedColumns(cfg)
		if err != nil {
			return err
		}

		for _, column := range columns {
			if _, exists := allowedColumns[column]; !exists {
				return fmt.Errorf("%w: unknown column %q for table %q", ErrInvalidExtractionRequest, column, tableName)
			}
		}
	}

	return nil
}

func extractRequestedColumns(cfg any) ([]string, error) {
	if cfg == nil {
		return nil, nil
	}

	cfgMap, ok := cfg.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: table configuration must be an object", ErrInvalidExtractionRequest)
	}

	for key := range cfgMap {
		if key != "columns" {
			return nil, fmt.Errorf("%w: unsupported table configuration key %q", ErrInvalidExtractionRequest, key)
		}
	}

	cols, ok := cfgMap["columns"]
	if !ok {
		return nil, nil
	}

	switch typed := cols.(type) {
	case []string:
		return validateRequestedColumns(typed)
	case []any:
		stringCols := make([]string, 0, len(typed))
		for _, raw := range typed {
			colName, isString := raw.(string)
			if !isString {
				return nil, fmt.Errorf("%w: columns must be strings", ErrInvalidExtractionRequest)
			}

			stringCols = append(stringCols, colName)
		}

		return validateRequestedColumns(stringCols)
	default:
		return nil, fmt.Errorf("%w: columns must be an array of strings", ErrInvalidExtractionRequest)
	}
}

func validateRequestedColumns(columns []string) ([]string, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("%w: columns must not be empty", ErrInvalidExtractionRequest)
	}

	normalized := make([]string, 0, len(columns))

	seen := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		trimmed := strings.TrimSpace(column)
		if trimmed == "" {
			return nil, fmt.Errorf("%w: columns must not contain blanks", ErrInvalidExtractionRequest)
		}

		if _, exists := seen[trimmed]; exists {
			continue
		}

		seen[trimmed] = struct{}{}
		normalized = append(normalized, trimmed)
	}

	return normalized, nil
}
