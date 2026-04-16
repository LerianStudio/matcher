package command

import (
	"context"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// buildExtractionJobInput assembles the Fetcher extraction request from the
// connection metadata, requested tables, and extraction parameters.
// MappedFields: configName -> table -> columns. Filters: configName -> table -> filter.
func buildExtractionJobInput(
	conn *entities.FetcherConnection,
	tables map[string]any,
	params sharedPorts.ExtractionParams,
) (sharedPorts.ExtractionJobInput, error) {
	configName := conn.ConfigName
	if configName == "" {
		configName = conn.FetcherConnID // fallback
	}

	tableMap := make(map[string][]string, len(tables))

	for name, cfg := range tables {
		columns, colErr := extractRequestedColumns(cfg)
		if colErr != nil {
			return sharedPorts.ExtractionJobInput{}, colErr
		}

		tableMap[name] = columns
	}

	mappedFields := map[string]map[string][]string{
		configName: tableMap,
	}

	// Build Filters (if any): configName -> table -> field -> condition.
	// Each column maps to a typed filter condition matching Fetcher's
	// FilterCondition contract (operator -> []values). The port-level type
	// is still map[string]any for transport generality; values are built
	// to match the fetcherFilterCondition shape: {"eq": ["val"], ...}.
	var filters map[string]map[string]map[string]any

	if params.Filters != nil {
		fieldConditions := buildTypedFilterConditions(params.Filters)
		if len(fieldConditions) > 0 {
			tableFilters := make(map[string]map[string]any, len(tables))
			for name := range tables {
				tableFilters[name] = fieldConditions
			}

			filters = map[string]map[string]map[string]any{
				configName: tableFilters,
			}
		}
	}

	// Build Metadata with required "source" key.
	// Use ConfigName (user-assigned unique identifier, e.g. "prod-db") rather than
	// ProductName (human label, e.g. "PostgreSQL 16.2") to ensure unique provenance
	// when multiple connections share the same engine type.
	metadata := map[string]any{
		"source": configName,
	}

	return sharedPorts.ExtractionJobInput{
		MappedFields: mappedFields,
		Filters:      filters,
		Metadata:     metadata,
	}, nil
}

// buildTypedFilterConditions converts ExtractionFilters into the per-field
// condition map matching Fetcher's FilterCondition contract.
// Each column in ExtractionFilters.Equals is mapped to {"eq": [value]},
// which mirrors fetcherFilterCondition{Eq: []any{value}}.
// The output is map[string]any so it fits the port-level ExtractionJobInput.Filters leaf.
func buildTypedFilterConditions(filters *sharedPorts.ExtractionFilters) map[string]any {
	if filters == nil {
		return nil
	}

	result := make(map[string]any, len(filters.Equals))

	for column, value := range filters.Equals {
		result[column] = map[string]any{
			"eq": []any{value},
		}
	}

	if len(result) == 0 {
		return nil
	}

	return result
}

// extractSubmittedColumns reconstructs a table→columns map from the entity's
// Tables field ({"table": {"columns": ["id","amount"]}}) for comparison against
// Fetcher's echo MappedFields. Returns nil on empty/unparseable input.
func extractSubmittedColumns(tables map[string]any) map[string][]string {
	if len(tables) == 0 {
		return nil
	}

	result := make(map[string][]string, len(tables))

	for name, cfg := range tables {
		cols, err := extractRequestedColumns(cfg)
		if err != nil {
			continue // skip unparseable entries
		}

		result[name] = cols
	}

	if len(result) == 0 {
		return nil
	}

	return result
}

// logMappedFieldsDivergence compares submitted table→columns against the echo
// values returned by Fetcher's job status response. The submitted map is the
// inner table level (no configName wrapper); the returned map is the full
// MappedFields (configName → table → columns). Comparison flattens the returned
// map to its inner tables for matching.
//
// Divergence is diagnostic only (DEBUG level) — Fetcher may auto-qualify table
// names (e.g. "txns" → "public.txns") which changes the keys without error.
func logMappedFieldsDivergence(
	ctx context.Context,
	submittedTables map[string][]string,
	returned map[string]map[string][]string,
	fetcherJobID string,
) bool {
	if len(returned) == 0 || len(submittedTables) == 0 {
		return false // no data to compare
	}

	returnedTables := flattenReturnedTableColumns(returned)

	if tableColumnsEqual(submittedTables, returnedTables) {
		return false
	}

	logger, _, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	logger.With(
		libLog.String("fetcher.job_id", fetcherJobID),
		libLog.Int("submitted_table_count", len(submittedTables)),
		libLog.Int("returned_table_count", len(returnedTables)),
	).Log(ctx, libLog.LevelDebug,
		"extraction job mapped fields divergence: Fetcher response differs from submitted request")

	return true
}

func flattenReturnedTableColumns(returned map[string]map[string][]string) map[string][]string {
	for _, tables := range returned {
		return tables
	}

	return nil
}

// tableColumnsEqual does a deep equality check on table→columns maps.
func tableColumnsEqual(left, right map[string][]string) bool {
	if len(left) != len(right) {
		return false
	}

	for table, leftCols := range left {
		rightCols, ok := right[table]
		if !ok || len(leftCols) != len(rightCols) {
			return false
		}

		for i, col := range leftCols {
			if col != rightCols[i] {
				return false
			}
		}
	}

	return true
}
