// Copyright 2025 Lerian Studio.

package bootstrap

import (
	"fmt"
	"regexp"
	"strings"
)

var postgresIdentifierPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

func validatePostgresIdentifier(kind, value string) error {
	trimmedValue := strings.TrimSpace(value)
	if !postgresIdentifierPattern.MatchString(trimmedValue) {
		return fmt.Errorf("%w %s %q (must match %s)", ErrInvalidPostgresIdentifier, kind, value, postgresIdentifierPattern.String())
	}

	return nil
}

// ValidatePostgresObjectNames validates the schema, table, and channel names
// used by the PostgreSQL systemplane bootstrap configuration.
func ValidatePostgresObjectNames(schema, entriesTable, historyTable, revisionTable, notifyChannel string) error {
	if err := validatePostgresIdentifier("schema", schema); err != nil {
		return err
	}

	if err := validatePostgresIdentifier("entries table", entriesTable); err != nil {
		return err
	}

	if err := validatePostgresIdentifier("history table", historyTable); err != nil {
		return err
	}

	if err := validatePostgresIdentifier("revision table", revisionTable); err != nil {
		return err
	}

	if err := validatePostgresIdentifier("notify channel", notifyChannel); err != nil {
		return err
	}

	return nil
}
