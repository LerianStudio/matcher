// Package repositories provides reporting persistence contracts.
package repositories

import "errors"

// ErrExportJobNotFound is returned when an export job is not found.
var ErrExportJobNotFound = errors.New("export job not found")
