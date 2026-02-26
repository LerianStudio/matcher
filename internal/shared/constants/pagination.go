// Package constants provides application-wide constant values.
package constants

const (
	// DefaultPaginationLimit is the default number of items returned when clients
	// do not provide a valid positive limit.
	DefaultPaginationLimit = 20
	// MaximumPaginationLimit is the hard cap for list endpoints and repositories.
	MaximumPaginationLimit = 200
)
