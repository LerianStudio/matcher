// Package exception_creator provides postgres adapters for exceptions.
package exception_creator

import "errors"

var (
	// ErrRepoNotInitialized is returned when repository is missing dependencies.
	ErrRepoNotInitialized = errors.New("exception creator repository not initialized")
	// ErrInvalidTx is returned when an invalid transaction is provided.
	ErrInvalidTx = errors.New("exception creator repository invalid transaction")
)
