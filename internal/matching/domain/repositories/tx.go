// Package repositories provides matching persistence abstractions.
package repositories

// Tx represents an opaque transaction handle passed through repository calls.
// Implementations are responsible for asserting and using the concrete type.
type Tx any
