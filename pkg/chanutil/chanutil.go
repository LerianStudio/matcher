// Package chanutil provides utilities for Go channel operations.
package chanutil

// Closed reports whether a struct{} channel has been closed without blocking.
// Returns true for nil channels (a nil channel blocks forever on receive,
// so it is treated as effectively closed for lifecycle-check purposes).
func Closed(ch <-chan struct{}) bool {
	if ch == nil {
		return true
	}

	select {
	case <-ch:
		return true
	default:
		return false
	}
}
