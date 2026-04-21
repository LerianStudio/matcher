package chanutil

// ClosedSignalChannel reports whether a close-only lifecycle channel is nil or
// closed without blocking.
//
// CONTRACT: callers must never send values on ch. This helper is only valid for
// stop/done-style channels that communicate solely through close(ch).
func ClosedSignalChannel(ch <-chan struct{}) bool {
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
