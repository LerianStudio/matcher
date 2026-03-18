// Copyright 2025 Lerian Studio.

package changefeed

import (
	"errors"
	"fmt"

	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

// ErrHandlerPanic indicates the downstream handler panicked during signal processing.
var ErrHandlerPanic = errors.New("changefeed: handler panic")

// SafeInvokeHandler calls handler and converts panics into errors so feed
// implementations can fail safely instead of crashing the process.
func SafeInvokeHandler(handler func(ports.ChangeSignal), signal ports.ChangeSignal) (err error) {
	if handler == nil {
		return ErrNilHandler
	}

	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("%w: %v", ErrHandlerPanic, recovered)
		}
	}()

	handler(signal)

	return nil
}
