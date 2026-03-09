package ports

import libCommons "github.com/LerianStudio/lib-commons/v4/commons"

// Dispatcher runs background tasks (e.g., outbox processing).
// Implementations should block until stopped.
type Dispatcher interface {
	Run(launcher *libCommons.Launcher) error
	Stop()
}
