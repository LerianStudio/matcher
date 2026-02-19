package ports

import "github.com/LerianStudio/lib-uncommons/v2/uncommons"

// Dispatcher runs background tasks (e.g., outbox processing).
// Implementations should block until stopped.
type Dispatcher interface {
	Run(launcher *uncommons.Launcher) error
	Stop()
}
