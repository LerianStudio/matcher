package ports

import (
	"context"
	"time"

	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
)

// FXSource is optional for this task.
// Only implement/wire it if matching must fetch rates at runtime.
type FXSource interface {
	GetRate(
		ctx context.Context,
		fromCurrency, toCurrency string,
		effectiveAt time.Time,
	) (matchingVO.FXRate, error)
}
