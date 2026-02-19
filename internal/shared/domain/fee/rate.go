package fee

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Rate represents a fee rate configuration with its calculation structure.
type Rate struct {
	ID        uuid.UUID
	Currency  string
	Structure FeeStructure
	CreatedAt time.Time
	UpdatedAt time.Time
}

// NewRateInput contains parameters for creating a new Rate.
type NewRateInput struct {
	ID        uuid.UUID
	Currency  string
	Structure FeeStructure
}

// NewRate creates and validates a new Rate from the given input.
func NewRate(ctx context.Context, in NewRateInput) (*Rate, error) {
	if in.Structure == nil {
		return nil, fmt.Errorf("rate structure: %w", ErrNilFeeStructure)
	}

	currency, err := NormalizeCurrency(in.Currency)
	if err != nil {
		return nil, fmt.Errorf("normalize currency %q: %w", in.Currency, err)
	}

	if err := in.Structure.Validate(ctx); err != nil {
		return nil, fmt.Errorf("validate fee structure: %w", err)
	}

	id := in.ID
	if id == uuid.Nil {
		id = uuid.New()
	}

	now := time.Now().UTC()

	return &Rate{
		ID:        id,
		Currency:  currency,
		Structure: in.Structure,
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}
