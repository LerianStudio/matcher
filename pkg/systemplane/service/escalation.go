// Copyright 2025 Lerian Studio.

// Package service orchestrates systemplane runtime operations.
package service

import (
	"fmt"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/LerianStudio/matcher/pkg/systemplane/registry"
)

// Escalate determines the strongest apply behavior required for a batch of
// write operations. It returns the escalated behavior, the list of keys that
// drove the escalation, and any error.
//
// If any key in the batch has ApplyBootstrapOnly or Mutable=false, the entire
// batch is rejected with ErrKeyNotMutable. Unknown keys are rejected with
// ErrKeyUnknown.
//
// An empty batch is a no-op and returns ApplyLiveRead with no keys.
func Escalate(reg registry.Registry, ops []ports.WriteOp) (domain.ApplyBehavior, []string, error) {
	if len(ops) == 0 {
		return domain.ApplyLiveRead, nil, nil
	}

	var (
		strongest     domain.ApplyBehavior
		strongestKeys []string
		maxStrength   int
		seen          = make(map[string]struct{}, len(ops))
	)

	for _, op := range ops {
		if _, exists := seen[op.Key]; exists {
			return "", nil, fmt.Errorf("duplicate key %q in batch: %w", op.Key, domain.ErrValueInvalid)
		}

		seen[op.Key] = struct{}{}

		def, ok := reg.Get(op.Key)
		if !ok {
			return "", nil, fmt.Errorf("key %q: %w", op.Key, domain.ErrKeyUnknown)
		}

		if def.ApplyBehavior == domain.ApplyBootstrapOnly {
			return "", nil, fmt.Errorf("key %q: %w", op.Key, domain.ErrKeyNotMutable)
		}

		if !def.MutableAtRuntime {
			return "", nil, fmt.Errorf("key %q: %w", op.Key, domain.ErrKeyNotMutable)
		}

		strength := def.ApplyBehavior.Strength()
		if strength > maxStrength {
			maxStrength = strength
			strongest = def.ApplyBehavior
			strongestKeys = []string{op.Key}
		} else if strength == maxStrength {
			strongestKeys = append(strongestKeys, op.Key)
		}
	}

	return strongest, strongestKeys, nil
}
