// Copyright 2025 Lerian Studio.

// Package registry provides key definition registration and validation.
package registry

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
)

// Registry manages the set of known configuration key definitions.
// It is typically populated once at startup via Register/MustRegister calls.
type Registry interface {
	// Register adds a key definition to the registry.
	// Returns an error if the key is already registered or the definition is invalid.
	Register(def domain.KeyDef) error

	// MustRegister adds a key definition and panics on error.
	// Intended for use in init() or startup wiring only.
	MustRegister(def domain.KeyDef)

	// Get retrieves a key definition by key name.
	Get(key string) (domain.KeyDef, bool)

	// List returns all key definitions filtered by kind.
	List(kind domain.Kind) []domain.KeyDef

	// Validate checks that a value is valid for a given key.
	// Checks existence, value type compatibility, and custom validator.
	Validate(key string, value any) error
}

// New returns a new in-memory Registry implementation.
func New() Registry {
	return &inMemoryRegistry{defs: make(map[string]domain.KeyDef)}
}

type inMemoryRegistry struct {
	mu   sync.RWMutex
	defs map[string]domain.KeyDef
}

var errKeyAlreadyRegistered = errors.New("already registered")

// Register adds a key definition to the registry after validating it. It
// returns an error if the definition is invalid or if a key with the same
// name is already registered.
func (registry *inMemoryRegistry) Register(def domain.KeyDef) error {
	if err := def.Validate(); err != nil {
		return fmt.Errorf("register key %q: %w", def.Key, err)
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()

	if _, exists := registry.defs[def.Key]; exists {
		return fmt.Errorf("register key %q: %w", def.Key, errKeyAlreadyRegistered)
	}

	registry.defs[def.Key] = def

	return nil
}

// MustRegister adds a key definition and panics on error. It is intended for
// use in init() functions or startup wiring where registration failure is
// unrecoverable.
func (registry *inMemoryRegistry) MustRegister(def domain.KeyDef) {
	if err := registry.Register(def); err != nil {
		//nolint:forbidigo // MustRegister intentionally fails fast during startup wiring.
		panic(fmt.Sprintf("must register: %v", err))
	}
}

// Get retrieves a key definition by its unique key name. The second return
// value indicates whether the key was found.
func (registry *inMemoryRegistry) Get(key string) (domain.KeyDef, bool) {
	registry.mu.RLock()
	defer registry.mu.RUnlock()

	def, ok := registry.defs[key]

	return def, ok
}

// List returns all registered key definitions whose Kind matches the supplied
// filter. Results are sorted by key name for deterministic output.
func (registry *inMemoryRegistry) List(kind domain.Kind) []domain.KeyDef {
	registry.mu.RLock()
	defer registry.mu.RUnlock()

	result := make([]domain.KeyDef, 0, len(registry.defs))

	for _, def := range registry.defs {
		if def.Kind == kind {
			result = append(result, def)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})

	return result
}

// Validate checks that a value is valid for the given key. It verifies
// that the key is registered, then delegates to type and custom validation.
// A nil value (reset to default) is always considered valid.
func (registry *inMemoryRegistry) Validate(key string, value any) error {
	def, ok := registry.Get(key)
	if !ok {
		return fmt.Errorf("key %q: %w", key, domain.ErrKeyUnknown)
	}

	// nil means "reset to default", always valid.
	if value == nil {
		return nil
	}

	return validateValue(def, value)
}
