// Package ports defines external dependency abstractions for the discovery context.
//
// The discovery context currently exposes repository interfaces via
// domain/repositories/ following the hexagonal architecture pattern. This ports
// package provides a home for future non-repository ports (Fetcher API clients,
// event publishers, cache providers, etc.) and signals architectural intent
// consistent with other bounded contexts.
//
// Cross-context ports live in internal/shared/ports/.
package ports
