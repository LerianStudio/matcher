// Package ports defines the external interfaces for the governance context.
//
// The governance context currently exposes repository interfaces via
// domain/repositories/ following its original design. This ports package
// provides a home for future non-repository ports (event publishers,
// external service clients, etc.) and signals architectural intent
// consistent with other bounded contexts.
package ports
