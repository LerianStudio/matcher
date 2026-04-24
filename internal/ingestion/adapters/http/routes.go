// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package http

import (
	"errors"

	"github.com/gofiber/fiber/v2"

	"github.com/LerianStudio/matcher/internal/auth"
)

var (
	// ErrProtectedRouteHelperRequired indicates protected route helper is nil.
	ErrProtectedRouteHelperRequired = errors.New("protected route helper is required")
	// ErrHandlersRequired indicates handlers are nil.
	ErrHandlersRequired = errors.New("ingestion handlers are required")
)

// RegisterRoutes registers the ingestion HTTP routes.
func RegisterRoutes(
	protected func(resource string, actions ...string) fiber.Router,
	handlers *Handlers,
) error {
	if protected == nil {
		return ErrProtectedRouteHelperRequired
	}

	if handlers == nil {
		return ErrHandlersRequired
	}

	protected(
		auth.ResourceIngestion,
		auth.ActionImportCreate,
	).Post("/v1/imports/contexts/:contextId/sources/:sourceId/upload", handlers.UploadFile)
	protected(
		auth.ResourceIngestion,
		auth.ActionJobRead,
	).Post("/v1/imports/contexts/:contextId/sources/:sourceId/preview", handlers.PreviewFile)
	protected(
		auth.ResourceIngestion,
		auth.ActionJobRead,
	).Get("/v1/imports/contexts/:contextId/jobs", handlers.ListJobsByContext)
	protected(
		auth.ResourceIngestion,
		auth.ActionJobRead,
	).Get("/v1/imports/contexts/:contextId/jobs/:jobId", handlers.GetJob)
	protected(
		auth.ResourceIngestion,
		auth.ActionJobRead,
	).Get("/v1/imports/contexts/:contextId/jobs/:jobId/transactions", handlers.ListTransactionsByJob)
	protected(
		auth.ResourceIngestion,
		auth.ActionTransactionIgnore,
	).Post("/v1/imports/contexts/:contextId/transactions/:transactionId/ignore", handlers.IgnoreTransaction)
	protected(
		auth.ResourceIngestion,
		auth.ActionTransactionSearch,
	).Get("/v1/imports/contexts/:contextId/transactions/search", handlers.SearchTransactions)

	return nil
}
