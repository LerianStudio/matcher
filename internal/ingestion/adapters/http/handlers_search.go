// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package http

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel/trace"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/ingestion/adapters/http/dto"
	ingestionRepositories "github.com/LerianStudio/matcher/internal/ingestion/domain/repositories"
	"github.com/LerianStudio/matcher/internal/ingestion/services/command"
)

// IgnoreTransaction handles POST /v1/imports/contexts/:contextId/transactions/:transactionId/ignore
// @Summary Ignore a transaction
// @Description Marks a transaction as "Do Not Match" with a required reason. Only UNMATCHED transactions can be ignored.
// @ID ignoreTransaction
// @Tags Ingestion
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param transactionId path string true "Transaction ID" format(uuid)
// @Param request body dto.IgnoreTransactionRequest true "Ignore transaction request"
// @Success 200 {object} dto.IgnoreTransactionResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Transaction not found"
// @Failure 409 {object} sharedhttp.ErrorResponse "Transaction already matched/ignored or idempotency conflict"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/imports/contexts/{contextId}/transactions/{transactionId}/ignore [post]
func (handler *Handlers) IgnoreTransaction(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.ingestion.ignore_transaction")
	defer span.End()

	contextID, tenantID, err := libHTTP.ParseAndVerifyTenantScopedID(
		fiberCtx,
		"contextId",
		libHTTP.IDLocationParam,
		handler.contextVerifier,
		auth.GetTenantID,
		libHTTP.ErrMissingContextID,
		libHTTP.ErrInvalidContextID,
		libHTTP.ErrContextAccessDenied,
	)
	if err != nil {
		return handler.handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	transactionID, err := uuid.Parse(fiberCtx.Params("transactionId"))
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid transaction_id", err)
	}

	var req dto.IgnoreTransactionRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid request body", err)
	}

	tx, err := handler.commandUC.IgnoreTransaction(ctx, command.IgnoreTransactionInput{
		TransactionID: transactionID,
		ContextID:     contextID,
		Reason:        req.Reason,
	})
	if err != nil {
		return handler.handleIgnoreTransactionError(ctx, fiberCtx, span, logger, err)
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.IgnoreTransactionResponse{
		TransactionResponse: dto.TransactionToResponse(tx, tx.IngestionJobID, contextID),
	}); err != nil {
		return fmt.Errorf("respond ignore transaction: %w", err)
	}

	return nil
}

// SearchTransactions handles GET /v1/imports/contexts/:contextId/transactions/search
// @Summary Search transactions
// @Description Searches transactions within a reconciliation context by text, amount range, date range, currency, source, and status. Returns offset-paginated results with total count.
// @ID searchTransactions
// @Tags Ingestion
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param q query string false "Free text search across external ID and description"
// @Param amount_min query string false "Minimum transaction amount"
// @Param amount_max query string false "Maximum transaction amount"
// @Param date_from query string false "Start date filter (RFC3339)" format(date-time)
// @Param date_to query string false "End date filter (RFC3339)" format(date-time)
// @Param reference query string false "Exact external ID match"
// @Param currency query string false "Currency code filter"
// @Param source_id query string false "Source ID filter"
// @Param status query string false "Transaction status filter" Enums(UNMATCHED,MATCHED,IGNORED,PENDING_REVIEW)
// @Param limit query int false "Maximum number of records to return" default(20) minimum(1) maximum(50)
// @Param offset query int false "Number of records to skip" default(0) minimum(0)
// @Success 200 {object} dto.SearchTransactionsResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/imports/contexts/{contextId}/transactions/search [get]
func (handler *Handlers) SearchTransactions(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.ingestion.search_transactions")
	defer span.End()

	contextID, tenantID, err := libHTTP.ParseAndVerifyTenantScopedID(
		fiberCtx,
		"contextId",
		libHTTP.IDLocationParam,
		handler.contextVerifier,
		auth.GetTenantID,
		libHTTP.ErrMissingContextID,
		libHTTP.ErrInvalidContextID,
		libHTTP.ErrContextAccessDenied,
	)
	if err != nil {
		return handler.handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	var req dto.SearchTransactionsRequest
	if err := fiberCtx.QueryParser(&req); err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid query parameters", err)
	}

	searchParams, err := parseSearchParams(req)
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid search parameters", err)
	}

	transactions, total, err := handler.transactionRepo.SearchTransactions(ctx, contextID, searchParams)
	if err != nil {
		handler.logSpanError(ctx, span, logger, "failed to search transactions", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	items := dto.SearchTransactionsToResponse(transactions, contextID)

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.SearchTransactionsResponse{
		Items:  items,
		Total:  total,
		Limit:  searchParams.Limit,
		Offset: searchParams.Offset,
	}); err != nil {
		return fmt.Errorf("respond search transactions: %w", err)
	}

	return nil
}

//nolint:cyclop // parsing multiple optional search parameters
func parseSearchParams(
	req dto.SearchTransactionsRequest,
) (ingestionRepositories.TransactionSearchParams, error) {
	params := ingestionRepositories.TransactionSearchParams{
		Query:     strings.TrimSpace(req.Query),
		Reference: strings.TrimSpace(req.Reference),
		Currency:  strings.TrimSpace(req.Currency),
		Status:    strings.TrimSpace(req.Status),
		Limit:     req.Limit,
		Offset:    req.Offset,
	}

	if params.Limit <= 0 {
		params.Limit = 20
	}

	const maxSearchLimit = 50
	if params.Limit > maxSearchLimit {
		params.Limit = maxSearchLimit
	}

	if params.Offset < 0 {
		params.Offset = 0
	}

	if req.AmountMin != "" {
		val, err := parseDecimal(req.AmountMin)
		if err != nil {
			return params, fmt.Errorf("invalid amount_min: %w", err)
		}

		params.AmountMin = &val
	}

	if req.AmountMax != "" {
		val, err := parseDecimal(req.AmountMax)
		if err != nil {
			return params, fmt.Errorf("invalid amount_max: %w", err)
		}

		params.AmountMax = &val
	}

	if req.DateFrom != "" {
		t, err := parseRFC3339(req.DateFrom)
		if err != nil {
			return params, fmt.Errorf("invalid date_from: %w", err)
		}

		params.DateFrom = &t
	}

	if req.DateTo != "" {
		t, err := parseRFC3339(req.DateTo)
		if err != nil {
			return params, fmt.Errorf("invalid date_to: %w", err)
		}

		params.DateTo = &t
	}

	if req.SourceID != "" {
		id, err := uuid.Parse(req.SourceID)
		if err != nil {
			return params, fmt.Errorf("invalid source_id: %w", err)
		}

		params.SourceID = &id
	}

	return params, nil
}

func parseDecimal(s string) (decimal.Decimal, error) {
	d, err := decimal.NewFromString(s)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf("parse decimal: %w", err)
	}

	return d, nil
}

func parseRFC3339(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse time: %w", err)
	}

	return t.UTC(), nil
}

func (handler *Handlers) handleIgnoreTransactionError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	err error,
) error {
	if errors.Is(err, command.ErrTransactionNotFound) {
		return handler.notFound(ctx, fiberCtx, span, logger, "not_found", "transaction not found", err)
	}

	if errors.Is(err, command.ErrTransactionNotIgnorable) {
		handler.logSpanError(ctx, span, logger, "transaction cannot be ignored", err)

		return respondError(
			fiberCtx,
			fiber.StatusConflict,
			"invalid_state",
			"transaction cannot be ignored: only UNMATCHED transactions can be ignored",
		)
	}

	if errors.Is(err, command.ErrReasonRequired) {
		return handler.badRequest(ctx, fiberCtx, span, logger, "reason is required", err)
	}

	handler.logSpanError(ctx, span, logger, "failed to ignore transaction", err)

	return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
}
