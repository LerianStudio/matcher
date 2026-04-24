// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package dto

// UploadRequest represents file upload metadata.
// @Description File upload metadata
type UploadRequest struct {
	// Original file name
	FileName string `json:"fileName" validate:"required"                    example:"transactions_2024.csv"`
	// File format
	Format string `json:"format"   validate:"required,oneof=csv json xml" example:"csv"                   enums:"csv,json,xml"`
}

// IgnoreTransactionRequest represents the request body for ignoring a transaction.
// @Description Ignore transaction request
type IgnoreTransactionRequest struct {
	// Reason for ignoring the transaction
	Reason string `json:"reason" validate:"required" example:"Duplicate entry - already processed manually"`
}

// FilePreviewRequest represents the form fields for a file preview upload.
// @Description File preview request parameters
type FilePreviewRequest struct {
	// File format (csv, json, xml) — auto-detected from extension if omitted
	Format string `form:"format" example:"csv" enums:"csv,json,xml"`
	// Maximum sample rows to return (default 5, max 20)
	MaxRows int `form:"max_rows" example:"5" minimum:"1" maximum:"20"`
}

// SearchTransactionsRequest represents query parameters for transaction search.
// @Description Transaction search query parameters
type SearchTransactionsRequest struct {
	// Free text search across external ID and description
	Query string `query:"q"          example:"TXN-12345"`
	// Minimum transaction amount
	AmountMin string `query:"amount_min" example:"100.00"`
	// Maximum transaction amount
	AmountMax string `query:"amount_max" example:"5000.00"`
	// Start date filter (RFC3339 format)
	DateFrom string `query:"date_from"  example:"2025-01-01T00:00:00Z" format:"date-time"`
	// End date filter (RFC3339 format)
	DateTo string `query:"date_to"    example:"2025-12-31T23:59:59Z" format:"date-time"`
	// Exact reference (external_id) match
	Reference string `query:"reference"  example:"TXN-12345"`
	// Currency code filter
	Currency string `query:"currency"   example:"USD"`
	// Source ID filter
	SourceID string `query:"source_id"  validate:"omitempty,uuid" example:"550e8400-e29b-41d4-a716-446655440000"`
	// Transaction status filter
	Status string `query:"status"     example:"UNMATCHED"                              enums:"UNMATCHED,MATCHED,IGNORED,PENDING_REVIEW"`
	// Maximum number of results (default 20, max 50)
	Limit int `query:"limit"      validate:"omitempty,min=1,max=50" example:"20"  minimum:"1"                                      maximum:"50"`
	// Pagination offset
	Offset int `query:"offset"     validate:"omitempty,min=0"       example:"0"   minimum:"0"`
}
