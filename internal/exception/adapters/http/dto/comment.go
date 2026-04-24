// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package dto

import (
	"time"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
)

// AddCommentRequest represents the payload for adding a comment to an exception.
// @Description Add comment request payload
type AddCommentRequest struct {
	// Comment content
	Content string `json:"content" validate:"required,min=1,max=5000" example:"This transaction needs review by the finance team."`
}

// CommentResponse represents a comment in API responses.
// @Description Comment details
type CommentResponse struct {
	// Unique identifier for the comment
	ID string `json:"id"          example:"550e8400-e29b-41d4-a716-446655440000"`
	// Exception ID this comment belongs to
	ExceptionID string `json:"exceptionId" example:"550e8400-e29b-41d4-a716-446655440001"`
	// Author of the comment
	Author string `json:"author"      example:"user@example.com"`
	// Comment content
	Content string `json:"content"     example:"This transaction needs review."`
	// Creation timestamp in RFC3339 format
	CreatedAt string `json:"createdAt"   example:"2025-01-15T10:30:00Z"`
	// Last update timestamp in RFC3339 format
	UpdatedAt string `json:"updatedAt"   example:"2025-01-15T10:30:00Z"`
}

// ListCommentsResponse represents a list of comments.
// @Description List of comments for an exception
type ListCommentsResponse struct {
	// List of comments
	Items []*CommentResponse `json:"items"`
}

// CommentToResponse converts a domain entity to a response DTO.
func CommentToResponse(comment *entities.ExceptionComment) *CommentResponse {
	if comment == nil {
		return nil
	}

	return &CommentResponse{
		ID:          comment.ID.String(),
		ExceptionID: comment.ExceptionID.String(),
		Author:      comment.Author,
		Content:     comment.Content,
		CreatedAt:   comment.CreatedAt.Format(time.RFC3339),
		UpdatedAt:   comment.UpdatedAt.Format(time.RFC3339),
	}
}

// CommentsToResponse converts a slice of comment entities to response DTOs.
func CommentsToResponse(comments []*entities.ExceptionComment) []*CommentResponse {
	result := make([]*CommentResponse, 0, len(comments))

	for _, c := range comments {
		if resp := CommentToResponse(c); resp != nil {
			result = append(result, resp)
		}
	}

	return result
}
