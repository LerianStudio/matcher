// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package dto

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

func TestCommentToResponse_NilInput(t *testing.T) {
	t.Parallel()

	resp := CommentToResponse(nil)
	assert.Nil(t, resp)
}

func TestCommentToResponse_ValidInput(t *testing.T) {
	t.Parallel()

	now := testutil.FixedTime()
	comment := &entities.ExceptionComment{
		ID:          testutil.DeterministicUUID("comment-id"),
		ExceptionID: testutil.DeterministicUUID("exception-id"),
		Author:      "user@example.com",
		Content:     "Test comment",
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	resp := CommentToResponse(comment)
	assert.Equal(t, comment.ID.String(), resp.ID)
	assert.Equal(t, comment.ExceptionID.String(), resp.ExceptionID)
	assert.Equal(t, "user@example.com", resp.Author)
	assert.Equal(t, "Test comment", resp.Content)
	assert.Equal(t, now.Format(time.RFC3339), resp.CreatedAt)
}

func TestCommentsToResponse_Empty(t *testing.T) {
	t.Parallel()

	result := CommentsToResponse(nil)
	assert.NotNil(t, result)
	assert.Empty(t, result)
}

func TestCommentsToResponse_SkipsNil(t *testing.T) {
	t.Parallel()

	now := testutil.FixedTime()
	comments := []*entities.ExceptionComment{
		{ID: testutil.DeterministicUUID("comment-a"), ExceptionID: testutil.DeterministicUUID("exception-a"), Author: "a", Content: "c", CreatedAt: now, UpdatedAt: now},
		nil,
		{ID: testutil.DeterministicUUID("comment-b"), ExceptionID: testutil.DeterministicUUID("exception-b"), Author: "b", Content: "d", CreatedAt: now, UpdatedAt: now},
	}

	result := CommentsToResponse(comments)
	assert.Len(t, result, 2)
}
