// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package command

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func (uc *ExceptionUseCase) validateCallback(cmd ProcessCallbackCommand) (*callbackParams, error) {
	if err := uc.validateCallbackDeps(); err != nil {
		return nil, err
	}

	if cmd.ExceptionID == uuid.Nil {
		return nil, ErrExceptionIDRequired
	}

	idempotencyKey, err := parseIdempotencyKey(cmd.IdempotencyKey)
	if err != nil {
		return nil, err
	}

	externalSystem, err := resolveExternalSystem(cmd)
	if err != nil {
		return nil, err
	}

	externalIssueID, err := resolveExternalIssueID(cmd)
	if err != nil {
		return nil, err
	}

	status, err := resolveCallbackStatus(cmd)
	if err != nil {
		return nil, err
	}

	resolutionNotes := resolveResolutionNotes(cmd)
	assignee := resolveAssignee(cmd)

	dueAt, err := resolveDueAt(cmd)
	if err != nil {
		return nil, err
	}

	updatedAt, err := resolveUpdatedAt(cmd)
	if err != nil {
		return nil, err
	}

	return &callbackParams{
		idempotencyKey:  idempotencyKey,
		externalSystem:  externalSystem,
		externalIssueID: externalIssueID,
		status:          status,
		resolutionNotes: resolutionNotes,
		assignee:        assignee,
		dueAt:           dueAt,
		updatedAt:       updatedAt,
	}, nil
}

func parseIdempotencyKey(key string) (shared.IdempotencyKey, error) {
	parsedKey, err := shared.ParseIdempotencyKey(key)
	if err != nil {
		if errors.Is(err, shared.ErrEmptyIdempotencyKey) {
			return "", shared.ErrEmptyIdempotencyKey
		}

		if errors.Is(err, shared.ErrInvalidIdempotencyKey) {
			return "", shared.ErrInvalidIdempotencyKey
		}

		return "", fmt.Errorf("parse callback idempotency key: %w", err)
	}

	return parsedKey, nil
}

func resolveExternalSystem(cmd ProcessCallbackCommand) (string, error) {
	externalSystem := normalizeCallbackString(cmd.ExternalSystem)
	if externalSystem == "" {
		externalSystem = normalizeCallbackString(cmd.CallbackType)
	}

	if externalSystem == "" {
		externalSystem = payloadString(cmd.Payload, "external_system", "externalSystem")
	}

	if externalSystem == "" {
		return "", ErrCallbackExternalSystem
	}

	return strings.ToUpper(externalSystem), nil
}

func resolveExternalIssueID(cmd ProcessCallbackCommand) (string, error) {
	externalIssueID := normalizeCallbackString(cmd.ExternalIssueID)
	if externalIssueID == "" {
		externalIssueID = payloadString(cmd.Payload, "external_issue_id", "externalIssueID")
	}

	if externalIssueID == "" {
		return "", ErrCallbackExternalIssueID
	}

	return externalIssueID, nil
}

func resolveCallbackStatus(cmd ProcessCallbackCommand) (value_objects.ExceptionStatus, error) {
	statusValue := normalizeCallbackString(cmd.Status)
	if statusValue == "" {
		statusValue = payloadString(cmd.Payload, "status")
	}

	if statusValue == "" {
		return value_objects.ExceptionStatus(""), ErrCallbackStatusRequired
	}

	status, err := value_objects.ParseExceptionStatus(strings.ToUpper(statusValue))
	if err != nil {
		return value_objects.ExceptionStatus(""), fmt.Errorf("parse status: %w", err)
	}

	return status, nil
}

func resolveResolutionNotes(cmd ProcessCallbackCommand) *string {
	resolutionNotes := normalizeOptionalString(cmd.ResolutionNotes)
	if resolutionNotes == nil {
		resolutionNotes = normalizeOptionalString(
			payloadString(cmd.Payload, "resolution_notes", "resolutionNotes"),
		)
	}

	return resolutionNotes
}

func resolveAssignee(cmd ProcessCallbackCommand) string {
	assignee := normalizeCallbackString(cmd.Assignee)
	if assignee == "" {
		assignee = payloadString(cmd.Payload, "assignee")
	}

	return assignee
}

func resolveDueAt(cmd ProcessCallbackCommand) (*time.Time, error) {
	if cmd.DueAt != nil {
		return cmd.DueAt, nil
	}

	parsedDueAt, err := payloadTime(cmd.Payload, "due_at", "dueAt")
	if err != nil {
		return nil, err
	}

	return parsedDueAt, nil
}

func resolveUpdatedAt(cmd ProcessCallbackCommand) (*time.Time, error) {
	if cmd.UpdatedAt != nil {
		return cmd.UpdatedAt, nil
	}

	parsedUpdatedAt, err := payloadTime(cmd.Payload, "updated_at", "updatedAt")
	if err != nil {
		return nil, err
	}

	return parsedUpdatedAt, nil
}
