// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package command

import (
	"fmt"
	"strings"
	"time"
)

func normalizeCallbackString(value string) string {
	return strings.TrimSpace(value)
}

func normalizeOptionalString(value string) *string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}

	return &trimmed
}

func payloadString(payload map[string]any, keys ...string) string {
	if payload == nil {
		return ""
	}

	for _, key := range keys {
		if value, ok := payload[key]; ok {
			switch typed := value.(type) {
			case string:
				return strings.TrimSpace(typed)
			case fmt.Stringer:
				return strings.TrimSpace(typed.String())
			}
		}
	}

	return ""
}

func payloadTime(payload map[string]any, keys ...string) (*time.Time, error) {
	if payload == nil {
		return nil, nil
	}

	for _, key := range keys {
		if value, ok := payload[key]; ok {
			switch typed := value.(type) {
			case time.Time:
				copyValue := typed
				return &copyValue, nil
			case *time.Time:
				if typed != nil {
					copyValue := *typed
					return &copyValue, nil
				}
			case string:
				trimmed := strings.TrimSpace(typed)
				if trimmed == "" {
					return nil, nil
				}

				parsed, err := time.Parse(time.RFC3339, trimmed)
				if err != nil {
					return nil, fmt.Errorf("parse %s: %w", key, err)
				}

				return &parsed, nil
			}
		}
	}

	return nil, nil
}
