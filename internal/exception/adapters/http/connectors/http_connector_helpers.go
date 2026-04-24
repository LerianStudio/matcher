// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package connectors

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode"

	"github.com/LerianStudio/lib-commons/v5/commons/backoff"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"

	"github.com/LerianStudio/matcher/internal/exception/domain/services"
)

func readRequestBody(ctx context.Context, req *http.Request, logger libLog.Logger) ([]byte, error) {
	if req.Body == nil {
		return nil, nil
	}

	bodyBytes, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, fmt.Errorf("read request body: %w", err)
	}

	if err := req.Body.Close(); err != nil {
		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("failed to close request body: %v", err))
	}

	return bodyBytes, nil
}

// readResponseBody reads a limited amount from the response body, closes it,
// and returns a sanitized string safe for error messages and log output.
func readResponseBody(ctx context.Context, resp *http.Response, logger libLog.Logger) string {
	if resp.Body == nil {
		return ""
	}

	defer func() {
		if err := resp.Body.Close(); err != nil {
			logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("failed to close response body: %v", err))
		}
	}()

	limited := io.LimitReader(resp.Body, maxBodyReadLimit)

	bodyBytes, err := io.ReadAll(limited)
	if err != nil {
		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("failed to read response body for error context: %v", err))

		return "[UNREADABLE]"
	}

	if len(bodyBytes) == 0 {
		return ""
	}

	return sanitizeBody(string(bodyBytes))
}

// sanitizeBody strips non-printable/control characters and truncates content
// to a safe length for inclusion in error messages and log output.
func sanitizeBody(raw string) string {
	sanitized := strings.Map(func(r rune) rune {
		if unicode.IsPrint(r) {
			return r
		}

		return -1
	}, raw)

	sanitized = strings.TrimSpace(sanitized)

	if len(sanitized) > maxBodyLogLength {
		return sanitized[:maxBodyLogLength] + " [TRUNCATED]"
	}

	return sanitized
}

func retryDelay(baseBackoff time.Duration, attempt int) time.Duration {
	return backoff.ExponentialWithJitter(baseBackoff, attempt)
}

func sleepWithContext(ctx context.Context, duration time.Duration) error {
	if err := backoff.WaitContext(ctx, duration); err != nil {
		return fmt.Errorf("sleep interrupted: %w", err)
	}

	return nil
}

func isRetryableStatus(statusCode int) bool {
	return statusCode == http.StatusTooManyRequests || statusCode >= 500
}

func computeHMACSHA256(payload []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)

	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// generateIdempotencyKey builds a deterministic key for dispatch deduplication.
// The same exception dispatched to the same target always produces the same key,
// ensuring at-most-once delivery within the Redis TTL window. Key expiration for
// retries is handled by Redis TTL, so no timestamp component is needed.
func generateIdempotencyKey(
	exceptionID string,
	target services.RoutingTarget,
	queue string,
) string {
	key := fmt.Sprintf("dispatch:%s:%s", target, exceptionID)

	if queue != "" {
		key = fmt.Sprintf("%s:%s", key, queue)
	}

	return key
}
