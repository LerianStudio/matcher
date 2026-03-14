// Package tenant provides shared helpers for tenant-aware infrastructure keys.
package tenant

import (
	"context"
	"errors"
	"strings"

	"github.com/LerianStudio/matcher/internal/auth"
)

const objectStorageExtraSegments = 2

var (
	// ErrTenantIDRequired is returned when a tenant-scoped storage key is built without a tenant identifier.
	ErrTenantIDRequired = errors.New("tenant id is required")
	// ErrInvalidObjectStoragePathSegment is returned when a path segment is empty or path-normalizing.
	ErrInvalidObjectStoragePathSegment = errors.New("invalid object storage path segment")
)

// ScopedRedisSegments joins Redis key segments while inserting the tenant scope
// after the first prefix segment when tenant scoping is required.
func ScopedRedisSegments(ctx context.Context, includeDefaultTenant bool, segments ...string) string {
	cleaned := make([]string, 0, len(segments)+1)
	for _, segment := range segments {
		if trimmed := strings.TrimSpace(segment); trimmed != "" {
			cleaned = append(cleaned, trimmed)
		}
	}

	if len(cleaned) == 0 {
		return ""
	}

	tenantID, ok := auth.LookupTenantID(ctx)
	if !ok {
		return strings.Join(cleaned, ":")
	}

	if !includeDefaultTenant && tenantID == strings.TrimSpace(auth.GetDefaultTenantID()) {
		return strings.Join(cleaned, ":")
	}

	if len(cleaned) == 1 {
		return tenantID + ":" + cleaned[0]
	}

	parts := make([]string, 0, len(cleaned)+1)
	parts = append(parts, cleaned[0], tenantID)
	parts = append(parts, cleaned[1:]...)

	return strings.Join(parts, ":")
}

// ScopedObjectStorageKey joins object-storage path segments while placing the
// tenant identifier immediately after the logical prefix segment.
func ScopedObjectStorageKey(prefix, tenantID string, segments ...string) (string, error) {
	parts := make([]string, 0, len(segments)+objectStorageExtraSegments)

	if trimmedPrefix := strings.Trim(strings.TrimSpace(prefix), "/"); trimmedPrefix != "" {
		prefixParts, err := splitObjectStoragePrefix(trimmedPrefix)
		if err != nil {
			return "", err
		}

		parts = append(parts, prefixParts...)
	}

	trimmedTenantID, err := validateObjectStoragePathSegment(tenantID)
	if err != nil {
		return "", errors.Join(ErrTenantIDRequired, err)
	}

	parts = append(parts, trimmedTenantID)

	for _, segment := range segments {
		trimmedSegment := strings.TrimSpace(segment)
		if trimmedSegment == "" {
			continue
		}

		validatedSegment, validateErr := validateObjectStoragePathSegment(trimmedSegment)
		if validateErr != nil {
			return "", validateErr
		}

		parts = append(parts, validatedSegment)
	}

	return strings.Join(parts, "/"), nil
}

func splitObjectStoragePrefix(prefix string) ([]string, error) {
	if strings.Contains(prefix, `\`) {
		return nil, ErrInvalidObjectStoragePathSegment
	}

	rawParts := strings.Split(prefix, "/")

	parts := make([]string, 0, len(rawParts))
	for _, part := range rawParts {
		validatedPart, err := validateObjectStoragePathSegment(part)
		if err != nil {
			return nil, err
		}

		parts = append(parts, validatedPart)
	}

	return parts, nil
}

func validateObjectStoragePathSegment(segment string) (string, error) {
	trimmedSegment := strings.TrimSpace(segment)
	if trimmedSegment == "" {
		return "", ErrInvalidObjectStoragePathSegment
	}

	if strings.ContainsAny(trimmedSegment, `/\`) {
		return "", ErrInvalidObjectStoragePathSegment
	}

	if trimmedSegment == "." || trimmedSegment == ".." {
		return "", ErrInvalidObjectStoragePathSegment
	}

	return trimmedSegment, nil
}
