// Copyright 2025 Lerian Studio.

package mongodb

import (
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
)

// BSON helpers.

// targetFromDoc extracts a domain.Target and revision from a BSON document
// containing kind, scope, subject, and revision fields. Returns false when
// any required field is missing or invalid.
func targetFromDoc(doc *bson.D) (domain.Target, domain.Revision, domain.ApplyBehavior, bool) {
	kindStr := bsonLookupString(doc, "kind")
	scopeStr := bsonLookupString(doc, "scope")
	subject := bsonLookupString(doc, "subject")

	kind, err := domain.ParseKind(kindStr)
	if err != nil {
		return domain.Target{}, domain.RevisionZero, domain.ApplyBundleRebuild, false
	}

	scope, err := domain.ParseScope(scopeStr)
	if err != nil {
		return domain.Target{}, domain.RevisionZero, domain.ApplyBundleRebuild, false
	}

	target, err := domain.NewTarget(kind, scope, subject)
	if err != nil {
		return domain.Target{}, domain.RevisionZero, domain.ApplyBundleRebuild, false
	}

	revisionRaw, ok := bsonLookupUint64(doc, "revision")
	if !ok {
		return domain.Target{}, domain.RevisionZero, domain.ApplyBundleRebuild, false
	}

	revision := domain.Revision(revisionRaw)
	behavior := domain.ApplyBehavior(bsonLookupString(doc, "apply_behavior"))

	return target, revision, behavior, true
}

// bsonLookupString extracts a string value from a bson.D by key.
// Returns "" when the key is absent or the value is not a string.
func bsonLookupString(doc *bson.D, key string) string {
	if doc == nil {
		return ""
	}

	for _, elem := range *doc {
		if elem.Key == key {
			s, ok := elem.Value.(string)
			if ok {
				return s
			}

			return ""
		}
	}

	return ""
}

// bsonLookupUint64 extracts a uint64 value from a bson.D by key.
// Returns false when the key is absent or the value cannot be represented as uint64.
func bsonLookupUint64(doc *bson.D, key string) (uint64, bool) {
	if doc == nil {
		return 0, false
	}

	for _, elem := range *doc {
		if elem.Key == key {
			switch value := elem.Value.(type) {
			case int32:
				if value < 0 {
					return 0, false
				}

				return uint64(value), true
			case int64:
				if value < 0 {
					return 0, false
				}

				return uint64(value), true
			case uint64:
				return value, true
			default:
				return 0, false
			}
		}
	}

	return 0, false
}
