// Copyright 2025 Lerian Studio.

// Package mongodb provides MongoDB-backed adapters for the systemplane stores.
package mongodb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/LerianStudio/matcher/pkg/systemplane/adapters/store/secretcodec"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

// Compile-time interface check.
var _ ports.HistoryStore = (*HistoryStore)(nil)

// HistoryStore implements ports.HistoryStore backed by MongoDB. History
// documents are stored in a dedicated collection and returned in reverse
// chronological order (newest first).
type HistoryStore struct {
	history     *mongo.Collection
	secretCodec *secretcodec.Codec
}

// ListHistory retrieves history entries matching the given filter. Results
// are sorted by changed_at descending (newest first). Only non-zero filter
// fields are applied as query predicates. Limit and Offset control
// pagination.
func (historyStore *HistoryStore) ListHistory(ctx context.Context,
	filter ports.HistoryFilter,
) ([]ports.HistoryEntry, error) {
	if historyStore == nil {
		return nil, ErrNilStore
	}

	if historyStore.history == nil {
		return nil, ErrNilHistory
	}

	query := buildHistoryFilter(filter)

	opts := options.Find().SetSort(bson.D{{Key: "changed_at", Value: -1}, {Key: "revision", Value: -1}, {Key: "_id", Value: -1}})

	if filter.Offset > 0 {
		opts.SetSkip(int64(filter.Offset))
	}

	if filter.Limit > 0 {
		opts.SetLimit(int64(filter.Limit))
	}

	cursor, err := historyStore.history.Find(ctx, query, opts)
	if err != nil {
		return nil, fmt.Errorf("mongodb history list: find: %w", err)
	}
	defer cursor.Close(ctx)

	var docs []historyDoc
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("mongodb history list: decode: %w", err)
	}

	if len(docs) == 0 {
		return nil, nil
	}

	entries := make([]ports.HistoryEntry, len(docs))
	for i := range docs {
		entry, err := docs[i].toHistoryEntryWithCodec(historyStore.secretCodec)
		if err != nil {
			return nil, fmt.Errorf("mongodb history list: decode secret history: %w", err)
		}

		entries[i] = entry
	}

	return entries, nil
}

// buildHistoryFilter constructs a BSON filter from the non-zero fields of
// a HistoryFilter. Only fields with non-empty values are included as query
// predicates.
func buildHistoryFilter(filter ports.HistoryFilter) bson.D {
	query := bson.D{}

	if filter.Kind != "" {
		query = append(query, bson.E{Key: "kind", Value: string(filter.Kind)})
	}

	if filter.Scope != "" {
		query = append(query, bson.E{Key: "scope", Value: string(filter.Scope)})
	}

	if filter.SubjectID != "" {
		query = append(query, bson.E{Key: "subject", Value: filter.SubjectID})
	}

	if filter.Key != "" {
		query = append(query, bson.E{Key: "key", Value: filter.Key})
	}

	return query
}
