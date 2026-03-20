// Copyright 2025 Lerian Studio.

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/LerianStudio/matcher/pkg/systemplane/adapters/store/secretcodec"
	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

// Compile-time interface check.
var _ ports.HistoryStore = (*HistoryStore)(nil)

// HistoryStore provides read access to the configuration change audit trail
// stored in PostgreSQL.
type HistoryStore struct {
	db           *sql.DB
	schema       string
	historyTable string
	secretCodec  *secretcodec.Codec
}

// ListHistory retrieves history entries matching the given filter. Results are
// returned in reverse chronological order (newest first). All filter fields are
// optional; when empty/zero they are omitted from the WHERE clause.
func (historyStore *HistoryStore) ListHistory(ctx context.Context, filter ports.HistoryFilter) ([]ports.HistoryEntry, error) {
	if historyStore == nil || historyStore.db == nil {
		return nil, ErrNilDB
	}

	builder := newHistoryQueryBuilder()
	builder.addFilterClause("kind", string(filter.Kind))
	builder.addFilterClause("scope", string(filter.Scope))
	builder.addFilterClause("subject", filter.SubjectID)
	builder.addFilterClause("key", filter.Key)

	// #nosec G202 -- table identifier is validated in bootstrap (operator-controlled, not user input).
	query := "SELECT key, scope, subject, old_value, new_value, revision, actor_id, changed_at FROM " +
		qualify(historyStore.schema, historyStore.historyTable)

	if len(builder.clauses) > 0 {
		query += " WHERE " + strings.Join(builder.clauses, " AND ")
	}

	query += " ORDER BY changed_at DESC, revision DESC, id DESC"
	builder.addPaginationClause(&query, "LIMIT", filter.Limit)
	builder.addPaginationClause(&query, "OFFSET", filter.Offset)

	rows, err := historyStore.db.QueryContext(ctx, query, builder.args...)
	if err != nil {
		return nil, fmt.Errorf("postgres history list: query: %w", err)
	}

	defer rows.Close()

	var entries []ports.HistoryEntry

	for rows.Next() {
		entry, err := historyStore.scanHistoryEntry(rows)
		if err != nil {
			return nil, err
		}

		entries = append(entries, entry)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres history list: rows iteration: %w", err)
	}

	return entries, nil
}

type historyQueryBuilder struct {
	clauses []string
	args    []any
	argIdx  int
}

func newHistoryQueryBuilder() *historyQueryBuilder {
	return &historyQueryBuilder{}
}

func (queryBuilder *historyQueryBuilder) addFilterClause(column, value string) {
	if value == "" {
		return
	}

	queryBuilder.clauses = append(queryBuilder.clauses, column+" = "+queryBuilder.nextArg())
	queryBuilder.args = append(queryBuilder.args, value)
}

func (queryBuilder *historyQueryBuilder) addPaginationClause(query *string, keyword string, value int) {
	if value <= 0 {
		return
	}

	*query += " " + keyword + " " + queryBuilder.nextArg()
	queryBuilder.args = append(queryBuilder.args, value)
}

func (queryBuilder *historyQueryBuilder) nextArg() string {
	queryBuilder.argIdx++
	return "$" + strconv.Itoa(queryBuilder.argIdx)
}

func (historyStore *HistoryStore) scanHistoryEntry(rows *sql.Rows) (ports.HistoryEntry, error) {
	var (
		key         string
		scope       string
		subject     string
		oldValueRaw []byte
		newValueRaw []byte
		revision    uint64
		actorID     string
		changedAt   time.Time
	)

	if err := rows.Scan(&key, &scope, &subject, &oldValueRaw, &newValueRaw, &revision, &actorID, &changedAt); err != nil {
		return ports.HistoryEntry{}, fmt.Errorf("postgres history list: scan: %w", err)
	}

	entry := ports.HistoryEntry{
		Key:       key,
		Scope:     domain.Scope(scope),
		SubjectID: subject,
		Revision:  domain.Revision(revision),
		ActorID:   actorID,
		ChangedAt: changedAt,
	}

	decodedOldValue, hasOldValue, err := decodeOptionalJSONValue(oldValueRaw, "old_value")
	if err != nil {
		return ports.HistoryEntry{}, err
	}

	if hasOldValue {
		decodedOldValue, err = historyStore.decryptValue(domain.Target{Kind: domain.KindConfig, Scope: entry.Scope, SubjectID: entry.SubjectID}, key, decodedOldValue)
		if err != nil {
			return ports.HistoryEntry{}, fmt.Errorf("postgres history list: decrypt old_value: %w", err)
		}
		entry.OldValue = decodedOldValue
	}

	decodedNewValue, hasNewValue, err := decodeOptionalJSONValue(newValueRaw, "new_value")
	if err != nil {
		return ports.HistoryEntry{}, err
	}

	if hasNewValue {
		decodedNewValue, err = historyStore.decryptValue(domain.Target{Kind: domain.KindConfig, Scope: entry.Scope, SubjectID: entry.SubjectID}, key, decodedNewValue)
		if err != nil {
			return ports.HistoryEntry{}, fmt.Errorf("postgres history list: decrypt new_value: %w", err)
		}
		entry.NewValue = decodedNewValue
	}

	return entry, nil
}

func (historyStore *HistoryStore) decryptValue(target domain.Target, key string, value any) (any, error) {
	if historyStore == nil || historyStore.secretCodec == nil {
		return value, nil
	}

	return historyStore.secretCodec.Decrypt(target, key, value)
}

func decodeOptionalJSONValue(rawValue []byte, fieldName string) (any, bool, error) {
	if rawValue == nil {
		return nil, false, nil
	}

	decodedValue, err := decodeJSONValue(rawValue)
	if err != nil {
		return nil, false, fmt.Errorf("postgres history list: unmarshal %s: %w", fieldName, err)
	}

	return decodedValue, true, nil
}
