package command

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
)

func (uc *UseCase) cloneSourcesIntoResult(ctx context.Context, input CloneContextInput, newContextID uuid.UUID, result *entities.CloneResult) error {
	sources, fieldMaps, err := uc.cloneSourcesAndFieldMaps(ctx, nil, input.SourceContextID, newContextID)
	if err != nil {
		return err
	}

	result.SourcesCloned = sources
	result.FieldMapsCloned = fieldMaps

	return nil
}

func (uc *UseCase) cloneSourcesIntoResultWithTx(ctx context.Context, tx *sql.Tx, input CloneContextInput, newContextID uuid.UUID, result *entities.CloneResult) error {
	sources, fieldMaps, err := uc.cloneSourcesAndFieldMaps(ctx, tx, input.SourceContextID, newContextID)
	if err != nil {
		return err
	}

	result.SourcesCloned = sources
	result.FieldMapsCloned = fieldMaps

	return nil
}

func (uc *UseCase) cloneSourcesAndFieldMaps(ctx context.Context, tx *sql.Tx, sourceContextID, newContextID uuid.UUID) (sourcesCloned, fieldMapsCloned int, err error) {
	sources, err := uc.fetchAllSources(ctx, sourceContextID)
	if err != nil {
		return 0, 0, err
	}

	if len(sources) == 0 {
		return 0, 0, nil
	}

	sourceIDs := make([]uuid.UUID, len(sources))
	for i, src := range sources {
		sourceIDs[i] = src.ID
	}

	fieldMapsExist, err := uc.fieldMapRepo.ExistsBySourceIDs(ctx, sourceIDs)
	if err != nil {
		return 0, 0, fmt.Errorf("checking field maps existence: %w", err)
	}

	now := time.Now().UTC()

	for _, src := range sources {
		newSourceID := uuid.New()
		newSource := &entities.ReconciliationSource{
			ID:        newSourceID,
			ContextID: newContextID,
			Name:      src.Name,
			Type:      src.Type,
			Side:      src.Side,
			Config:    cloneMap(ctx, src.Config),
			CreatedAt: now,
			UpdatedAt: now,
		}

		if createErr := uc.createSourceWithOptionalTx(ctx, tx, newSource); createErr != nil {
			return sourcesCloned, fieldMapsCloned, fmt.Errorf("creating cloned source %q: %w", src.Name, createErr)
		}

		sourcesCloned++

		if fieldMapsExist[src.ID] {
			cloned, cloneErr := uc.cloneFieldMap(ctx, tx, src.ID, newContextID, newSourceID, now)
			if cloneErr != nil {
				return sourcesCloned, fieldMapsCloned, cloneErr
			}

			if cloned {
				fieldMapsCloned++
			}
		}
	}

	return sourcesCloned, fieldMapsCloned, nil
}

func (uc *UseCase) createSourceWithOptionalTx(ctx context.Context, tx *sql.Tx, source *entities.ReconciliationSource) error {
	if tx != nil {
		txCreator, ok := uc.sourceRepo.(sourceTxCreator)
		if !ok {
			return fmt.Errorf("source repository does not support CreateWithTx: %w", ErrCloneProviderRequired)
		}

		_, err := txCreator.CreateWithTx(ctx, tx, source)

		return err
	}

	_, err := uc.sourceRepo.Create(ctx, source)

	return err
}

func (uc *UseCase) cloneFieldMap(ctx context.Context, tx *sql.Tx, oldSourceID, newContextID, newSourceID uuid.UUID, now time.Time) (bool, error) {
	fm, err := uc.fieldMapRepo.FindBySourceID(ctx, oldSourceID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}

		return false, fmt.Errorf("fetching field map for source %s: %w", oldSourceID, err)
	}

	newFieldMap := &entities.FieldMap{
		ID:        uuid.New(),
		ContextID: newContextID,
		SourceID:  newSourceID,
		Mapping:   cloneMap(ctx, fm.Mapping),
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if tx != nil {
		txCreator, ok := uc.fieldMapRepo.(fieldMapTxCreator)
		if !ok {
			return false, fmt.Errorf("field map repository does not support CreateWithTx: %w", ErrCloneProviderRequired)
		}

		if _, err := txCreator.CreateWithTx(ctx, tx, newFieldMap); err != nil {
			return false, fmt.Errorf("creating cloned field map: %w", err)
		}

		return true, nil
	}

	if _, err := uc.fieldMapRepo.Create(ctx, newFieldMap); err != nil {
		return false, fmt.Errorf("creating cloned field map: %w", err)
	}

	return true, nil
}

func (uc *UseCase) fetchAllSources(ctx context.Context, contextID uuid.UUID) ([]*entities.ReconciliationSource, error) {
	var allSources []*entities.ReconciliationSource

	cursor := ""
	for {
		sources, pagination, err := uc.sourceRepo.FindByContextID(ctx, contextID, cursor, maxClonePaginationLimit)
		if err != nil {
			return nil, fmt.Errorf("fetching sources page: %w", err)
		}

		allSources = append(allSources, sources...)

		if pagination.Next == "" {
			break
		}

		cursor = pagination.Next
	}

	return allSources, nil
}

func cloneMap(ctx context.Context, src map[string]any) map[string]any {
	if src == nil {
		return nil
	}

	cloned, _ := cloneValue(ctx, src).(map[string]any)
	if cloned == nil {
		return map[string]any{}
	}

	return cloned
}

func cloneValue(ctx context.Context, value any) any {
	switch typed := value.(type) {
	case nil,
		bool,
		string,
		float32,
		float64,
		int,
		int8,
		int16,
		int32,
		int64,
		uint,
		uint8,
		uint16,
		uint32,
		uint64,
		uintptr:
		return typed
	case map[string]any:
		cloned := make(map[string]any, len(typed))
		for key, nested := range typed {
			cloned[key] = cloneValue(ctx, nested)
		}

		return cloned
	case []any:
		return cloneSlice(ctx, typed)
	}

	reflected := reflect.ValueOf(value)
	if !reflected.IsValid() {
		return nil
	}

	switch reflected.Kind() {
	case reflect.Interface, reflect.Pointer:
		if reflected.IsNil() {
			return nil
		}

		return cloneValue(ctx, reflected.Elem().Interface())
	case reflect.Map:
		if reflected.Type().Key().Kind() != reflect.String {
			return value
		}

		cloned := make(map[string]any, reflected.Len())

		iterator := reflected.MapRange()
		for iterator.Next() {
			cloned[iterator.Key().String()] = cloneValue(ctx, iterator.Value().Interface())
		}

		return cloned
	case reflect.Slice, reflect.Array:
		return cloneSliceValue(ctx, reflected)
	default:
		return value
	}
}

func cloneSlice(ctx context.Context, src []any) []any {
	if src == nil {
		return nil
	}

	cloned := make([]any, len(src))
	for i, item := range src {
		cloned[i] = cloneValue(ctx, item)
	}

	return cloned
}

func cloneSliceValue(ctx context.Context, src reflect.Value) []any {
	cloned := make([]any, src.Len())
	for i := 0; i < src.Len(); i++ {
		cloned[i] = cloneValue(ctx, src.Index(i).Interface())
	}

	return cloned
}
