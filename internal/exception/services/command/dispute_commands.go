package command

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/exception/domain/dispute"
	"github.com/LerianStudio/matcher/internal/exception/domain/repositories"
	"github.com/LerianStudio/matcher/internal/exception/ports"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// DisputeUseCase implements dispute resolution commands.
type DisputeUseCase struct {
	disputeRepo    repositories.DisputeRepository
	exceptionRepo  repositories.ExceptionRepository
	auditPublisher ports.AuditPublisher
	actorExtractor ports.ActorExtractor
	infraProvider  sharedPorts.InfrastructureProvider
}

// NewDisputeUseCase creates a new DisputeUseCase with the required dependencies.
func NewDisputeUseCase(
	disputeRepo repositories.DisputeRepository,
	exceptionRepo repositories.ExceptionRepository,
	audit ports.AuditPublisher,
	actor ports.ActorExtractor,
	infraProvider sharedPorts.InfrastructureProvider,
) (*DisputeUseCase, error) {
	if disputeRepo == nil {
		return nil, ErrNilDisputeRepository
	}

	if exceptionRepo == nil {
		return nil, ErrNilExceptionRepository
	}

	if audit == nil {
		return nil, ErrNilAuditPublisher
	}

	if actor == nil {
		return nil, ErrNilActorExtractor
	}

	if infraProvider == nil {
		return nil, ErrNilInfraProvider
	}

	return &DisputeUseCase{
		disputeRepo:    disputeRepo,
		exceptionRepo:  exceptionRepo,
		auditPublisher: audit,
		actorExtractor: actor,
		infraProvider:  infraProvider,
	}, nil
}

// OpenDisputeCommand contains parameters for opening a dispute.
type OpenDisputeCommand struct {
	ExceptionID uuid.UUID
	Category    string
	Description string
}

type openDisputeParams struct {
	actor       string
	category    dispute.DisputeCategory
	description string
}

func (uc *DisputeUseCase) validateOpenDispute(
	ctx context.Context,
	cmd OpenDisputeCommand,
) (*openDisputeParams, error) {
	if uc == nil || uc.disputeRepo == nil {
		return nil, ErrNilDisputeRepository
	}

	if uc.exceptionRepo == nil {
		return nil, ErrNilExceptionRepository
	}

	if uc.auditPublisher == nil {
		return nil, ErrNilAuditPublisher
	}

	if uc.actorExtractor == nil {
		return nil, ErrNilActorExtractor
	}

	if cmd.ExceptionID == uuid.Nil {
		return nil, ErrExceptionIDRequired
	}

	actor := strings.TrimSpace(uc.actorExtractor.GetActor(ctx))
	if actor == "" {
		return nil, ErrActorRequired
	}

	trimmedCategory := strings.TrimSpace(cmd.Category)
	if trimmedCategory == "" {
		return nil, ErrDisputeCategoryRequired
	}

	category, err := dispute.ParseDisputeCategory(trimmedCategory)
	if err != nil {
		return nil, fmt.Errorf("parse dispute category: %w", err)
	}

	description := strings.TrimSpace(cmd.Description)
	if description == "" {
		return nil, ErrDisputeDescriptionRequired
	}

	return &openDisputeParams{actor: actor, category: category, description: description}, nil
}

// OpenDispute creates and opens a new dispute for an exception.
func (uc *DisputeUseCase) OpenDispute(
	ctx context.Context,
	cmd OpenDisputeCommand,
) (*dispute.Dispute, error) {
	params, err := uc.validateOpenDispute(ctx, cmd)
	if err != nil {
		return nil, err
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.open_dispute")
	defer span.End()

	return uc.processOpenDispute(ctx, cmd, params, logger, span)
}

func (uc *DisputeUseCase) processOpenDispute(
	ctx context.Context,
	cmd OpenDisputeCommand,
	params *openDisputeParams,
	logger libLog.Logger,
	span trace.Span,
) (*dispute.Dispute, error) {
	if span != nil {
		span.SetAttributes(
			attribute.String("exception.dispute_type", params.category.String()),
			attribute.String("exception.exception_id", cmd.ExceptionID.String()),
		)
	}

	_, err := uc.exceptionRepo.FindByID(ctx, cmd.ExceptionID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to load exception", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to load exception: %v", err))

		return nil, fmt.Errorf("find exception: %w", err)
	}

	newDispute, err := dispute.NewDispute(
		ctx,
		cmd.ExceptionID,
		params.category,
		params.description,
		params.actor,
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to create dispute", err)

		return nil, fmt.Errorf("create dispute: %w", err)
	}

	if err := newDispute.Open(ctx); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to open dispute", err)

		return nil, fmt.Errorf("open dispute: %w", err)
	}

	// Atomic transaction: create dispute AND create audit log in same transaction.
	// This ensures SOX compliance - if either fails, both are rolled back.
	txLease, err := uc.infraProvider.BeginTx(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to begin transaction", err)

		return nil, fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		if rbErr := txLease.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			libOpentelemetry.HandleSpanError(span, "tx.Rollback failed", rbErr)
		}
	}()

	created, err := uc.disputeRepo.CreateWithTx(ctx, txLease.SQLTx(), newDispute)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to persist dispute", err)

		return nil, fmt.Errorf("create dispute: %w", err)
	}

	metadata := map[string]string{
		"dispute_id": created.ID.String(),
		"category":   params.category.String(),
	}

	if err := uc.auditPublisher.PublishExceptionEventWithTx(ctx, txLease.SQLTx(), ports.AuditEvent{
		ExceptionID: cmd.ExceptionID,
		Action:      "DISPUTE_OPENED",
		Actor:       params.actor,
		Notes:       params.description,
		OccurredAt:  time.Now().UTC(),
		Metadata:    metadata,
	}); err != nil {
		libOpentelemetry.HandleSpanError(span, "audit publish failed", err)

		return nil, fmt.Errorf("publish audit: %w", err)
	}

	if err := txLease.Commit(); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to commit transaction", err)

		return nil, fmt.Errorf("commit transaction: %w", err)
	}

	return created, nil
}

// CloseDisputeCommand contains parameters for closing a dispute.
type CloseDisputeCommand struct {
	DisputeID  uuid.UUID
	Resolution string
	Won        bool
}

type closeDisputeParams struct {
	actor      string
	resolution string
}

func (uc *DisputeUseCase) validateCloseDispute(
	ctx context.Context,
	cmd CloseDisputeCommand,
) (*closeDisputeParams, error) {
	if uc == nil || uc.disputeRepo == nil {
		return nil, ErrNilDisputeRepository
	}

	if uc.exceptionRepo == nil {
		return nil, ErrNilExceptionRepository
	}

	if uc.auditPublisher == nil {
		return nil, ErrNilAuditPublisher
	}

	if uc.actorExtractor == nil {
		return nil, ErrNilActorExtractor
	}

	if cmd.DisputeID == uuid.Nil {
		return nil, ErrDisputeIDRequired
	}

	actor := strings.TrimSpace(uc.actorExtractor.GetActor(ctx))
	if actor == "" {
		return nil, ErrActorRequired
	}

	resolution := strings.TrimSpace(cmd.Resolution)
	if resolution == "" {
		return nil, ErrDisputeResolutionRequired
	}

	return &closeDisputeParams{actor: actor, resolution: resolution}, nil
}

// CloseDispute closes a dispute as won or lost.
func (uc *DisputeUseCase) CloseDispute(
	ctx context.Context,
	cmd CloseDisputeCommand,
) (*dispute.Dispute, error) {
	params, err := uc.validateCloseDispute(ctx, cmd)
	if err != nil {
		return nil, err
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.close_dispute")
	defer span.End()

	return uc.processCloseDispute(ctx, cmd, params, logger, span)
}

func (uc *DisputeUseCase) processCloseDispute(
	ctx context.Context,
	cmd CloseDisputeCommand,
	params *closeDisputeParams,
	logger libLog.Logger,
	span trace.Span,
) (*dispute.Dispute, error) {
	existingDispute, err := uc.disputeRepo.FindByID(ctx, cmd.DisputeID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to load dispute", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to load dispute: %v", err))

		return nil, fmt.Errorf("find dispute: %w", err)
	}

	action := "DISPUTE_LOST"

	if cmd.Won {
		if err := existingDispute.Win(ctx, params.resolution); err != nil {
			libOpentelemetry.HandleSpanError(span, "failed to win dispute", err)

			return nil, fmt.Errorf("win dispute: %w", err)
		}

		action = "DISPUTE_WON"
	} else {
		if err := existingDispute.Lose(ctx, params.resolution); err != nil {
			libOpentelemetry.HandleSpanError(span, "failed to lose dispute", err)

			return nil, fmt.Errorf("lose dispute: %w", err)
		}
	}

	// Atomic transaction: update dispute state AND create audit log in same transaction.
	// This ensures SOX compliance - if either fails, both are rolled back.
	txLease, err := uc.infraProvider.BeginTx(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to begin transaction", err)

		return nil, fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		if rbErr := txLease.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			libOpentelemetry.HandleSpanError(span, "tx.Rollback failed", rbErr)
		}
	}()

	updated, err := uc.disputeRepo.UpdateWithTx(ctx, txLease.SQLTx(), existingDispute)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to update dispute", err)

		return nil, fmt.Errorf("update dispute: %w", err)
	}

	metadata := map[string]string{
		"dispute_id": updated.ID.String(),
	}

	if err := uc.auditPublisher.PublishExceptionEventWithTx(ctx, txLease.SQLTx(), ports.AuditEvent{
		ExceptionID: updated.ExceptionID,
		Action:      action,
		Actor:       params.actor,
		Notes:       params.resolution,
		OccurredAt:  time.Now().UTC(),
		Metadata:    metadata,
	}); err != nil {
		libOpentelemetry.HandleSpanError(span, "audit publish failed", err)

		return nil, fmt.Errorf("publish audit: %w", err)
	}

	if err := txLease.Commit(); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to commit transaction", err)

		return nil, fmt.Errorf("commit transaction: %w", err)
	}

	return updated, nil
}

// SubmitEvidenceCommand contains parameters for submitting evidence to a dispute.
type SubmitEvidenceCommand struct {
	DisputeID uuid.UUID
	Comment   string
	FileURL   *string
}

type submitEvidenceParams struct {
	actor   string
	comment string
	fileURL *string
}

func (uc *DisputeUseCase) validateSubmitEvidence(
	ctx context.Context,
	cmd SubmitEvidenceCommand,
) (*submitEvidenceParams, error) {
	if err := uc.validateSubmitEvidenceDeps(); err != nil {
		return nil, err
	}

	if cmd.DisputeID == uuid.Nil {
		return nil, ErrDisputeIDRequired
	}

	actor := strings.TrimSpace(uc.actorExtractor.GetActor(ctx))
	if actor == "" {
		return nil, ErrActorRequired
	}

	comment := strings.TrimSpace(cmd.Comment)
	if comment == "" {
		return nil, ErrDisputeCommentRequired
	}

	fileURL := parseOptionalFileURL(cmd.FileURL)

	return &submitEvidenceParams{actor: actor, comment: comment, fileURL: fileURL}, nil
}

func (uc *DisputeUseCase) validateSubmitEvidenceDeps() error {
	if uc == nil || uc.disputeRepo == nil {
		return ErrNilDisputeRepository
	}

	if uc.exceptionRepo == nil {
		return ErrNilExceptionRepository
	}

	if uc.auditPublisher == nil {
		return ErrNilAuditPublisher
	}

	if uc.actorExtractor == nil {
		return ErrNilActorExtractor
	}

	return nil
}

func parseOptionalFileURL(fileURL *string) *string {
	if fileURL == nil {
		return nil
	}

	trimmed := strings.TrimSpace(*fileURL)
	if trimmed == "" {
		return nil
	}

	return &trimmed
}

// SubmitEvidence adds evidence to an existing dispute.
func (uc *DisputeUseCase) SubmitEvidence(
	ctx context.Context,
	cmd SubmitEvidenceCommand,
) (*dispute.Dispute, error) {
	params, err := uc.validateSubmitEvidence(ctx, cmd)
	if err != nil {
		return nil, err
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.submit_evidence")
	defer span.End()

	return uc.processSubmitEvidence(ctx, cmd, params, logger, span)
}

func (uc *DisputeUseCase) processSubmitEvidence(
	ctx context.Context,
	cmd SubmitEvidenceCommand,
	params *submitEvidenceParams,
	logger libLog.Logger,
	span trace.Span,
) (*dispute.Dispute, error) {
	existingDispute, err := uc.disputeRepo.FindByID(ctx, cmd.DisputeID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to load dispute", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to load dispute: %v", err))

		return nil, fmt.Errorf("find dispute: %w", err)
	}

	if err := existingDispute.AddEvidence(ctx, params.comment, params.actor, params.fileURL); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to add evidence", err)

		return nil, fmt.Errorf("add evidence: %w", err)
	}

	// Atomic transaction: update dispute state AND create audit log in same transaction.
	// This ensures SOX compliance - if either fails, both are rolled back.
	txLease, err := uc.infraProvider.BeginTx(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to begin transaction", err)

		return nil, fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		if rbErr := txLease.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			libOpentelemetry.HandleSpanError(span, "tx.Rollback failed", rbErr)
		}
	}()

	updated, err := uc.disputeRepo.UpdateWithTx(ctx, txLease.SQLTx(), existingDispute)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to update dispute", err)

		return nil, fmt.Errorf("update dispute: %w", err)
	}

	metadata := map[string]string{
		"dispute_id": updated.ID.String(),
	}

	if params.fileURL != nil {
		metadata["file_url"] = *params.fileURL
	}

	if err := uc.auditPublisher.PublishExceptionEventWithTx(ctx, txLease.SQLTx(), ports.AuditEvent{
		ExceptionID: updated.ExceptionID,
		Action:      "EVIDENCE_SUBMITTED",
		Actor:       params.actor,
		Notes:       params.comment,
		OccurredAt:  time.Now().UTC(),
		Metadata:    metadata,
	}); err != nil {
		libOpentelemetry.HandleSpanError(span, "audit publish failed", err)

		return nil, fmt.Errorf("publish audit: %w", err)
	}

	if err := txLease.Commit(); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to commit transaction", err)

		return nil, fmt.Errorf("commit transaction: %w", err)
	}

	return updated, nil
}
