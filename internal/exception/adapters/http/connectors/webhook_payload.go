package connectors

import (
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
)

// ErrMissingWebhookEventType is returned when the webhook event type is not provided.
var ErrMissingWebhookEventType = errors.New("webhook event type is required")

// WebhookConfig holds configuration for webhook dispatch.
type WebhookConfig struct {
	EventType string
}

// Validate checks if the webhook configuration is valid.
func (c WebhookConfig) Validate() error {
	if strings.TrimSpace(c.EventType) == "" {
		return ErrMissingWebhookEventType
	}

	return nil
}

// WebhookPayload represents the webhook event payload structure.
type WebhookPayload struct {
	EventID   string               `json:"eventId"`
	EventType string               `json:"eventType"`
	Timestamp string               `json:"timestamp"`
	Data      WebhookExceptionData `json:"data"`
	Metadata  WebhookMetadata      `json:"metadata"`
}

// WebhookExceptionData contains exception details for webhook payload.
type WebhookExceptionData struct {
	ExceptionID   string `json:"exceptionId"`
	TransactionID string `json:"transactionId"`
	Severity      string `json:"severity"`
	Status        string `json:"status"`
	Amount        string `json:"amount"`
	Currency      string `json:"currency"`
	Reason        string `json:"reason"`
	SourceType    string `json:"sourceType"`
	CreatedAt     string `json:"createdAt"`
	DueAt         string `json:"dueAt,omitempty"`
}

// WebhookMetadata contains routing and tracing information.
type WebhookMetadata struct {
	TraceID  string `json:"traceId,omitempty"`
	Target   string `json:"target"`
	Queue    string `json:"queue,omitempty"`
	RuleName string `json:"ruleName,omitempty"`
	Assignee string `json:"assignee,omitempty"`
}

// BuildWebhookPayload creates a webhook payload from dispatch context.
func BuildWebhookPayload(
	ctx *DispatchContext,
	cfg WebhookConfig,
	eventID uuid.UUID,
) (*WebhookPayload, error) {
	if err := validateWebhookInput(ctx, cfg); err != nil {
		return nil, err
	}

	data := WebhookExceptionData{
		ExceptionID:   ctx.Snapshot.ID.String(),
		TransactionID: ctx.Snapshot.TransactionID.String(),
		Severity:      ctx.Snapshot.Severity.String(),
		Status:        ctx.Snapshot.Status.String(),
		Amount:        ctx.Snapshot.Amount.String(),
		Currency:      ctx.Snapshot.Currency,
		Reason:        ctx.Snapshot.Reason,
		SourceType:    ctx.Snapshot.SourceType,
		CreatedAt:     ctx.Snapshot.CreatedAt.UTC().Format(time.RFC3339),
	}

	if ctx.Snapshot.DueAt != nil {
		data.DueAt = ctx.Snapshot.DueAt.UTC().Format(time.RFC3339)
	}

	metadata := WebhookMetadata{
		TraceID:  ctx.TraceID,
		Target:   string(ctx.Decision.Target),
		Queue:    ctx.Decision.Queue,
		RuleName: ctx.Decision.RuleName,
		Assignee: ctx.Decision.Assignee,
	}

	payload := &WebhookPayload{
		EventID:   eventID.String(),
		EventType: strings.TrimSpace(cfg.EventType),
		Timestamp: ctx.Timestamp.UTC().Format(time.RFC3339),
		Data:      data,
		Metadata:  metadata,
	}

	return payload, nil
}

func validateWebhookInput(ctx *DispatchContext, cfg WebhookConfig) error {
	if ctx == nil {
		return ErrNilDispatchContext
	}

	return cfg.Validate()
}
