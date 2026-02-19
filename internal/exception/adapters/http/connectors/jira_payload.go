package connectors

import (
	"errors"
	"fmt"
	"strings"

	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
)

const (
	jiraSummaryIDLength = 8
	jiraSummaryMaxLen   = 255
)

// JIRA payload builder errors.
var (
	ErrNilDispatchContext    = errors.New("dispatch context is required")
	ErrMissingJiraProjectKey = errors.New("JIRA project key is required")
	ErrMissingJiraIssueType  = errors.New("JIRA issue type is required")
)

// JiraConfig holds configuration for JIRA issue creation.
type JiraConfig struct {
	ProjectKey string
	IssueType  string
}

// Validate checks if the JIRA configuration is valid.
func (c JiraConfig) Validate() error {
	if strings.TrimSpace(c.ProjectKey) == "" {
		return ErrMissingJiraProjectKey
	}

	if strings.TrimSpace(c.IssueType) == "" {
		return ErrMissingJiraIssueType
	}

	return nil
}

// JiraIssuePayload represents the JIRA API issue creation payload.
type JiraIssuePayload struct {
	Fields JiraIssueFields `json:"fields"`
}

// JiraIssueFields contains the fields for a JIRA issue.
type JiraIssueFields struct {
	Project     JiraProject   `json:"project"`
	Summary     string        `json:"summary"`
	Description string        `json:"description"`
	IssueType   JiraIssueType `json:"issuetype"`
	Priority    *JiraPriority `json:"priority,omitempty"`
	Assignee    *JiraAssignee `json:"assignee,omitempty"`
}

// JiraProject identifies the JIRA project.
type JiraProject struct {
	Key string `json:"key"`
}

// JiraIssueType specifies the issue type.
type JiraIssueType struct {
	Name string `json:"name"`
}

// JiraPriority specifies the issue priority.
type JiraPriority struct {
	Name string `json:"name"`
}

// JiraAssignee identifies the issue assignee.
type JiraAssignee struct {
	Name string `json:"name"`
}

// BuildJiraPayload creates a JIRA issue payload from dispatch context.
func BuildJiraPayload(ctx *DispatchContext, cfg JiraConfig) (*JiraIssuePayload, error) {
	if err := validateJiraInput(ctx, cfg); err != nil {
		return nil, err
	}

	payload := &JiraIssuePayload{
		Fields: JiraIssueFields{
			Project: JiraProject{
				Key: strings.TrimSpace(cfg.ProjectKey),
			},
			Summary:     buildJiraSummary(ctx),
			Description: buildJiraDescription(ctx),
			IssueType: JiraIssueType{
				Name: strings.TrimSpace(cfg.IssueType),
			},
			Priority: mapSeverityToJiraPriority(ctx.Snapshot.Severity),
		},
	}

	if ctx.Decision.Assignee != "" {
		payload.Fields.Assignee = &JiraAssignee{
			Name: ctx.Decision.Assignee,
		}
	}

	return payload, nil
}

func validateJiraInput(ctx *DispatchContext, cfg JiraConfig) error {
	if ctx == nil {
		return ErrNilDispatchContext
	}

	return cfg.Validate()
}

func buildJiraSummary(ctx *DispatchContext) string {
	summary := fmt.Sprintf("[%s] Exception %s - %s",
		ctx.Snapshot.Severity.String(),
		truncate(ctx.Snapshot.ID.String(), jiraSummaryIDLength),
		ctx.Snapshot.Reason,
	)

	return truncate(summary, jiraSummaryMaxLen)
}

func buildJiraDescription(ctx *DispatchContext) string {
	var builder strings.Builder

	builder.WriteString("h2. Exception Details\n\n")
	fmt.Fprintf(&builder, "*Exception ID:* %s\n", ctx.Snapshot.ID.String())
	fmt.Fprintf(&builder, "*Transaction ID:* %s\n", ctx.Snapshot.TransactionID.String())
	fmt.Fprintf(&builder, "*Severity:* %s\n", ctx.Snapshot.Severity.String())
	fmt.Fprintf(&builder, "*Status:* %s\n", ctx.Snapshot.Status.String())
	fmt.Fprintf(&builder, "*Amount:* %s %s\n", ctx.Snapshot.Amount.String(), ctx.Snapshot.Currency)
	fmt.Fprintf(&builder, "*Reason:* %s\n", ctx.Snapshot.Reason)
	fmt.Fprintf(&builder, "*Source Type:* %s\n", ctx.Snapshot.SourceType)
	fmt.Fprintf(&builder, "*Created At:* %s\n", ctx.Snapshot.CreatedAt.Format("2006-01-02 15:04:05 UTC"))

	if ctx.Snapshot.DueAt != nil {
		fmt.Fprintf(&builder, "*Due At:* %s\n", ctx.Snapshot.DueAt.Format("2006-01-02 15:04:05 UTC"))
	}

	builder.WriteString("\nh2. Routing Information\n\n")
	fmt.Fprintf(&builder, "*Target:* %s\n", ctx.Decision.Target)
	fmt.Fprintf(&builder, "*Queue:* %s\n", ctx.Decision.Queue)
	fmt.Fprintf(&builder, "*Rule:* %s\n", ctx.Decision.RuleName)

	if ctx.TraceID != "" {
		fmt.Fprintf(&builder, "\n*Trace ID:* %s\n", ctx.TraceID)
	}

	return builder.String()
}

func mapSeverityToJiraPriority(severity value_objects.ExceptionSeverity) *JiraPriority {
	var priorityName string

	switch severity {
	case value_objects.ExceptionSeverityCritical:
		priorityName = "Highest"
	case value_objects.ExceptionSeverityHigh:
		priorityName = "High"
	case value_objects.ExceptionSeverityMedium:
		priorityName = "Medium"
	case value_objects.ExceptionSeverityLow:
		priorityName = "Low"
	default:
		priorityName = "Medium"
	}

	return &JiraPriority{Name: priorityName}
}

const truncateEllipsisLen = 3

func truncate(value string, maxLen int) string {
	runes := []rune(value)
	if len(runes) <= maxLen {
		return value
	}

	if maxLen <= truncateEllipsisLen {
		return string(runes[:maxLen])
	}

	return string(runes[:maxLen-truncateEllipsisLen]) + "..."
}
