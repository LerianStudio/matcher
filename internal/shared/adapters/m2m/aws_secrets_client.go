package m2m

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	awssm "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/aws/smithy-go"

	"github.com/LerianStudio/matcher/internal/shared/ports"
)

// Sentinel errors for AWS Secrets Manager operations.
var (
	ErrM2MCredentialsNotFound = errors.New("M2M credentials not found in secret store")
	ErrM2MVaultAccessDenied   = errors.New("access denied to M2M credentials vault")
	ErrM2MInvalidCredentials  = errors.New("M2M secret value missing required fields (clientId, clientSecret)")
	ErrAWSSecretsClientNil    = errors.New("AWS Secrets Manager client is nil")
)

// AWSSecretsManagerAPI is the subset of the AWS Secrets Manager client interface
// required for M2M credential retrieval. Using an interface enables test mocking
// without importing the full SDK in test files.
type AWSSecretsManagerAPI interface {
	GetSecretValue(ctx context.Context, params *awssm.GetSecretValueInput, optFns ...func(*awssm.Options)) (*awssm.GetSecretValueOutput, error)
}

// AWSSecretsClient wraps the AWS Secrets Manager SDK to implement SecretsClient.
// It fetches M2M credentials from the canonical path:
//
//	tenants/{env}/{tenantOrgID}/{applicationName}/m2m/{targetService}/credentials
type AWSSecretsClient struct {
	client AWSSecretsManagerAPI
}

// NewAWSSecretsClient creates a new AWS Secrets Manager client adapter.
// Returns ErrAWSSecretsClientNil if client is nil to prevent nil-pointer panics on first use.
func NewAWSSecretsClient(client AWSSecretsManagerAPI) (*AWSSecretsClient, error) {
	if client == nil {
		return nil, ErrAWSSecretsClientNil
	}

	return &AWSSecretsClient{client: client}, nil
}

// secretPayload is the expected JSON structure of the secret value.
type secretPayload struct {
	ClientID     string `json:"clientId"`
	ClientSecret string `json:"clientSecret"` //nolint:nolintlint,gosec // G117: internal deserialization struct for AWS Secrets Manager, not exposed in API
}

// GetM2MCredentials retrieves M2M credentials from AWS Secrets Manager.
func (client *AWSSecretsClient) GetM2MCredentials(
	ctx context.Context,
	env, tenantOrgID, applicationName, targetService string,
) (*ports.M2MCredentials, error) {
	secretPath := fmt.Sprintf("tenants/%s/%s/%s/m2m/%s/credentials",
		env, tenantOrgID, applicationName, targetService)

	input := &awssm.GetSecretValueInput{
		SecretId: &secretPath,
	}

	output, err := client.client.GetSecretValue(ctx, input)
	if err != nil {
		return nil, classifyAWSError(err, tenantOrgID)
	}

	if output.SecretString == nil {
		return nil, fmt.Errorf("%w: secret value is nil for tenant %s", ErrM2MInvalidCredentials, tenantOrgID)
	}

	var payload secretPayload
	if err := json.Unmarshal([]byte(*output.SecretString), &payload); err != nil {
		return nil, fmt.Errorf("unmarshal M2M credentials for tenant %s: %w", tenantOrgID, err)
	}

	if payload.ClientID == "" || payload.ClientSecret == "" {
		return nil, fmt.Errorf("%w: tenant %s", ErrM2MInvalidCredentials, tenantOrgID)
	}

	return &ports.M2MCredentials{
		ClientID:     payload.ClientID,
		ClientSecret: payload.ClientSecret,
	}, nil
}

// classifyAWSError maps AWS SDK errors to domain sentinel errors.
func classifyAWSError(err error, tenantOrgID string) error {
	var notFoundErr *smtypes.ResourceNotFoundException
	if errors.As(err, &notFoundErr) {
		return fmt.Errorf("%w: tenant %s", ErrM2MCredentialsNotFound, tenantOrgID)
	}

	// IAM AccessDeniedException is returned as a Smithy API error with code "AccessDeniedException".
	// This covers both IAM permission issues and KMS decryption failures.
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "AccessDeniedException" {
		return fmt.Errorf("%w: tenant %s: %w", ErrM2MVaultAccessDenied, tenantOrgID, err)
	}

	return fmt.Errorf("retrieve M2M credentials for tenant %s: %w", tenantOrgID, err)
}
