//go:build unit

package m2m_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssm "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/shared/adapters/m2m"
)

// mockAWSSecretsManager implements AWSSecretsManagerAPI for testing.
type mockAWSSecretsManager struct {
	output *awssm.GetSecretValueOutput
	err    error
	input  *awssm.GetSecretValueInput
}

func (m *mockAWSSecretsManager) GetSecretValue(
	_ context.Context,
	input *awssm.GetSecretValueInput,
	_ ...func(*awssm.Options),
) (*awssm.GetSecretValueOutput, error) {
	m.input = input

	return m.output, m.err
}

func TestAWSSecretsClient_GetM2MCredentials_Success(t *testing.T) {
	t.Parallel()

	secret := `{"clientId":"cid-123","clientSecret":"csecret-456"}`

	mock := &mockAWSSecretsManager{
		output: &awssm.GetSecretValueOutput{
			SecretString: aws.String(secret),
		},
	}

	client, clientErr := m2m.NewAWSSecretsClient(mock)
	require.NoError(t, clientErr)

	creds, err := client.GetM2MCredentials(context.Background(), "staging", "org-1", "matcher", "fetcher")
	require.NoError(t, err)
	assert.Equal(t, "cid-123", creds.ClientID)
	assert.Equal(t, "csecret-456", creds.ClientSecret)

	// Verify the secret path matches the canonical pattern
	require.NotNil(t, mock.input, "GetSecretValue input should be captured")
	assert.Equal(t, "tenants/staging/org-1/matcher/m2m/fetcher/credentials",
		aws.ToString(mock.input.SecretId), "secret path should follow canonical pattern")
}

func TestAWSSecretsClient_GetM2MCredentials_NotFound(t *testing.T) {
	t.Parallel()

	mock := &mockAWSSecretsManager{
		err: &smtypes.ResourceNotFoundException{
			Message: aws.String("secret not found"),
		},
	}

	client, clientErr := m2m.NewAWSSecretsClient(mock)
	require.NoError(t, clientErr)

	creds, err := client.GetM2MCredentials(context.Background(), "staging", "org-1", "matcher", "fetcher")
	assert.Nil(t, creds)
	require.Error(t, err)
	assert.True(t, errors.Is(err, m2m.ErrM2MCredentialsNotFound))
}

func TestAWSSecretsClient_GetM2MCredentials_MissingFields(t *testing.T) {
	t.Parallel()

	// clientSecret is missing
	secret := `{"clientId":"cid-123"}`

	mock := &mockAWSSecretsManager{
		output: &awssm.GetSecretValueOutput{
			SecretString: aws.String(secret),
		},
	}

	client, clientErr := m2m.NewAWSSecretsClient(mock)
	require.NoError(t, clientErr)

	creds, err := client.GetM2MCredentials(context.Background(), "staging", "org-1", "matcher", "fetcher")
	assert.Nil(t, creds)
	require.Error(t, err)
	assert.True(t, errors.Is(err, m2m.ErrM2MInvalidCredentials))
}

func TestAWSSecretsClient_GetM2MCredentials_NilSecretString(t *testing.T) {
	t.Parallel()

	mock := &mockAWSSecretsManager{
		output: &awssm.GetSecretValueOutput{
			SecretString: nil,
		},
	}

	client, clientErr := m2m.NewAWSSecretsClient(mock)
	require.NoError(t, clientErr)

	creds, err := client.GetM2MCredentials(context.Background(), "staging", "org-1", "matcher", "fetcher")
	assert.Nil(t, creds)
	require.Error(t, err)
	assert.True(t, errors.Is(err, m2m.ErrM2MInvalidCredentials))
}

func TestAWSSecretsClient_GetM2MCredentials_NilOutput(t *testing.T) {
	t.Parallel()

	mock := &mockAWSSecretsManager{
		output: nil,
		err:    nil,
	}

	client, clientErr := m2m.NewAWSSecretsClient(mock)
	require.NoError(t, clientErr)

	creds, err := client.GetM2MCredentials(context.Background(), "staging", "org-1", "matcher", "fetcher")
	assert.Nil(t, creds)
	require.Error(t, err)
	assert.ErrorIs(t, err, m2m.ErrM2MInvalidCredentials)
	assert.Contains(t, err.Error(), "response is nil")
}

func TestAWSSecretsClient_GetM2MCredentials_InvalidJSON(t *testing.T) {
	t.Parallel()

	mock := &mockAWSSecretsManager{
		output: &awssm.GetSecretValueOutput{
			SecretString: aws.String("not json"),
		},
	}

	client, clientErr := m2m.NewAWSSecretsClient(mock)
	require.NoError(t, clientErr)

	creds, err := client.GetM2MCredentials(context.Background(), "staging", "org-1", "matcher", "fetcher")
	assert.Nil(t, creds)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal M2M credentials")
}

func TestAWSSecretsClient_GetM2MCredentials_GenericError(t *testing.T) {
	t.Parallel()

	mock := &mockAWSSecretsManager{
		err: errors.New("network timeout"),
	}

	client, clientErr := m2m.NewAWSSecretsClient(mock)
	require.NoError(t, clientErr)

	creds, err := client.GetM2MCredentials(context.Background(), "staging", "org-1", "matcher", "fetcher")
	assert.Nil(t, creds)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "network timeout")
	assert.False(t, errors.Is(err, m2m.ErrM2MCredentialsNotFound))
}

func TestAWSSecretsClient_GetM2MCredentials_AccessDenied(t *testing.T) {
	t.Parallel()

	// Smithy API errors use the ResponseError wrapper with a concrete type.
	mock := &mockAWSSecretsManager{
		err: &smithy.OperationError{
			ServiceID:     "SecretsManager",
			OperationName: "GetSecretValue",
			Err: &smithyhttp.ResponseError{
				Response: &smithyhttp.Response{},
				Err: &smithy.GenericAPIError{
					Code:    "AccessDeniedException",
					Message: "User is not authorized",
				},
			},
		},
	}

	client, clientErr := m2m.NewAWSSecretsClient(mock)
	require.NoError(t, clientErr)

	creds, err := client.GetM2MCredentials(context.Background(), "staging", "org-1", "matcher", "fetcher")
	assert.Nil(t, creds)
	require.Error(t, err)
	assert.ErrorIs(t, err, m2m.ErrM2MVaultAccessDenied)
}

func TestNewAWSSecretsClient_NilClient_ReturnsError(t *testing.T) {
	t.Parallel()

	client, err := m2m.NewAWSSecretsClient(nil)
	assert.Nil(t, client)
	require.Error(t, err)
	assert.ErrorIs(t, err, m2m.ErrAWSSecretsClientNil)
}
