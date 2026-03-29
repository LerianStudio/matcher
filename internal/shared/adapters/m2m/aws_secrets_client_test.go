//go:build unit

package m2m_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssm "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/shared/adapters/m2m"
)

// mockAWSSecretsManager implements AWSSecretsManagerAPI for testing.
type mockAWSSecretsManager struct {
	output *awssm.GetSecretValueOutput
	err    error
}

func (m *mockAWSSecretsManager) GetSecretValue(
	_ context.Context,
	_ *awssm.GetSecretValueInput,
	_ ...func(*awssm.Options),
) (*awssm.GetSecretValueOutput, error) {
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

	client := m2m.NewAWSSecretsClient(mock)

	creds, err := client.GetM2MCredentials(context.Background(), "staging", "org-1", "matcher", "fetcher")
	require.NoError(t, err)
	assert.Equal(t, "cid-123", creds.ClientID)
	assert.Equal(t, "csecret-456", creds.ClientSecret)
}

func TestAWSSecretsClient_GetM2MCredentials_NotFound(t *testing.T) {
	t.Parallel()

	mock := &mockAWSSecretsManager{
		err: &smtypes.ResourceNotFoundException{
			Message: aws.String("secret not found"),
		},
	}

	client := m2m.NewAWSSecretsClient(mock)

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

	client := m2m.NewAWSSecretsClient(mock)

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

	client := m2m.NewAWSSecretsClient(mock)

	creds, err := client.GetM2MCredentials(context.Background(), "staging", "org-1", "matcher", "fetcher")
	assert.Nil(t, creds)
	require.Error(t, err)
	assert.True(t, errors.Is(err, m2m.ErrM2MInvalidCredentials))
}

func TestAWSSecretsClient_GetM2MCredentials_InvalidJSON(t *testing.T) {
	t.Parallel()

	mock := &mockAWSSecretsManager{
		output: &awssm.GetSecretValueOutput{
			SecretString: aws.String("not json"),
		},
	}

	client := m2m.NewAWSSecretsClient(mock)

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

	client := m2m.NewAWSSecretsClient(mock)

	creds, err := client.GetM2MCredentials(context.Background(), "staging", "org-1", "matcher", "fetcher")
	assert.Nil(t, creds)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "network timeout")
	assert.False(t, errors.Is(err, m2m.ErrM2MCredentialsNotFound))
}
