// Copyright 2025 Lerian Studio.

package secretcodec

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
)

const envelopeVersion = 1

var (
	ErrMasterKeyRequired = errors.New("systemplane secret codec: master key is required")
	ErrWeakMasterKey     = errors.New("systemplane secret codec: master key must be 32 raw bytes or base64-encoded 32 bytes")
	ErrDecryptFailed     = errors.New("systemplane secret codec: decrypt failed")
	ErrInvalidEnvelope   = errors.New("systemplane secret codec: invalid encrypted envelope")
)

type Codec struct {
	aead       cipher.AEAD
	secretKeys map[string]struct{}
}

type envelope struct {
	Version    int    `json:"__systemplane_secret_v" bson:"__systemplane_secret_v"`
	Algorithm  string `json:"alg" bson:"alg"`
	Nonce      string `json:"nonce" bson:"nonce"`
	Ciphertext string `json:"ciphertext" bson:"ciphertext"`
}

func New(masterKey string, secretKeys []string) (*Codec, error) {
	if len(secretKeys) == 0 {
		return nil, nil
	}
	if strings.TrimSpace(masterKey) == "" {
		return nil, ErrMasterKeyRequired
	}

	keyBytes, err := deriveKey(masterKey)
	if err != nil {
		return nil, err
	}

	block, err := aes.NewCipher(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create gcm: %w", err)
	}

	keySet := make(map[string]struct{}, len(secretKeys))
	for _, key := range secretKeys {
		if key == "" {
			continue
		}
		keySet[key] = struct{}{}
	}

	return &Codec{aead: aead, secretKeys: keySet}, nil
}

func (codec *Codec) Encrypt(target domain.Target, key string, value any) (any, error) {
	if !codec.isSecretKey(key) || value == nil {
		return value, nil
	}

	plaintext, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal plaintext: %w", err)
	}

	nonce := make([]byte, codec.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("generate nonce: %w", err)
	}

	ciphertext := codec.aead.Seal(nil, nonce, plaintext, additionalData(target, key))

	return envelope{
		Version:    envelopeVersion,
		Algorithm:  "aes-256-gcm",
		Nonce:      base64.StdEncoding.EncodeToString(nonce),
		Ciphertext: base64.StdEncoding.EncodeToString(ciphertext),
	}, nil
}

func (codec *Codec) Decrypt(target domain.Target, key string, value any) (any, error) {
	if !codec.isSecretKey(key) || value == nil {
		return value, nil
	}

	env, ok, err := parseEnvelope(value)
	if err != nil {
		return nil, err
	}
	if !ok {
		return value, nil
	}

	nonce, err := base64.StdEncoding.DecodeString(env.Nonce)
	if err != nil {
		return nil, fmt.Errorf("%w: decode nonce: %v", ErrInvalidEnvelope, err)
	}

	ciphertext, err := base64.StdEncoding.DecodeString(env.Ciphertext)
	if err != nil {
		return nil, fmt.Errorf("%w: decode ciphertext: %v", ErrInvalidEnvelope, err)
	}

	plaintext, err := codec.aead.Open(nil, nonce, ciphertext, additionalData(target, key))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDecryptFailed, err)
	}

	var decoded any
	if err := json.Unmarshal(plaintext, &decoded); err != nil {
		return nil, fmt.Errorf("unmarshal decrypted value: %w", err)
	}

	return decoded, nil
}

func (codec *Codec) isSecretKey(key string) bool {
	if codec == nil {
		return false
	}

	_, ok := codec.secretKeys[key]
	return ok
}

func parseEnvelope(value any) (envelope, bool, error) {
	if env, ok := value.(envelope); ok {
		return env, true, nil
	}

	object, ok := value.(map[string]any)
	if !ok {
		return envelope{}, false, nil
	}

	version, ok := object["__systemplane_secret_v"]
	if !ok {
		return envelope{}, false, nil
	}

	parsedVersion, ok := parseVersion(version)
	if !ok || parsedVersion != envelopeVersion {
		return envelope{}, false, ErrInvalidEnvelope
	}

	alg, _ := object["alg"].(string)
	nonce, _ := object["nonce"].(string)
	ciphertext, _ := object["ciphertext"].(string)
	if alg == "" || nonce == "" || ciphertext == "" {
		return envelope{}, false, ErrInvalidEnvelope
	}

	return envelope{Version: parsedVersion, Algorithm: alg, Nonce: nonce, Ciphertext: ciphertext}, true, nil
}

func parseVersion(value any) (int, bool) {
	switch typedValue := value.(type) {
	case float64:
		return int(typedValue), true
	case int:
		return typedValue, true
	case int32:
		return int(typedValue), true
	case int64:
		return int(typedValue), true
	default:
		return 0, false
	}
}

func deriveKey(masterKey string) ([]byte, error) {
	if decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(masterKey)); err == nil && len(decoded) == 32 {
		return decoded, nil
	}

	if raw := []byte(masterKey); len(raw) == 32 {
		return raw, nil
	}

	return nil, ErrWeakMasterKey
}

func additionalData(target domain.Target, key string) []byte {
	return []byte(string(target.Kind) + "|" + string(target.Scope) + "|" + target.SubjectID + "|" + key)
}
