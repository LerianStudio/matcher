//go:build unit

package value_objects

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfidenceScore_ParseConfidenceScore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   int
		wantErr error
	}{
		{"valid zero", 0, nil},
		{"valid mid", 50, nil},
		{"valid max", 100, nil},
		{"invalid negative", -1, ErrConfidenceScoreOutOfRange},
		{"invalid over max", 101, ErrConfidenceScoreOutOfRange},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cs, err := ParseConfidenceScore(tt.value)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.value, cs.Value())
			}
		})
	}
}

func TestConfidenceScore_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		score ConfidenceScore
		want  bool
	}{
		{"zero value struct is valid", ConfidenceScore{}, true},
		{"score 0 is valid", ConfidenceScore{value: 0}, true},
		{"score 50 is valid", ConfidenceScore{value: 50}, true},
		{"score 100 is valid", ConfidenceScore{value: 100}, true},
		{"score -1 is invalid", ConfidenceScore{value: -1}, false},
		{"score 101 is invalid", ConfidenceScore{value: 101}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.score.IsValid())
		})
	}
}

func TestConfidenceScore_MarshalJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		score ConfidenceScore
		want  string
	}{
		{"zero value", ConfidenceScore{value: 0}, "0"},
		{"mid value", ConfidenceScore{value: 50}, "50"},
		{"max value", ConfidenceScore{value: 100}, "100"},
		{"typical value", ConfidenceScore{value: 85}, "85"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			data, err := json.Marshal(tt.score)
			require.NoError(t, err)
			assert.Equal(t, tt.want, string(data))
		})
	}
}

func TestConfidenceScore_UnmarshalJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		json      string
		want      int
		wantErr   bool
		wantErrIs error
	}{
		{"zero value", "0", 0, false, nil},
		{"mid value", "50", 50, false, nil},
		{"max value", "100", 100, false, nil},
		{"typical value", "85", 85, false, nil},
		{"out of range negative", "-1", 0, true, ErrConfidenceScoreOutOfRange},
		{"out of range high", "101", 0, true, ErrConfidenceScoreOutOfRange},
		{"invalid json", "\"abc\"", 0, true, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var score ConfidenceScore

			err := json.Unmarshal([]byte(tt.json), &score)
			if tt.wantErr {
				require.Error(t, err)

				if tt.wantErrIs != nil {
					require.ErrorIs(t, err, tt.wantErrIs)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, score.Value())
			}
		})
	}
}

func TestConfidenceScore_JSONRoundTrip(t *testing.T) {
	t.Parallel()

	score, err := ParseConfidenceScore(75)
	require.NoError(t, err)

	data, err := json.Marshal(score)
	require.NoError(t, err)
	assert.Equal(t, "75", string(data))

	var unmarshaled ConfidenceScore

	err = json.Unmarshal(data, &unmarshaled)
	require.NoError(t, err)
	assert.Equal(t, score.Value(), unmarshaled.Value())
}
