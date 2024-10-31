package discovery

import (
	"os"
	"runtime"
	"testing"

	"github.com/google/uuid"
	"github.com/overmindtech/sdp-go"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngineConfigFromViper(t *testing.T) {
	tests := []struct {
		name                      string
		setupViper                func()
		engineType                string
		version                   string
		expectedSourceName        string
		expectedSourceUUID        uuid.UUID
		expectedSourceAccessToken string
		expectedSourceTokenType   string
		expectedManagedSource     sdp.SourceManaged
		expectedApp               string
		expectedApiKey            string
		expectedMaxParallel       int
		expectError               bool
	}{
		{
			name: "default values",
			setupViper: func() {
				viper.Reset()
				viper.Set("app", "https://app.overmind.tech")
				viper.Set("api-key", "api-key")
			},
			engineType:                "test-engine",
			version:                   "1.0",
			expectedSourceName:        "test-engine-" + getHostname(t),
			expectedSourceUUID:        uuid.Nil,
			expectedSourceAccessToken: "",
			expectedSourceTokenType:   "",
			expectedManagedSource:     sdp.SourceManaged_LOCAL,
			expectedApp:               "https://app.overmind.tech",
			expectedApiKey:            "api-key",
			expectedMaxParallel:       runtime.NumCPU(),
			expectError:               false,
		},
		{
			name: "custom values",
			setupViper: func() {
				viper.Reset()
				viper.Set("source-name", "custom-source")
				viper.Set("source-uuid", "123e4567-e89b-12d3-a456-426614174000")
				viper.Set("app", "https://custom.app")
				viper.Set("api-key", "custom-api-key")
				viper.Set("max-parallel", 10)
			},
			engineType:                "test-engine",
			version:                   "1.0",
			expectedSourceName:        "custom-source",
			expectedSourceUUID:        uuid.MustParse("123e4567-e89b-12d3-a456-426614174000"),
			expectedSourceAccessToken: "",
			expectedSourceTokenType:   "",
			expectedManagedSource:     sdp.SourceManaged_LOCAL,
			expectedApp:               "https://custom.app",
			expectedApiKey:            "custom-api-key",
			expectedMaxParallel:       10,
			expectError:               false,
		},
		{
			name: "invalid UUID",
			setupViper: func() {
				viper.Reset()
				viper.Set("source-uuid", "invalid-uuid")
			},
			engineType:  "test-engine",
			version:     "1.0",
			expectError: true,
		},
		{
			name: "custom values with source access token",
			setupViper: func() {
				viper.Reset()
				viper.Set("source-name", "custom-source")
				viper.Set("source-uuid", "123e4567-e89b-12d3-a456-426614174000")
				viper.Set("app", "https://custom.app")
				viper.Set("source-access-token", "custom-access-token")
				viper.Set("source-token-type", "custom-token-type")
				viper.Set("max-parallel", 10)
			},
			engineType:                "test-engine",
			version:                   "1.0",
			expectedSourceName:        "custom-source",
			expectedSourceUUID:        uuid.MustParse("123e4567-e89b-12d3-a456-426614174000"),
			expectedSourceAccessToken: "custom-access-token",
			expectedSourceTokenType:   "custom-token-type",
			expectedManagedSource:     sdp.SourceManaged_MANAGED,
			expectedApp:               "https://custom.app",
			expectedMaxParallel:       10,
			expectError:               false,
		},
		{
			name: "source access token and api key set",
			setupViper: func() {
				viper.Reset()
				viper.Set("source-access-token", "custom-access-token")
				viper.Set("api-key", "custom-api-key")
			},
			engineType:  "test-engine",
			version:     "1.0",
			expectError: true,
		},
		{
			name: "source access token and api key not set",
			setupViper: func() {
				viper.Reset()
			},
			engineType:  "test-engine",
			version:     "1.0",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupViper()
			config, err := EngineConfigFromViper(tt.engineType, tt.version)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.engineType, config.EngineType)
				assert.Equal(t, tt.version, config.Version)
				assert.Equal(t, tt.expectedSourceName, config.SourceName)
				if tt.expectedSourceUUID == uuid.Nil {
					assert.NotEqual(t, uuid.Nil, config.SourceUUID)
				} else {
					assert.Equal(t, tt.expectedSourceUUID, config.SourceUUID)
				}
				assert.Equal(t, tt.expectedSourceAccessToken, config.SourceAccessToken)
				assert.Equal(t, tt.expectedSourceTokenType, config.SourceTokenType)
				assert.Equal(t, tt.expectedManagedSource, config.OvermindManagedSource)
				assert.Equal(t, tt.expectedApp, config.App)
				assert.Equal(t, tt.expectedApiKey, config.ApiKey)
				assert.Equal(t, tt.expectedMaxParallel, config.MaxParallelExecutions)
			}
		})
	}
}

func getHostname(t *testing.T) string {
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatalf("error getting hostname: %v", err)
	}
	return hostname
}
