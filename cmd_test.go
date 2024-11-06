package discovery

import (
	"os"
	"runtime"
	"testing"

	"github.com/google/uuid"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngineConfigFromViper(t *testing.T) {
	tests := []struct {
		name                string
		setupViper          func()
		engineType          string
		version             string
		expectedSourceName  string
		expectedSourceUUID  uuid.UUID
		expectedApp         string
		expectedApiKey      string
		expectedMaxParallel int
		expectError         bool
	}{
		{
			name: "default values",
			setupViper: func() {
				viper.Reset()
				viper.Set("app", "https://app.overmind.tech")
			},
			engineType:          "test-engine",
			version:             "1.0",
			expectedSourceName:  "test-engine-" + getHostname(t),
			expectedSourceUUID:  uuid.Nil,
			expectedApp:         "https://app.overmind.tech",
			expectedApiKey:      "",
			expectedMaxParallel: runtime.NumCPU(),
			expectError:         false,
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
			engineType:          "test-engine",
			version:             "1.0",
			expectedSourceName:  "custom-source",
			expectedSourceUUID:  uuid.MustParse("123e4567-e89b-12d3-a456-426614174000"),
			expectedApp:         "https://custom.app",
			expectedApiKey:      "custom-api-key",
			expectedMaxParallel: 10,
			expectError:         false,
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