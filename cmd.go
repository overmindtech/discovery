package discovery

import (
	"fmt"
	"os"
	"runtime"

	"github.com/google/uuid"
	"github.com/overmindtech/sdp-go"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func AddEngineFlags(command *cobra.Command) {
	command.PersistentFlags().String("source-name", "", "The name of the source")
	cobra.CheckErr(viper.BindEnv("source-name", "SOURCE_NAME"))
	command.PersistentFlags().String("source-uuid", "", "The UUID of the source, is this is blank it will be auto-generated. This is used in heartbeats and shouldn't be supplied usually")
	cobra.CheckErr(viper.BindEnv("source-uuid", "SOURCE_UUID"))
	command.PersistentFlags().String("app", "https://app.overmind.tech", "The URL of the Overmind app to use")
	cobra.CheckErr(viper.BindEnv("app", "APP"))
	command.PersistentFlags().String("api-key", "", "The API key to use to authenticate to the Overmind API")
	cobra.CheckErr(viper.BindEnv("api-key", "OVM_API_KEY", "API_KEY"))
	command.PersistentFlags().String("source-access-token", "", "The access token to use to authenticate the source for managed sources")
	cobra.CheckErr(viper.BindEnv("source-access-token", "SOURCE_ACCESS_TOKEN"))
	// SOURCE_TOKEN_TYPE
	command.PersistentFlags().String("source-token-type", "", "The type of token to use to authenticate the source for managed sources")
	cobra.CheckErr(viper.BindEnv("source-token-type", "SOURCE_TOKEN_TYPE"))
	command.PersistentFlags().Int("max-parallel", 0, "The maximum number of parallel executions")
	cobra.CheckErr(viper.BindEnv("max-parallel", "MAX_PARALLEL"))
}

func EngineConfigFromViper(engineType, version string) (*EngineConfig, error) {
	var sourceName string
	if viper.GetString("source-name") == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, fmt.Errorf("error getting hostname: %w", err)
		}
		sourceName = fmt.Sprintf("%s-%s", engineType, hostname)
	} else {
		sourceName = viper.GetString("source-name")
	}

	sourceUUIDString := viper.GetString("source-uuid")
	var sourceUUID uuid.UUID
	if sourceUUIDString == "" {
		sourceUUID = uuid.New()
	} else {
		var err error
		sourceUUID, err = uuid.Parse(sourceUUIDString)
		if err != nil {
			return nil, fmt.Errorf("error parsing source-uuid: %w", err)
		}
	}

	// checks for  source-access-token and api-key, if both set error, if none set error
	if viper.GetString("source-access-token") != "" && viper.GetString("api-key") != "" {
		return nil, fmt.Errorf("source-access-token and api-key cannot be set at the same time")
	}
	if viper.GetString("source-access-token") == "" && viper.GetString("api-key") == "" {
		return nil, fmt.Errorf("source-access-token or api-key must be set")
	}

	var managedSource sdp.SourceManaged
	if viper.GetString("source-access-token") != "" {
		managedSource = sdp.SourceManaged_MANAGED
	} else {
		managedSource = sdp.SourceManaged_LOCAL
	}

	maxParallelExecutions := viper.GetInt("max-parallel")
	if maxParallelExecutions == 0 {
		maxParallelExecutions = runtime.NumCPU()
	}

	return &EngineConfig{
		EngineType:            engineType,
		Version:               version,
		SourceName:            sourceName,
		SourceUUID:            sourceUUID,
		SourceAccessToken:     viper.GetString("source-access-token"),
		SourceTokenType:       viper.GetString("source-token-type"),
		OvermindManagedSource: managedSource,
		App:                   viper.GetString("app"),
		ApiKey:                viper.GetString("api-key"),
		MaxParallelExecutions: maxParallelExecutions,
	}, nil
}

// MapFromEngineConfig Returns the config as a map
func MapFromEngineConfig(c *EngineConfig) map[string]any {

	var apiKeyClientSecret string

	if c.ApiKey != "" {
		apiKeyClientSecret = "[REDACTED]"
	}

	return map[string]interface{}{
		"engine-type":             c.EngineType,
		"version":                 c.Version,
		"source-name":             c.SourceName,
		"source-uuid":             c.SourceUUID,
		"app":                     c.App,
		"api-key":                 apiKeyClientSecret,
		"max-parallel-executions": c.MaxParallelExecutions,
	}
}
