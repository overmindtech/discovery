package sources

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/spf13/viper"
)

// Backend is an interface that all sources should implement.
//
// It sets out the basic functions that a source is required to perform and
// allows sources to be stored in a variable with a reasonable type.
type Backend interface {
	ItemSource

	// Type is the type of items that this backend is responsible for
	// discovering
	Type() string

	// BackendPackage is the name of the backend package that this backend came
	// from. This will be used in metadata
	BackendPackage() string

	// Backends need to be able to tell us if they are threadsafe or not
	// Backends that marks themsalves as not being threadsafe will be locked
	// with a mutex before they are called to ensure that they aren't accessed
	// in parallel. Note however that you were to create two source objects
	// with the same type, then register the same backend with each of them,
	// you would end up with two mutexes and therefore they wouldn't be
	// threadsafe anymore
	Threadsafe() bool
}

// ConditionallySupportedBackend Represents a backend that can tell whether it
// is supported or not on this platform. If a backend doesn't implement this
// then it's assumened that it is always supported
type ConditionallySupportedBackend interface {
	Supported() bool
}

// CacheDefiner Some backends may implement the CacheDefiner interface which
// means that they specify a custom default cache interval. The config will
// always take precedence however
type CacheDefiner interface {
	DefaultCacheDuration() time.Duration
}

// PriorityDefiner A backend that satisfies this interface is able to decide its
// own default priority
type PriorityDefiner interface {
	DefaultPriority() int
}

// Contextual represents an object that knows about the context it is related
// to. This will be used for backends so that they can define a context other
// than the "local" context if they choose
type Contextual interface {
	Context() string
}

// BackendList is just a list of Backends
type BackendList []*BackendInfo

// BackendInfo contains both the backend itself and required metadata
type BackendInfo struct {
	Backend       Backend
	Priority      int
	CacheDuration time.Duration
	Context       string
	Cache         Cache
	mux           sync.Mutex
}

var localContext string

// LocalContext returns the context of the local machine that this is being run
// on, this defaults to the hostname. If the hostname cannot be found it will
// use the UnknownContext()
func LocalContext() string {
	// Only read the hostname from the system once, it's not likely to change...
	if localContext == "" {
		var err error

		localContext, err = os.Hostname()

		if err != nil {
			localContext = UnknownContext()
		}
	}

	return localContext
}

// UnknownContext returns a string representing the unknown context
func UnknownContext() string {
	return "unknown"
}

// Config bindings for Viper
// The idea here is that the sources will be able call these to get wherever
// config they need. It would be really good though if we could have some good
// way of ensure that the config doesn't get out of control, or at least some
// way of doing automatic documentation, like maybe if the backends were to
// register the config they wanted or something.. Not sure...
//
// TODO: See if I can implement automatic documentation of the settings

// ConfigGet gets the raw value of a piece of config
func ConfigGet(key string, backendPackage string) interface{} {
	return viper.Get(configLocation(key, backendPackage))
}

// ConfigGetBool gets the value of a given config item as a bool
func ConfigGetBool(key string, backendPackage string) bool {
	return viper.GetBool(configLocation(key, backendPackage))
}

// ConfigGetFloat64 gets the value of a given config item as a float64
func ConfigGetFloat64(key string, backendPackage string) float64 {
	return viper.GetFloat64(configLocation(key, backendPackage))
}

// ConfigGetInt gets the value of a given config item as a int
func ConfigGetInt(key string, backendPackage string) int {
	return viper.GetInt(configLocation(key, backendPackage))
}

// ConfigGetIntSlice gets the value of a given config item as a []int
func ConfigGetIntSlice(key string, backendPackage string) []int {
	return viper.GetIntSlice(configLocation(key, backendPackage))
}

// ConfigGetString gets the value of a given config item as a string
func ConfigGetString(key string, backendPackage string) string {
	return viper.GetString(configLocation(key, backendPackage))
}

// ConfigGetStringMap gets the value of a given config item as a
// map[string]interface{}
func ConfigGetStringMap(key string, backendPackage string) map[string]interface{} {
	return viper.GetStringMap(configLocation(key, backendPackage))
}

// ConfigGetStringMapString gets the value of a given config item as a
// map[string]string
func ConfigGetStringMapString(key string, backendPackage string) map[string]string {
	return viper.GetStringMapString(configLocation(key, backendPackage))
}

// ConfigGetStringSlice gets the value of a given config item as a []string
func ConfigGetStringSlice(key string, backendPackage string) []string {
	return viper.GetStringSlice(configLocation(key, backendPackage))
}

// ConfigGetTime gets the value of a given config item as a time.Time
func ConfigGetTime(key string, backendPackage string) time.Time {
	return viper.GetTime(configLocation(key, backendPackage))
}

// ConfigGetDuration gets the value of a given config item as a time.Duration
func ConfigGetDuration(key string, backendPackage string) time.Duration {
	return viper.GetDuration(configLocation(key, backendPackage))
}

// ConfigIsSet checks if a config item is set
func ConfigIsSet(key string, backendPackage string) bool {
	return viper.IsSet(configLocation(key, backendPackage))
}

// ConfigAllSettings gets all settings
func ConfigAllSettings() map[string]interface{} {
	return viper.AllSettings()
}

// configLocation Returns the standard config location
func configLocation(key string, backendPackage string) string {
	return fmt.Sprintf(
		"backends.%v.%v",
		backendPackage,
		key,
	)
}
