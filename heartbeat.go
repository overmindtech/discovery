package discovery

import (
	"context"
	"errors"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/overmindtech/sdp-go"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/durationpb"
)

const DefaultHeartbeatFrequency = 5 * time.Minute

var ErrNoHealthcheckDefined = errors.New("no healthcheck defined")

// HeartbeatSender sends a heartbeat to the management API, this is called at
// `DefaultHeartbeatFrequency` by default when the engine is running, or
// `StartSendingHeartbeats` has been called manually. Users can also call this
// method to immediately send a heartbeat if required
func (e *Engine) SendHeartbeat(ctx context.Context) error {
	if e.HeartbeatOptions.HealthCheck == nil {
		return ErrNoHealthcheckDefined
	}

	healthCheckError := e.HeartbeatOptions.HealthCheck()

	var heartbeatError *string

	if healthCheckError != nil {
		heartbeatError = new(string)
		*heartbeatError = healthCheckError.Error()
	}

	var engineUUID []byte

	if e.UUID != uuid.Nil {
		engineUUID = e.UUID[:]
	}

	// Get available types and scopes
	availableTypesMap := make(map[string]bool)
	availableScopesMap := make(map[string]bool)
	for _, source := range e.sh.VisibleSources() {
		availableTypesMap[source.Type()] = true
		for _, scope := range source.Scopes() {
			availableScopesMap[scope] = true
		}
	}

	// Extract slices from maps
	availableTypes := make([]string, 0)
	availableScopes := make([]string, 0)
	for t := range availableTypesMap {
		availableTypes = append(availableTypes, t)
	}
	for s := range availableScopesMap {
		availableScopes = append(availableScopes, s)
	}

	// Calculate the duration for the next heartbeat, based on the current
	// frequency x2.5 to give us some leeway
	nextHeartbeat := time.Duration(float64(e.HeartbeatOptions.Frequency) * 2.5)

	_, err := e.HeartbeatOptions.ManagementClient.SubmitSourceHeartbeat(ctx, &connect.Request[sdp.SubmitSourceHeartbeatRequest]{
		Msg: &sdp.SubmitSourceHeartbeatRequest{
			UUID:             engineUUID,
			Version:          e.Version,
			Name:             e.Name,
			Type:             e.Type,
			AvailableTypes:   availableTypes,
			AvailableScopes:  availableScopes,
			Managed:          e.Managed,
			Error:            heartbeatError,
			NextHeartbeatMax: durationpb.New(nextHeartbeat),
		},
	})

	return err
}

// Starts sending heartbeats at the specified frequency. These will be sent in
// the background and this function will return immediately. Heartbeats are
// automatically started when the engine started, but if a sources has startup
// steps that take a long time, or are liable to fail, the user may want to
// start the heartbeats first so that users can see that the source has failed
// to start.
//
// If this is called multiple times, nothing will happen. Heartbeats will be
// stopped when the engine is stopped, or when the provided context is canceled.
func (e *Engine) StartSendingHeartbeats(ctx context.Context) {
	if e.HeartbeatOptions == nil || e.HeartbeatOptions.Frequency == 0 || e.heartbeatContext != nil {
		return
	}

	e.heartbeatContext, e.heartbeatCancel = context.WithCancel(ctx)

	go func() {
		ticker := time.NewTicker(e.HeartbeatOptions.Frequency)
		defer ticker.Stop()

		for {
			select {
			case <-e.heartbeatContext.Done():
				return
			case <-ticker.C:
				err := e.SendHeartbeat(e.heartbeatContext)
				if err != nil {
					log.WithError(err).Error("Failed to send heartbeat")
				}
			}
		}
	}()
}
