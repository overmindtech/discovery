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
	if e.HeartbeatOptions == nil || e.HeartbeatOptions.HealthCheck == nil {
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
	availableScopesMap := make(map[string]bool)
	adapterMetadata := []*sdp.AdapterMetadata{}
	for _, adapter := range e.sh.VisibleAdapters() {
		for _, scope := range adapter.Scopes() {
			availableScopesMap[scope] = true
		}
		metaData := adapter.Metadata()
		adapterMetadata = append(adapterMetadata, &metaData)
	}

	// Extract slices from maps
	availableScopes := make([]string, 0)
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
			AvailableScopes:  availableScopes,
			AdapterMetadata:  adapterMetadata,
			Managed:          e.Managed,
			Error:            heartbeatError,
			NextHeartbeatMax: durationpb.New(nextHeartbeat),
		},
	})

	return err
}

// Starts sending heartbeats at the specified frequency. These will be sent in
// the background and this function will return immediately. Heartbeats are
// automatically started when the engine started, but if an adapter has startup
// steps that take a long time, or are liable to fail, the user may want to
// start the heartbeats first so that users can see that the adapter has failed
// to start.
//
// If this is called multiple times, nothing will happen. Heartbeats will be
// stopped when the engine is stopped, or when the provided context is canceled.
//
// This will send one heartbeat initially when the method is called, and will
// then run in a background goroutine that sends heartbeats at the specified
// frequency, and will stop when the provided context is canceled.
func (e *Engine) StartSendingHeartbeats(ctx context.Context) {
	if e.HeartbeatOptions == nil || e.HeartbeatOptions.Frequency == 0 || e.heartbeatCancel != nil {
		return
	}

	var heartbeatContext context.Context
	heartbeatContext, e.heartbeatCancel = context.WithCancel(ctx)

	// Send one heartbeat at the beginning
	err := e.SendHeartbeat(heartbeatContext)
	if err != nil {
		log.WithError(err).Error("Failed to send heartbeat")
	}

	go func() {
		ticker := time.NewTicker(e.HeartbeatOptions.Frequency)
		defer ticker.Stop()

		for {
			select {
			case <-heartbeatContext.Done():
				return
			case <-ticker.C:
				err := e.SendHeartbeat(heartbeatContext)
				if err != nil {
					log.WithError(err).Error("Failed to send heartbeat")
				}
			}
		}
	}()
}
