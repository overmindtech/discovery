package discovery

import (
	"fmt"
	"regexp"

	"github.com/google/uuid"
	"github.com/overmindtech/sdp-go"
)

// Trigger defines a trigger that will send an ItemRequest to the engine of the
// conditions are met
type Trigger struct {
	// The item type that the trigger should activate on
	Type string

	// A regexp that will be used to filter items based on their
	// UniqueAttributeValue. If this is not supplied, the trigger will fire for
	// all items whose type matches
	UniqueAttributeValueRegex *regexp.Regexp

	// A function that will be run when a matching item is found. This should
	// return an ItemRequest and an error, if the error is nil, the ItemRequest
	// will be sent to the engine. Note that the returned ItemRequest only needs
	// to contain the following fields, the rest will be set automatically if
	// not provided:
	//
	// * Type
	// * Method
	// * Query
	//
	RequestGenerator func(in *sdp.Item) (*sdp.ItemRequest, error)
}

// ProcessItem Processes an item to see if the trigger should fire. If the error
// returned is nil, the returned item request should be sent to the engine. Any
// non-nil error means that the trigger has not fired
func (t *Trigger) ProcessItem(i *sdp.Item) (*sdp.ItemRequest, error) {
	if !t.shouldFire(i) {
		return nil, fmt.Errorf("item %v did not match trigger %v.%v", i.GloballyUniqueName(), t.Type, t.UniqueAttributeValueRegex)
	}

	req, err := t.RequestGenerator(i)

	if err != nil {
		return nil, err
	}

	// We can only fill the follwing fields if the item has an ItemRequest and
	// metadata, otherwise we should leave them as they are
	if i.Metadata != nil && i.Metadata.SourceRequest != nil {
		// If the values have not been set, copy their values from the source
		// request
		if req.LinkDepth == uint32(0) {
			req.LinkDepth = i.Metadata.SourceRequest.LinkDepth - 1
		}

		if !req.IgnoreCache {
			req.IgnoreCache = i.Metadata.SourceRequest.IgnoreCache
		}

		if req.Timeout == nil {
			req.Timeout = i.Metadata.SourceRequest.Timeout
		}

		if req.ItemSubject == "" {
			req.ItemSubject = i.Metadata.SourceRequest.ItemSubject
		}

		if req.ResponseSubject == "" {
			req.ResponseSubject = i.Metadata.SourceRequest.ResponseSubject
		}
	}

	if req.Context == "" {
		req.Context = i.Context
	}

	if req.UUID == nil {
		u := uuid.New()
		req.UUID = u[:]
	}

	return req, nil
}

// shouldFire Returns true if the given item should cause this trigger to fire
func (t *Trigger) shouldFire(item *sdp.Item) bool {
	switch t.Type {
	case "":
		// If this trigger has no type, never fire
		return false
	case item.Type:
		if t.UniqueAttributeValueRegex == nil {
			// If there is no regex, always fire
			return true
		}

		return t.UniqueAttributeValueRegex.MatchString(item.UniqueAttributeValue())
	default:
		// If the type does not match, don't fire
		return false
	}
}
