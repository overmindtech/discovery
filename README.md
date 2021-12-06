# SDP Discovery Libraries

[![Go Reference](https://pkg.go.dev/badge/github.com/overmindtech/discovery.svg)](https://pkg.go.dev/github.com/overmindtech/discovery) [![Tests](https://github.com/overmindtech/discovery/actions/workflows/test.yml/badge.svg)](https://github.com/overmindtech/discovery/actions/workflows/test.yml)

Code to help with all things related to discovering system state using [State Description Protocol](github.com/overmindtech/sdp). Allows users to easily create software that discovers system state, for example:

* Source containers for running with [srcman](https://github.com/overmindtech/srcman)
* Agents for discovering local state on servers and other devices

This library is currently under development and documentation can be found on [pkg.go.dev](https://pkg.go.dev/github.com/overmindtech/discovery)

## Engine

The engine is responsible for managing all communication over NATS, handling requests, reporting on progress, caching etc. Authors of sources should only need to do the following in order to have a functional source:

* Give the engine a name
    * Note that this name is used as the `Responder` when responding to requests, this means that this name should be unique as if there are multiple responders with the same name, users will not be able to properly track the progress of their requests
* Provide the engine with config
* Manage the engine's lifecycle (start and stop it)

Look at the tests for some simple examples of starting and running an engine, or use the [source-template](https://github.com/overmindtech/source-template) to generate the required wrapper code.

## Triggers

Triggers allow source developers to have their source be triggered by the discover of other items on the NATS network. This allows for a pattern where a source is triggered by a relevant resource being discovered by another query, rather than by being queried directly. This can be used to write secondary sources that fire automatically e.g.

> When a package with the name "nginx" is found in any context, the source should be triggered to try to find the config file for nginx in this context, parse it, and return more detailed information.

The anatomy of a trigger is as follows:

```go
var trigger = Trigger{
    // The type of item that this trigger should fire for
    Type:                      "person",
    // The trigger will only fire if both the type and the
    // UniqueAttributeValueRegex match
    UniqueAttributeValueRegex: regexp.MustCompile(`^[Dd]ylan$`),
    // When both of the above match, the below function will be called, this
    // function should return the request that should be forwarded to the
    // engine that the trigger is registered with
    RequestGenerator: func(in *sdp.Item) (*sdp.ItemRequest, error) {
        if in.GetContext() != "something" {
            return nil, errors.New("only 'something' context supported")
        } else {
            return &sdp.ItemRequest{
                Type:   "dog",
                Method: sdp.RequestMethod_SEARCH,
                Query:  "pug",
            }, nil
        }
    },
}
```

When the above trigger fires it will result in the engine that it is assigned to processing a SEARCH request as defined above. Note that while only the `Type`, `Method` and `Query` attributes have been specified, the rest will be filled in automatically with data from the `Metadata.SourceRequest` of the originating item to ensure that the responses are sent to the user that originated the request.

## Developing

This repository is configured to us [VSCode devcontainers](https://code.visualstudio.com/docs/remote/containers). This means that if you don't want to install Go locally, you can do all of your development inside a container. You can also use Github codespaces to host these containers meaning that the only requirement is having VSCode installed. Use of this is optional but does have some benefits:

* Local environment not polluted
* NATS sidecar container automatically started for end-to-end tests
