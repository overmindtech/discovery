# SDP Discovery Libraries

[![Go Reference](https://pkg.go.dev/badge/github.com/overmindtech/discovery.svg)](https://pkg.go.dev/github.com/overmindtech/discovery)

Code to help with all things related to discovering system state using [State Description Protocol](github.com/overmindtech/sdp). Allows users to easily create software that discovers system state, for example:

* Source containers for running with [srcman](https://github.com/overmindtech/srcman)
* Agents for discovering local state on servers and other devices

This library is currently under development and documentation can be found on [pkg.go.dev](https://pkg.go.dev/github.com/overmindtech/discovery)

## Developing

This repository is configured to us [VSCode devcontainers](https://code.visualstudio.com/docs/remote/containers). This means that if you don't want to install Go locally, you can do all of your development inside a container. You can also use Github codespaces to host these containers meaning that the only requirement is having VSCode installed. Use of this is optional but does have some benefits:

* Local environment not polluted
* NATS sidecar container automatically started for end-to-end tests
