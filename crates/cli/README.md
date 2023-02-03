# Tardigrade: gRPC Server

[![Build Status](https://github.com/slowli/tardigrade/workflows/CI/badge.svg?branch=main)](https://github.com/slowli/tardigrade/actions)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/License-MIT%2FApache--2.0-blue)](https://github.com/slowli/tardigrade#license)
![rust 1.66+ required](https://img.shields.io/badge/rust-1.66+-blue.svg?label=Required%20Rust)

This crate provides a gRPC server for the [Tardigrade runtime].
It builds on top of [`tardigrade-grpc`] and supports the same set of functionality.
The server also includes the gRPC reflection service (thus enabling the use
of tools like [`grpcurl`] or [`grpcui`]) and the end-to-end tracing of gRPC requests.

The functionality supported by the server covers most of what
the Tardigrade runtime is capable of:

- **Module operations:** deploying modules, listing deployed modules
- **Workflow operations:** creating workflows, getting information about a workflow,
  ticking a workflow
- **Channel operations:** creating channels, getting information about a channel,
  receiving and sending messages from the channels
- **Test extensions:** setting the current time in a mock scheduler

See the [`tardigrade-grpc`] crate as a lower-level alternative.

## Installation

Install with

```shell
cargo install --locked tardigrade-cli
# This will install `tardigrade-grpc` executable, which can be checked
# as follows:
tardigrade-grpc --help
```

<!-- TODO: reference Docker image once it's available -->

## Usage

The server can be launched with a command like

```shell
tardigrade-grpc 127.0.0.1:9000
```

where `127.0.0.1:9000` is a socket address to bind the server to.

Additional run options include:

- `--mock`: Use a mock timer / scheduler instead of a real one.
  The time can be advanced manually by calling the `SetTime` endpoint
  of the test gRPC service (`tardigrade.v0.TestService`).
- `--no-driver`: Do not drive workflows to completion when timers expire
  or new inbound messages arrive. This option is mostly useful for testing
  together with `--mock`. Workflows can be driven manually by calling
  the `TickWorkflow` endpoint of the main service (`tardigrade.v0.RuntimeService`).

Launch the CLI app with the `--help` option for more details about arguments.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE)
or [MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in `tardigrade` by you, as defined in the Apache-2.0 license,
shall be dual licensed as above, without any additional terms or conditions.

<!-- TODO: replace with crates.io links before publishing -->
[Tardigrade runtime]: ../rt
[`tardigrade-grpc`]: ../grpc
[`grpcurl`]: https://github.com/fullstorydev/grpcurl
[`grpcui`]: https://github.com/fullstorydev/grpcui
