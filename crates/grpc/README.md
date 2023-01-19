# Tardigrade: gRPC Bindings

[![Build Status](https://github.com/slowli/tardigrade/workflows/CI/badge.svg?branch=main)](https://github.com/slowli/tardigrade/actions)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/License-MIT%2FApache--2.0-blue)](https://github.com/slowli/tardigrade#license)
![rust 1.65+ required](https://img.shields.io/badge/rust-1.65+-blue.svg?label=Required%20Rust)

**Documentation:**
[![crate docs (main)](https://img.shields.io/badge/main-yellow.svg?label=docs)](https://slowli.github.io/tardigrade/tardigrade_grpc/)

This crate provides gRPC services encapsulating the [Tardigrade runtime]
powered by the [`tonic`] framework.
This allows launching a runtime alongside with other services or interceptors
(authentication, tracing etc.).

The functionality supported by the services exported from this crate covers most of what
the Tardigrade runtime is capable of:

- **Module operations:** deploying modules, listing deployed modules
- **Workflow operations:** creating workflows, getting information about a workflow,
  ticking a workflow
- **Channel operations:** creating channels, getting information about a channel,
  receiving and sending messages from the channels
- **Test extensions:** setting the current time in a mock scheduler

See the [`tardigrade-cli`] crate as a higher-level alternative.

## Usage

Add this to your `Crate.toml`:

```toml
[dependencies]
tardigrade-grpc = "0.1.0"
```

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE)
or [MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in `tardigrade` by you, as defined in the Apache-2.0 license,
shall be dual licensed as above, without any additional terms or conditions.

[`tonic`]: https://crates.io/crates/tonic
<!-- TODO: replace with crates.io links before publishing -->
[Tardigrade runtime]: ../rt
[`tardigrade-cli`]: ../cli
