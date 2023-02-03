# Tardigrade: Worker Interface & Implementation

[![Build Status](https://github.com/slowli/tardigrade/workflows/CI/badge.svg?branch=main)](https://github.com/slowli/tardigrade/actions)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/License-MIT%2FApache--2.0-blue)](https://github.com/slowli/tardigrade#license)
![rust 1.66+ required](https://img.shields.io/badge/rust-1.66+-blue.svg?label=Required%20Rust)

**Documentation:**
[![crate docs (main)](https://img.shields.io/badge/main-yellow.svg?label=docs)](https://slowli.github.io/tardigrade/tardigrade_worker/)

This crate provides API declarations and implementation of *workers*
for external tasks in the [Tardigrade runtime].

A worker represents a piece of functionality external to Tardigrade workflows.
A workflow can connect to the worker by referencing the worker by name 
in the interface specification.
A single worker can serve multiple workflows; i.e., it can be thought of as a server
in the client-server model, with workflows acting as clients. So far, workers
only implement the request-response communication pattern. Similar to modern RPC
protocols (e.g., gRPC), communication is non-blocking; multiple requests may be simultaneously
in flight. As with other Tardigrade components,
passing messages via channels leads to lax worker availability requirements; a worker
does not need to be highly available or work without failures.

## Usage

Add this to your `Crate.toml`:

```toml
[dependencies]
tardigrade-worker = "0.1.0"
```

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE)
or [MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in `tardigrade` by you, as defined in the Apache-2.0 license,
shall be dual licensed as above, without any additional terms or conditions.

<!-- TODO: replace with crates.io links before publishing -->
[Tardigrade runtime]: ../rt
