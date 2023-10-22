# Tardigrade: WASM workflow automation engine

[![Build Status](https://github.com/slowli/tardigrade/workflows/CI/badge.svg?branch=main)](https://github.com/slowli/tardigrade/actions)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/License-MIT%2FApache--2.0-blue)](https://github.com/slowli/tardigrade#license)
![rust 1.70+ required](https://img.shields.io/badge/rust-1.70+-blue.svg?label=Required%20Rust)

**Documentation:**
[![crate docs (main)](https://img.shields.io/badge/main-yellow.svg?label=docs)](https://slowli.github.io/tardigrade/tardigrade/)

Tardigrade is a Rust library for workflow automation, aka (business) process 
automation / orchestration. It allows defining workflows as a WASM module and running it
in a fully sandboxed, controlled environment.

## What's a workflow, anyway?

A key observation is that a workflow is essentially a future that interacts with the external
world via well-defined interfaces:

- Arguments provided to the workflow on creation
- Inbound and outbound message channels
- Timers
- Tasks

That is, given an async runtime that has capabilities to track the progress of external
futures, and is able to persist the progress of the workflow at wait points, 
the workflow itself can be represented as an ordinary future (i.e., just code)!
As such, concurrency primitives (fork / join, fork / select, etc.)
are not the responsibility of the workflow engine – they come for free with
the programming language / tooling; a workflow can do anything the underlying tooling can do.

*See [architecture](../../ARCHITECTURE.md) for more technical details.*

## Usage

Add this to your `Crate.toml`:

```toml
[dependencies]
tardigrade = "0.1.0"
```

To build a workflow module, it should be built for the `wasm32-unknown-unknown` target
with usual WASM config applied (e.g., `lib.crate-type = ["cdylib", "rlib"]` defined
in the crate manifest).

See the crate docs for more details and examples of workflow definitions.

## Project status 🚧

Extremely early-stage (approximately the first PoC).

## Project naming

[Tardigrades](https://en.wikipedia.org/wiki/Tardigrade) are micro-animals
known for resilience to extreme environments. Tardigrades are among the few
groups of species able to suspend their metabolism for continuous periods
(think years) and later come back to life.

## Alternatives / similar tools

[Camunda] is an example of a [BPMN]-based process orchestration engine.
Newer approaches to process orchestration include
[Temporal] (an example of code-based workflow definition) and [Netflix Conductor].

## License

Licensed under either of [Apache License, Version 2.0](../../LICENSE-APACHE)
or [MIT license](../../LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in `tardigrade` by you, as defined in the Apache-2.0 license,
shall be dual licensed as above, without any additional terms or conditions.

[Camunda]: https://camunda.com/
[BPMN]: https://en.wikipedia.org/wiki/Business_Process_Model_and_Notation
[Temporal]: https://temporal.io/
[Netflix Conductor]: https://conductor.netflix.com/
