# Tardigrade: Workflow Runtime

[![Build Status](https://github.com/slowli/tardigrade/workflows/CI/badge.svg?branch=main)](https://github.com/slowli/tardigrade/actions)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/License-MIT%2FApache--2.0-blue)](https://github.com/slowli/tardigrade#license)
![rust 1.70+ required](https://img.shields.io/badge/rust-1.70+-blue.svg?label=Required%20Rust)

**Documentation:**
[![crate docs (main)](https://img.shields.io/badge/main-yellow.svg?label=docs)](https://slowli.github.io/tardigrade/tardigrade_rt/)

This crate provides an embeddable runtime for [`tardigrade`] workflows.
Workflow modules can be run either using a high-level async interface,
or a lower-level sync API; the latter could be useful for greater control
over workflow execution. Both statically known and dynamically-typed
workflow interfaces are supported.

## Usage

Add this to your `Crate.toml`:

```toml
[dependencies]
tardigrade-rt = "0.1.0"
```

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE)
or [MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in `tardigrade` by you, as defined in the Apache-2.0 license,
shall be dual licensed as above, without any additional terms or conditions.

<!-- TODO: replace with crates.io links before publishing -->
[`tardigrade`]: ../..
