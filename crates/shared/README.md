# Tardigrade: Shared Data Types

[![Build Status](https://github.com/slowli/tardigrade/workflows/CI/badge.svg?branch=main)](https://github.com/slowli/tardigrade/actions)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/License-MIT%2FApache--2.0-blue)](https://github.com/slowli/tardigrade#license)
![rust 1.70+ required](https://img.shields.io/badge/rust-1.70+-blue.svg?label=Required%20Rust)

**Documentation:**
[![crate docs (main)](https://img.shields.io/badge/main-yellow.svg?label=docs)](https://slowli.github.io/tardigrade/tardigrade_shared/)

This crate provides data types for [`tardigrade`] shared among multiple libraries.

## Usage

Add this to your `Crate.toml`:

```toml
[dependencies]
tardigrade-shared = "0.1.0"
```

Note that a significant portion of the crate is re-exported by [`tardigrade`]
or [`tardigrade-rt`], so it is rarely necessary to include this crate as a direct dependency.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE)
or [MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in `tardigrade` by you, as defined in the Apache-2.0 license,
shall be dual licensed as above, without any additional terms or conditions.

[`tardigrade`]: https://crates.io/crates/tardigrade
[`tardigrade-rt`]: https://crates.io/crates/tardigrade-rt
