# Tardigrade: Workflow Runtime

This crate provides a specialized runtime for [`tardigrade`] workflows.
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

Note that async workflow execution and some other functionality
is provided by opt-in crate features.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE)
or [MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in `tardigrade` by you, as defined in the Apache-2.0 license,
shall be dual licensed as above, without any additional terms or conditions.

[`tardigrade`]: https://crates.io/crates/tardigrade
