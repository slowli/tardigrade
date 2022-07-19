# Tardigrade: Derive Macros

This crate provides procedural macros for [`tardigrade`] allowing
to simplify workflow definition.

## Usage

Add this to your `Crate.toml`:

```toml
[dependencies]
tardigrade-derive = "0.1.0"
```

The crate is re-exported by the [`tadigrade`] crate if its `derive` feature
is on (and it is on by default). Thus, it is rarely necessary to include
this crate as a direct dependency.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE)
or [MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in `tardigrade` by you, as defined in the Apache-2.0 license,
shall be dual licensed as above, without any additional terms or conditions.

[`tardigrade`]: https://crates.io/crates/tardigrade
