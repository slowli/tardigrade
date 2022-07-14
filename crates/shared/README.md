# Tardigrade: Shared Data Types

This crate provides data types for [`tardigrade`] shared among multiple libraries.

## Usage

Add this to your `Crate.toml`:

```toml
[dependencies]
tardigrade-shared = "0.1.0"
```

Note that a significant portion of the crate is re-exported by [`tardigrade`]
or [`tardigrade-rt`], so it is rarely necessary to include this crate as a direct dependency.

[`tardigrade`]: https://crates.io/crates/tardigrade
