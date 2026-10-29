# ipcy

`ipcy` is a native Rust port of [ipcz](https://chromium.googlesource.com/chromium/src/+/main/third_party/ipcz/), Chromium's inter-process communication library. It provides a high-level RAII API at the crate root on top of a port of the ipcz core (`core/`).

ipcz models communication as **nodes** connected by pairs of **portals**. Portals send and receive **parcels** (data plus handles), and portals themselves can be passed through other portals. Routing between nodes is handled by the library.

## Layout

- `crates/ipcy`: the only crate.
  - `src/core/`: port of the ipcz core (nodes, links, routers, shared memory).
  - `src/api.rs`, `src/handle.rs`: the RAII API (`Node`, `Portal`, `Parcel`, `BoxHandle`, `Trap`, transactions).
  - `src/drivers/`: transport drivers for Unix sockets (Linux only, uses memfd), Mach (macOS) and Windows.
  - `tests/`: integration tests, including multiprocess and layout checks against the C++ implementation.
- `docs/`: design documentation.
- `PLAN.md`, `TODO.md`: remaining work and known gaps.

## Usage

```toml
[dependencies]
ipcy = { path = "crates/ipcy" }
```

```rust
use ipcy::Node;

let node = Node::create(driver, flags, None)?;
let (a, b) = node.open_portals()?;
```

To connect two nodes, create a node on each side and call `Node::connect_node` with a driver transport. See the tests in `crates/ipcy/tests/` for complete examples.

## Building and testing

```sh
cargo build
cargo test
```

## Status

The core port and the Linux, macOS and Windows drivers are done. See `PLAN.md` for known gaps, such as large allocations, overlapped `begin_get` and cross-process driver testing on macOS and Windows.

## License

Licensed under either of [Apache License 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT), at your option.
