# ipcy TODO

`ipcy` is a native Rust port of ipcz (see `PLAN.md`). The core lives in
`crates/ipcy/src/core/`; the RAII API (`Node`, `Portal`, `Parcel`, `BoxHandle`, ...)
is at the crate root. It tracks upstream ipcz (mirror `main`, see `PLAN.md`).

## Open items

- [ ] High-level API: overlapped `begin_get` (`BeginGetFlags::OVERLAPPED`) is reachable only through
      `core::api`; decide how to expose it safely.
- [ ] High-level API: application-object boxing needs an ergonomic story beyond
      `Arc<dyn ApplicationObject>` (serializer failure reporting).
- [ ] Loom/Miri coverage for router bypass and the sequenced queue (see PLAN.md, Verification).
- [ ] Large allocations: `core/node_link_memory.rs` (~494, 522, 539) only supports block-based
      fragment allocation; add an alternative scheme for large requests and non-block allocations.
- [ ] Router queueing: `core/router.rs` (~410) notes that a queue may add latency along the route.
- [ ] Subparcel boxes: `handle.rs` (~135) returns `Unimplemented` for `BoxContents::Subparcel`.
- [ ] Non-Linux multiprocess test driver: `tests/support/multinode.rs` (~108) hits `unimplemented!`
      for `DriverKind::Multiprocess` off Linux.
- [ ] Cross-process driver tests: the Mach and Windows driver tests all run inside one process
      (Windows uses a handle to the current process as the peer); only Linux is tested with real
      child processes.
- [ ] macOS rendezvous: no Mach bootstrap-server lookup, so the two sides can't find each other by
      name.
- [ ] Windows broker policy: no sandbox or process-handle policy for handing process handles
      between broker and child.
