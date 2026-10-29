//! Compares the Rust shared-memory layouts against the upstream C++ ipcz
//! headers. Compiles `tests/layout/layout.cc` with the system C++ compiler.
//!
//! Needs `IPCZ_PINNED_SRC` (a checkout of the upstream ipcz tree) and
//! `IPCZ_ABSL_INCLUDE` (a directory containing `absl/`); skips when unset.

#![cfg(unix)]

use std::collections::HashMap;
use std::mem::{align_of, offset_of, size_of};
use std::path::{Path, PathBuf};
use std::process::Command;

use ipcy::core::fragment_descriptor::FragmentDescriptor;
use ipcy::core::node_name::NodeName;
use ipcy::core::ref_counted_fragment::RefCountedFragment;
use ipcy::core::router_link_state::RouterLinkState;

fn run_cpp(src: &Path, absl: &Path) -> HashMap<String, usize> {
    let out_dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"));
    let shim = out_dir.join("layout-shim");
    std::fs::create_dir_all(shim.join("third_party")).unwrap();
    let _ = std::fs::remove_file(shim.join("third_party/abseil-cpp"));
    std::os::unix::fs::symlink(absl, shim.join("third_party/abseil-cpp")).unwrap();

    let exe = out_dir.join("layout-cpp");
    let status = Command::new("c++")
        .args(["-std=c++20", "-Wno-invalid-offsetof"])
        .arg(format!("-I{}", src.join("src").display()))
        .arg(format!("-I{}", src.join("include").display()))
        .arg(format!("-I{}", shim.display()))
        .arg(format!("-I{}", absl.display()))
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/layout/layout.cc"))
        .arg(src.join("src/ipcz/router_link_state.cc"))
        .arg(src.join("src/ipcz/ref_counted_fragment.cc"))
        .arg("-o")
        .arg(&exe)
        .status()
        .expect("failed to run c++");
    assert!(status.success(), "C++ layout program failed to build");

    let output = Command::new(&exe).output().unwrap();
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .flat_map(|line| {
            // "T size=N align=M" or "T.field offset=N"
            let (name, rest) = line.split_once(' ').unwrap();
            rest.split(' ')
                .map(|kv| kv.split_once('=').unwrap())
                .map(|(k, v)| (format!("{name}.{k}"), v.parse().unwrap()))
                .collect::<Vec<_>>()
        })
        .collect()
}

#[test]
fn layouts_match_cpp() {
    let (Some(src), Some(absl)) = (
        std::env::var_os("IPCZ_PINNED_SRC"),
        std::env::var_os("IPCZ_ABSL_INCLUDE"),
    ) else {
        eprintln!("skipping: IPCZ_PINNED_SRC / IPCZ_ABSL_INCLUDE not set");
        return;
    };
    let cpp = run_cpp(Path::new(&src), Path::new(&absl));

    let expected = [
        ("NodeName.size", size_of::<NodeName>()),
        ("NodeName.align", align_of::<NodeName>()),
        ("FragmentDescriptor.size", size_of::<FragmentDescriptor>()),
        ("FragmentDescriptor.align", align_of::<FragmentDescriptor>()),
        ("RefCountedFragment.size", size_of::<RefCountedFragment>()),
        ("RefCountedFragment.align", align_of::<RefCountedFragment>()),
        ("RouterLinkState.size", size_of::<RouterLinkState>()),
        ("RouterLinkState.align", align_of::<RouterLinkState>()),
        ("RouterLinkState.status.offset", offset_of!(RouterLinkState, status)),
        (
            "RouterLinkState.allowed_bypass_request_source.offset",
            offset_of!(RouterLinkState, allowed_bypass_request_source),
        ),
        ("RouterLinkState.reserved1.offset", offset_of!(RouterLinkState, reserved1)),
    ];
    expected
        .iter()
        .for_each(|(key, rust)| assert_eq!(cpp.get(*key), Some(rust), "{key}"));
}
