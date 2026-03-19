pub const MAX_FILE_LINES: usize = 1000;

pub const MAX_FILE_LINES_ALLOWLIST: &[&str] = &[
    "src/block_hash.rs",
    "src/bin/fro-benchmark.rs",
    "src/bin/fro-optimize.rs",
    "src/main.rs",
    "src/reader.rs",
    "src/writer.rs",
];

pub const FILE_SIZE_ROOTS: &[&str] = &["src", "tests", "examples", "janitor/src"];

pub const README_LINK_CHECK_FILES: &[&str] = &["README.md"];

pub const SRC_TREE_SNAPSHOT_FILE: &str = "docs/architecture/src-tree.txt";

pub const SRC_TREE_MAX_DEPTH: usize = 6;

pub const FORMAL_TOOL_BINARIES: &[(&str, &str)] = &[
    ("cargo-miri", "Rust interpreter / UB checker"),
    ("cargo-kani", "CBMC-backed bounded model checking"),
    ("cargo-creusot", "Why3-based deductive verification"),
    ("prusti-rustc", "Viper-based specification verification"),
];
