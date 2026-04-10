#[path = "../bin_support/dedicated_hashsum.rs"]
mod dedicated_hashsum;

use fro::HashAlgorithm;
use std::process::ExitCode;

fn main() -> ExitCode {
    dedicated_hashsum::run(HashAlgorithm::Sha256, "sha256sum")
}
