mod block_hash;
mod common;
mod config;
mod coreutils;
mod differ;
mod help_compat;
mod io_util;
mod main_app;
mod mincore;
mod optimizer;
mod reader;
#[allow(dead_code)]
mod stream;
mod verified_copy;
mod writer;

fn main() {
    main_app::main();
}
