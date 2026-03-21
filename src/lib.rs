mod api;
mod blake3_hash;
mod common;
mod file_hash;
mod io_util;
mod mincore;
mod verified_copy;

pub mod block_hash;
pub mod config;
pub mod reader;
pub mod stream;
pub mod writer;

pub use api::{
    copy_file, copy_file_range_with_modes, copy_file_via_memory, copy_file_via_memory_with_modes,
    copy_file_with_modes, create, create_with_mode, indexed_writer, indexed_writer_with_mode,
    offset_writer, offset_writer_with_mode, offset_writer_with_options, open, open_with_mode,
    optimal_block_size, optimal_block_size_with_mode, read_file, read_file_with_mode, visit_blocks,
    visit_blocks_with_mode, write_file, write_file_range, write_file_range_with_mode,
    write_file_with_mode,
};
pub use blake3_hash::hash_file_blake3;
pub use common::{CopyAutoMode, CopyStrategy, IOMode};
pub use file_hash::{hash_file, hash_file_sha256, HashAlgorithm};
pub use stream::{
    BlockRange, ParallelFile, ParallelReadReport, ParallelWriteReport, ParallelWriter,
};
pub use verified_copy::{copy_file_verified, copy_file_verified_with_options, VerifiedCopyReport};
pub use writer::{OffsetWriter, SequentialWriter};
