extern crate self as fro;

mod api;
mod blake3_hash;
mod cksum_hash;
pub mod command_io;
mod common;
pub mod dd_tool;
mod file_hash;
mod io_util;
mod mincore;
pub mod test_sizing;
mod uring_util;
mod verified_copy;

pub mod block_hash;
pub mod config;
pub mod reader;
pub mod stream;
pub mod writer;

pub use api::{
    benchmark_page_cache_lift, copy_fd_range_to_fd_with_progress, copy_file,
    copy_file_range_with_modes, copy_file_via_memory, copy_file_via_memory_with_modes,
    copy_file_with_modes, copy_path_range_to_fd_with_progress, create, create_with_mode,
    indexed_writer, indexed_writer_with_mode, offset_writer, offset_writer_with_mode,
    offset_writer_with_options, open, open_with_mode, optimal_block_size,
    optimal_block_size_with_mode, read_file, read_file_with_mode, visit_blocks,
    visit_blocks_with_mode, visit_path_range_ordered, write_file, write_file_range,
    write_file_range_with_mode, write_file_with_mode, ByteRange, OrderedVisitDecision,
    PageCacheLiftBenchmarkReport,
};
pub use blake3_hash::hash_file_blake3;
pub use cksum_hash::{cksum_crc_block, cksum_crc_combine, finalize_cksum_crc, hash_file_crc32};
pub use common::{CopyAutoMode, CopyStrategy, IOMode};
pub use file_hash::{hash_file, hash_file_sha256, HashAlgorithm};
pub use reader::{BufReader, MappedReadBuffer};
pub use stream::transform::{
    auto_select_transform_io_pairing, grow_pipe_capacity_best_effort, run_file_transform_to_file,
    run_file_transform_to_pipe_with_owned_output, run_mapper_transform_io_pairing,
    run_mapper_transform_with_specs, run_reader_transform_to_file, run_reader_transform_to_pipe,
    run_staged_file_transform_to_file, run_transform_io_pairing, run_transform_with_specs,
    MapperTransformOutput, MapperTransformOutputKind, PipeOutputPolicy, ReaderTransformGeometry,
    TransformInputSpec, TransformIoPairing, TransformIoPairingKind, TransformOutputSpec,
};
pub use stream::{
    BlockRange, ParallelFile, ParallelReadReport, ParallelWriteReport, ParallelWriter,
};
pub use verified_copy::{copy_file_verified, copy_file_verified_with_options, VerifiedCopyReport};
pub use writer::{BufWriter, OffsetWriter, SequentialWriter};
