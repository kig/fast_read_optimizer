use super::*;

mod args;
mod execute;

#[allow(dead_code)]
pub(super) fn try_main() -> io::Result<i32> {
    try_main_from(std::env::args().collect())
}

pub(super) fn try_main_from(raw_args: Vec<String>) -> io::Result<i32> {
    match args::parse_cli_from(raw_args)? {
        args::ParseOutcome::Early(code) => Ok(code),
        args::ParseOutcome::Parsed(parsed) => execute::run(parsed),
    }
}

#[cfg(test)]
pub(super) struct TestCopyRunOptions {
    pub(super) source: String,
    pub(super) target: String,
    pub(super) recursive: bool,
    pub(super) verbose: bool,
    pub(super) cp_no_clobber: bool,
    pub(super) cp_no_target_directory: bool,
    pub(super) cp_update: bool,
    pub(super) cp_preserve_mode: bool,
    pub(super) cp_preserve_timestamps: bool,
    pub(super) cp_no_dereference: bool,
    pub(super) cp_dereference: bool,
}

#[cfg(test)]
pub(super) fn run_test_copy(options: TestCopyRunOptions) -> io::Result<i32> {
    execute::run(args::ParsedArgs {
        mode: "copy".to_string(),
        config_subcommand: None,
        config_target: None,
        io_mode: common::IOMode::Auto,
        io_mode_write: common::IOMode::Auto,
        to_memory: false,
        auto_lift: false,
        to_memory_mode: ReadToMemoryMode::Auto,
        to_memory_options: ReadToMemoryOptions::default(),
        manual_read_overrides: ManualReadOverrides::default(),
        via_memory: false,
        verify_copy: false,
        verify_copy_diff: false,
        recursive_copy: options.recursive,
        persist_verification_hashes: false,
        quiet: true,
        no_lock: false,
        keep_target_size: false,
        force_diff_copy: false,
        force_full_copy: false,
        force_copy_file_range: false,
        force_copy_file_range_single: false,
        force_threaded_copy: true,
        force_reflink: false,
        cp_compat: true,
        cp_no_clobber: options.cp_no_clobber,
        cp_target_directory: None,
        cp_no_target_directory: options.cp_no_target_directory,
        cp_update: options.cp_update,
        cp_preserve_mode: options.cp_preserve_mode,
        cp_preserve_timestamps: options.cp_preserve_timestamps,
        cp_no_dereference: options.cp_no_dereference,
        cp_dereference: options.cp_dereference,
        verbose: options.verbose,
        source: Some(options.source),
        pattern: String::new(),
        filename: options.target,
        extra_paths: Vec::new(),
        hash_base: None,
        recover_mode: RecoverMode::Standard,
        hash_type: BlockHashAlgorithm::Xxh3,
        hash_only: false,
        create_size: None,
        iterations: 1,
        save_config: false,
        config_path: None,
        overlap_large_file: None,
        small_file_thread_cache_state: None,
    })
}

#[allow(dead_code)]
pub(super) fn main() {
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        if is_broken_pipe_panic(info.payload()) {
            return;
        }
        default_hook(info);
    }));

    match std::panic::catch_unwind(try_main) {
        Ok(Ok(code)) if code == 0 => {}
        Ok(Ok(code)) => std::process::exit(code),
        Ok(Err(err)) => {
            if is_broken_pipe_error(&err) {
                std::process::exit(0);
            }
            fro::cio_eprintln!("Error: {}", err);
            std::process::exit(1);
        }
        Err(payload) => {
            if is_broken_pipe_panic(payload.as_ref()) {
                std::process::exit(0);
            }
            std::panic::resume_unwind(payload);
        }
    }
}
