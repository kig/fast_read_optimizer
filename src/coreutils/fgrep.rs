use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
struct FgrepOptions {
    count_only: bool,
    quiet: bool,
    files_with_matches: bool,
    files_without_match: bool,
    color: bool,
    only_matching: bool,
    suppress_messages: bool,
    initial_tab: bool,
    line_buffered: bool,
    before_context: u64,
    after_context: u64,
    offset_width: usize,
    print_line_numbers: bool,
    print_byte_offsets: bool,
    line_regexp: bool,
    word_regexp: bool,
    ignore_case: bool,
    invert_match: bool,
    max_count: Option<u64>,
    null_terminate_filenames: bool,
    null_data: bool,
    report_gbps: bool,
    group_separator: FgrepGroupSeparatorPolicy,
    binary_mode: FgrepBinaryMode,
    filename_mode: FgrepFilenameMode,
    device_policy: FgrepDevicePolicy,
    directory_policy: FgrepDirectoryPolicy,
}

impl FgrepOptions {
    fn record_sep(self) -> u8 {
        if self.null_data {
            b'\0'
        } else {
            b'\n'
        }
    }
}

#[derive(Clone)]
struct FgrepPattern {
    raw: Vec<u8>,
    normalized: Vec<u8>,
}

enum PatternSource {
    Inline(String),
    File(String),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum FgrepFilenameMode {
    #[default]
    Auto,
    Always,
    Never,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum FgrepBinaryMode {
    #[default]
    Default,
    Text,
    WithoutMatch,
    Binary,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum FgrepColorMode {
    #[default]
    Auto,
    Always,
    Never,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum FgrepGroupSeparatorPolicy {
    #[default]
    Default,
    Disabled,
    Custom(&'static str),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FgrepRegularFilePath {
    LiteralSearchOffsets,
    LineFilterSinglePattern,
    LineFilterMultiPattern,
}

const FGREP_SMALL_FILE_PROBE_LIMIT: u64 = 64 * 1024;
const FGREP_CONFLICTING_MATCHERS: &str = "grep: conflicting matchers specified";

struct ParsedFgrepArgs {
    io_mode: IOMode,
    options: FgrepOptions,
    pattern_sources: Vec<PatternSource>,
    files: Vec<String>,
    stdin_label: Option<String>,
    glob_filter: FgrepGlobFilter,
}

const FGREP_MAX_COUNT_REACHED: &str = "fgrep max count reached";

mod color;
mod context;
mod file_kinds;
mod line_matching;
mod only_matching;
mod parsing;
mod runtime;

use self::context::{fgrep_context_enabled, fgrep_reset_context_output_state};
use self::file_kinds::{
    collect_dir_files_sorted, collect_dir_files_sorted_dereference, fgrep_input_path_kind,
    parse_devices_value, parse_directories_value, FgrepDevicePolicy, FgrepDirectoryPolicy,
    FgrepGlobFilter, FgrepInputPathKind,
};
use self::line_matching::fgrep_short_flag_effect;
use self::parsing::{
    compile_patterns, fgrep_conflicting_matchers_error, fgrep_is_conflicting_matchers_error,
    load_exclude_from, parse_binary_files_value, parse_color_mode_value, parse_context_count_value,
    parse_max_count_value, parse_option_value, parse_pattern_file_bytes,
};
#[cfg(test)]
use self::runtime::count_literal_matching_lines;
use self::runtime::{
    fgrep_display_label, fgrep_exit_code, fgrep_report_input_error,
    fgrep_suppresses_matching_line_output, handle_loaded_match_result, write_count_line,
    write_filename_result, write_matching_stream_lines, write_matching_stream_lines_multi,
};

pub(super) fn fgrep_line_number_prefix(print_line_numbers: bool, line_no: u64) -> Option<u64> {
    print_line_numbers.then_some(line_no)
}

fn fgrep_stdout_is_tty() -> bool {
    unsafe { libc::isatty(fro::command_io::stdout_fd()) == 1 }
}

fn fgrep_probe_options(options: FgrepOptions) -> FgrepOptions {
    FgrepOptions {
        count_only: false,
        quiet: true,
        files_with_matches: false,
        files_without_match: false,
        color: options.color,
        only_matching: options.only_matching,
        suppress_messages: false,
        initial_tab: false,
        line_buffered: false,
        before_context: options.before_context,
        after_context: options.after_context,
        offset_width: options.offset_width,
        print_line_numbers: false,
        print_byte_offsets: false,
        line_regexp: false,
        word_regexp: false,
        ignore_case: options.ignore_case,
        invert_match: false,
        max_count: None,
        null_terminate_filenames: false,
        null_data: options.null_data,
        report_gbps: false,
        group_separator: options.group_separator,
        binary_mode: options.binary_mode,
        filename_mode: options.filename_mode,
        device_policy: options.device_policy,
        directory_policy: options.directory_policy,
    }
}

fn fgrep_data_is_binary(data: &[u8], options: FgrepOptions) -> bool {
    !options.null_data
        && !matches!(options.binary_mode, FgrepBinaryMode::Text)
        && memchr::memchr(b'\0', data).is_some()
}

fn fgrep_binary_without_match(options: FgrepOptions) -> bool {
    matches!(options.binary_mode, FgrepBinaryMode::WithoutMatch)
}

fn fgrep_binary_reports_match(options: FgrepOptions) -> bool {
    matches!(
        options.binary_mode,
        FgrepBinaryMode::Default | FgrepBinaryMode::Binary
    ) && !fgrep_suppresses_matching_line_output(options)
}

fn fgrep_regular_file_path(options: FgrepOptions, pattern_count: usize) -> FgrepRegularFilePath {
    if options.line_regexp
        || options.word_regexp
        || options.ignore_case
        || fgrep_context_enabled(options)
    {
        if pattern_count == 1 {
            FgrepRegularFilePath::LineFilterSinglePattern
        } else {
            FgrepRegularFilePath::LineFilterMultiPattern
        }
    } else if pattern_count == 1 {
        FgrepRegularFilePath::LiteralSearchOffsets
    } else {
        FgrepRegularFilePath::LineFilterMultiPattern
    }
}

fn try_load_small_regular_file_bytes(path: &str, io_mode: IOMode) -> io::Result<Option<Vec<u8>>> {
    if matches!(io_mode, IOMode::Direct) {
        return Ok(None);
    }
    if fs::metadata(path)?.len() > FGREP_SMALL_FILE_PROBE_LIMIT {
        return Ok(None);
    }
    fs::read(path).map(Some)
}

fn parse_fgrep_args(args: &[String]) -> io::Result<ParsedFgrepArgs> {
    let mut io_mode = IOMode::Auto;
    let mut options = FgrepOptions::default();
    let mut color_mode = FgrepColorMode::Auto;
    let mut pattern_sources = Vec::new();
    let mut positional_pattern = None::<String>;
    let mut files = Vec::new();
    let mut stdin_label = None::<String>;
    let mut glob_filter = FgrepGlobFilter::default();
    let mut end_flags = false;
    let mut index = 1usize;
    while index < args.len() {
        let arg = &args[index];
        if end_flags {
            if positional_pattern.is_none() && pattern_sources.is_empty() {
                positional_pattern = Some(arg.clone());
            } else {
                files.push(arg.clone());
            }
            index += 1;
            continue;
        }
        match arg.as_str() {
            "-c" | "--count" => options.count_only = true,
            "-n" | "--line-number" => options.print_line_numbers = true,
            "-x" | "--line-regexp" => options.line_regexp = true,
            "-w" | "--word-regexp" => options.word_regexp = true,
            "-i" | "--ignore-case" => options.ignore_case = true,
            "--no-ignore-case" => options.ignore_case = false,
            "-v" | "--invert-match" => options.invert_match = true,
            "-q" | "--quiet" | "--silent" => options.quiet = true,
            "--color" | "--colour" => color_mode = FgrepColorMode::Auto,
            "-o" | "--only-matching" => options.only_matching = true,
            "-l" | "--files-with-matches" => options.files_with_matches = true,
            "-L" | "--files-without-match" => options.files_without_match = true,
            "-H" | "--with-filename" => options.filename_mode = FgrepFilenameMode::Always,
            "-h" | "--no-filename" => options.filename_mode = FgrepFilenameMode::Never,
            "-b" | "--byte-offset" => options.print_byte_offsets = true,
            "-Z" | "--null" => options.null_terminate_filenames = true,
            "-z" | "--null-data" => options.null_data = true,
            "-T" | "--initial-tab" => options.initial_tab = true,
            "--line-buffered" => options.line_buffered = true,
            "-s" | "--no-messages" => options.suppress_messages = true,
            "--group-separator" => {
                let separator: &'static str = Box::leak(
                    parse_option_value(args, &mut index, None, "--group-separator")?
                        .into_boxed_str(),
                );
                options.group_separator = FgrepGroupSeparatorPolicy::Custom(separator);
            }
            "--no-group-separator" => options.group_separator = FgrepGroupSeparatorPolicy::Disabled,
            "-a" | "--text" | "--binary-files=text" => {
                options.binary_mode = FgrepBinaryMode::Text;
            }
            "-E" | "--extended-regexp" | "-G" | "--basic-regexp" | "-P" | "--perl-regexp" => {
                return Err(fgrep_conflicting_matchers_error());
            }
            "-I" | "--binary-files=without-match" => {
                options.binary_mode = FgrepBinaryMode::WithoutMatch;
            }
            "-U" | "--binary" | "--binary-files=binary" => {
                options.binary_mode = FgrepBinaryMode::Binary;
            }
            "--report-gbps" => options.report_gbps = true,
            "-F" | "--fixed-strings" => {}
            "-r" | "--recursive" | "--directories=recurse" => {
                options.directory_policy = FgrepDirectoryPolicy::Recurse;
            }
            "-R" | "--dereference-recursive" => {
                options.directory_policy = FgrepDirectoryPolicy::RecurseDereference;
            }
            "--auto" => io_mode = IOMode::Auto,
            "--direct" => io_mode = IOMode::Direct,
            "--no-direct" => io_mode = IOMode::PageCache,
            "--" => end_flags = true,
            "-e" => pattern_sources.push(PatternSource::Inline(parse_option_value(
                args, &mut index, None, "-e",
            )?)),
            "--regexp" => pattern_sources.push(PatternSource::Inline(parse_option_value(
                args, &mut index, None, "--regexp",
            )?)),
            "-f" => pattern_sources.push(PatternSource::File(parse_option_value(
                args, &mut index, None, "-f",
            )?)),
            "--file" => pattern_sources.push(PatternSource::File(parse_option_value(
                args, &mut index, None, "--file",
            )?)),
            "-m" => {
                options.max_count = Some(parse_max_count_value(&parse_option_value(
                    args, &mut index, None, "-m",
                )?)?)
            }
            "--max-count" => {
                options.max_count = Some(parse_max_count_value(&parse_option_value(
                    args,
                    &mut index,
                    None,
                    "--max-count",
                )?)?)
            }
            "-A" => {
                options.after_context =
                    parse_context_count_value(&parse_option_value(args, &mut index, None, "-A")?)?
            }
            "--after-context" => {
                options.after_context = parse_context_count_value(&parse_option_value(
                    args,
                    &mut index,
                    None,
                    "--after-context",
                )?)?
            }
            "-B" => {
                options.before_context =
                    parse_context_count_value(&parse_option_value(args, &mut index, None, "-B")?)?
            }
            "--before-context" => {
                options.before_context = parse_context_count_value(&parse_option_value(
                    args,
                    &mut index,
                    None,
                    "--before-context",
                )?)?
            }
            "-C" => {
                let n =
                    parse_context_count_value(&parse_option_value(args, &mut index, None, "-C")?)?;
                options.before_context = n;
                options.after_context = n;
            }
            "--context" => {
                let n = parse_context_count_value(&parse_option_value(
                    args,
                    &mut index,
                    None,
                    "--context",
                )?)?;
                options.before_context = n;
                options.after_context = n;
            }
            "-D" => {
                options.device_policy =
                    parse_devices_value(&parse_option_value(args, &mut index, None, "-D")?)?
            }
            "--devices" => {
                options.device_policy =
                    parse_devices_value(&parse_option_value(args, &mut index, None, "--devices")?)?
            }
            "-d" => {
                options.directory_policy =
                    parse_directories_value(&parse_option_value(args, &mut index, None, "-d")?)?
            }
            "--directories" => {
                options.directory_policy = parse_directories_value(&parse_option_value(
                    args,
                    &mut index,
                    None,
                    "--directories",
                )?)?
            }
            "--label" => stdin_label = Some(parse_option_value(args, &mut index, None, "--label")?),
            "--include" => {
                let v = parse_option_value(args, &mut index, None, "--include")?;
                glob_filter
                    .include
                    .push(FgrepGlobFilter::parse_glob("--include", &v)?);
            }
            "--exclude" => {
                let v = parse_option_value(args, &mut index, None, "--exclude")?;
                glob_filter
                    .exclude
                    .push(FgrepGlobFilter::parse_glob("--exclude", &v)?);
            }
            "--exclude-dir" => {
                let v = parse_option_value(args, &mut index, None, "--exclude-dir")?;
                glob_filter
                    .exclude_dir
                    .push(FgrepGlobFilter::parse_glob("--exclude-dir", &v)?);
            }
            "--exclude-from" => {
                let path = parse_option_value(args, &mut index, None, "--exclude-from")?;
                load_exclude_from(&mut glob_filter, &path)?;
            }
            other => {
                if let Some(v) = other.strip_prefix("--include=") {
                    glob_filter
                        .include
                        .push(FgrepGlobFilter::parse_glob("--include", v)?);
                } else if let Some(v) = other.strip_prefix("--exclude=") {
                    glob_filter
                        .exclude
                        .push(FgrepGlobFilter::parse_glob("--exclude", v)?);
                } else if let Some(v) = other.strip_prefix("--exclude-dir=") {
                    glob_filter
                        .exclude_dir
                        .push(FgrepGlobFilter::parse_glob("--exclude-dir", v)?);
                } else if let Some(v) = other.strip_prefix("--exclude-from=") {
                    load_exclude_from(&mut glob_filter, v)?;
                } else if let Some(v) = other.strip_prefix("--regexp=") {
                    pattern_sources.push(PatternSource::Inline(v.to_string()));
                } else if let Some(v) = other.strip_prefix("--file=") {
                    pattern_sources.push(PatternSource::File(v.to_string()));
                } else if let Some(v) = other.strip_prefix("--max-count=") {
                    options.max_count = Some(parse_max_count_value(v)?);
                } else if let Some(v) = other.strip_prefix("--after-context=") {
                    options.after_context = parse_context_count_value(v)?;
                } else if let Some(v) = other.strip_prefix("--before-context=") {
                    options.before_context = parse_context_count_value(v)?;
                } else if let Some(v) = other.strip_prefix("--context=") {
                    let n = parse_context_count_value(v)?;
                    options.before_context = n;
                    options.after_context = n;
                } else if let Some(v) = other.strip_prefix("--devices=") {
                    options.device_policy = parse_devices_value(v)?;
                } else if let Some(v) = other.strip_prefix("--directories=") {
                    options.directory_policy = parse_directories_value(v)?;
                } else if let Some(v) = other.strip_prefix("--label=") {
                    stdin_label = Some(v.to_string());
                } else if let Some(v) = other.strip_prefix("--binary-files=") {
                    options.binary_mode = parse_binary_files_value(v)?;
                } else if let Some(v) = other.strip_prefix("--color=") {
                    color_mode = parse_color_mode_value(Some(v), "--color")?;
                } else if let Some(v) = other.strip_prefix("--colour=") {
                    color_mode = parse_color_mode_value(Some(v), "--colour")?;
                } else if let Some(v) = other.strip_prefix("--group-separator=") {
                    let separator: &'static str = Box::leak(v.to_string().into_boxed_str());
                    options.group_separator = FgrepGroupSeparatorPolicy::Custom(separator);
                } else if let Some(v) = other.strip_prefix("-e") {
                    if !v.is_empty() {
                        pattern_sources.push(PatternSource::Inline(v.to_string()));
                    }
                } else if let Some(v) = other.strip_prefix("-f") {
                    if !v.is_empty() {
                        pattern_sources.push(PatternSource::File(v.to_string()));
                    }
                } else if let Some(v) = other.strip_prefix("-m") {
                    if !v.is_empty() {
                        options.max_count = Some(parse_max_count_value(v)?);
                    }
                } else if let Some(v) = other.strip_prefix("-A") {
                    if !v.is_empty() {
                        options.after_context = parse_context_count_value(v)?;
                    }
                } else if let Some(v) = other.strip_prefix("-B") {
                    if !v.is_empty() {
                        options.before_context = parse_context_count_value(v)?;
                    }
                } else if let Some(v) = other.strip_prefix("-C") {
                    if !v.is_empty() {
                        let n = parse_context_count_value(v)?;
                        options.before_context = n;
                        options.after_context = n;
                    }
                } else if let Some(v) = other.strip_prefix("-D") {
                    if !v.is_empty() {
                        options.device_policy = parse_devices_value(v)?;
                    }
                } else if let Some(v) = other.strip_prefix("-d") {
                    if !v.is_empty() {
                        options.directory_policy = parse_directories_value(v)?;
                    }
                } else if other.len() > 1
                    && other.starts_with('-')
                    && other[1..].bytes().all(|b| b.is_ascii_digit())
                {
                    let n = parse_context_count_value(&other[1..])?;
                    options.before_context = n;
                    options.after_context = n;
                } else if let [b'-', flag] = other.as_bytes() {
                    if fgrep_short_flag_effect(*flag).is_none()
                        && (positional_pattern.is_none() && pattern_sources.is_empty())
                    {
                        positional_pattern = Some(other.to_string());
                    }
                } else if positional_pattern.is_none() && pattern_sources.is_empty() {
                    positional_pattern = Some(other.to_string());
                } else {
                    files.push(other.to_string());
                }
            }
        }
        index += 1;
    }
    if let Some(pattern) = positional_pattern {
        pattern_sources.push(PatternSource::Inline(pattern));
    }
    if pattern_sources.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "fgrep requires a search pattern",
        ));
    }
    options.color = match color_mode {
        FgrepColorMode::Always => true,
        FgrepColorMode::Never => false,
        FgrepColorMode::Auto => fgrep_stdout_is_tty(),
    };
    Ok(ParsedFgrepArgs {
        io_mode,
        options,
        pattern_sources,
        files,
        stdin_label,
        glob_filter,
    })
}

pub(super) fn run_fgrep(args: &[String]) -> io::Result<i32> {
    let ParsedFgrepArgs {
        io_mode,
        options,
        pattern_sources,
        files,
        stdin_label,
        glob_filter,
    } = match parse_fgrep_args(args) {
        Ok(parsed) => parsed,
        Err(err) if fgrep_is_conflicting_matchers_error(&err) => {
            fro::cio_eprintln!("{err}");
            return Ok(2);
        }
        Err(err) => return Err(err),
    };
    let patterns = compile_patterns(pattern_sources, options.ignore_case)?;
    if patterns.is_empty() {
        return Ok(1);
    }
    let pattern = &patterns[0];
    let regular_file_path = fgrep_regular_file_path(options, patterns.len());
    let raw_inputs = parse_stream_inputs(files);

    let mut saw_error = false;
    let recurse = matches!(
        options.directory_policy,
        FgrepDirectoryPolicy::Recurse | FgrepDirectoryPolicy::RecurseDereference
    );
    // Expand directory arguments when --directories=recurse/-r/-R is active so
    // that multi_file and the main processing loop see a flat file list.
    let (inputs, any_dir_expanded) = if recurse {
        let mut expanded: Vec<StreamInput> = Vec::new();
        let mut any_dir = false;
        let dereference = matches!(
            options.directory_policy,
            FgrepDirectoryPolicy::RecurseDereference
        );
        for input in raw_inputs {
            match &input {
                StreamInput::File(file) => {
                    match fgrep_input_path_kind(file) {
                        Ok(FgrepInputPathKind::Directory) => {
                            any_dir = true;
                            let dir_files = if dereference {
                                collect_dir_files_sorted_dereference(
                                    file,
                                    &glob_filter,
                                    &mut |path, e| {
                                        fgrep_report_input_error(path, &e, options);
                                        saw_error = true;
                                    },
                                )
                            } else {
                                collect_dir_files_sorted(file, &glob_filter, &mut |path, e| {
                                    fgrep_report_input_error(path, &e, options);
                                    saw_error = true;
                                })
                            };
                            for f in dir_files {
                                expanded.push(StreamInput::File(f));
                            }
                        }
                        // For non-directory explicit arguments, apply --include/--exclude
                        // to the file's basename (GNU grep behaviour).
                        _ => {
                            if !glob_filter.is_empty() {
                                let basename = std::path::Path::new(file.as_str())
                                    .file_name()
                                    .map(|n| n.as_encoded_bytes())
                                    .unwrap_or(file.as_bytes());
                                if glob_filter.file_allowed(basename) {
                                    expanded.push(input);
                                }
                            } else {
                                expanded.push(input);
                            }
                        }
                    }
                }
                _ => expanded.push(input),
            }
        }
        (expanded, any_dir)
    } else if !glob_filter.is_empty() {
        // No recursion but glob filters are active: apply --include/--exclude
        // to explicit file arguments (GNU grep does the same).
        let filtered = raw_inputs
            .into_iter()
            .filter(|input| match input {
                StreamInput::File(file) => {
                    let basename = std::path::Path::new(file.as_str())
                        .file_name()
                        .map(|n| n.as_encoded_bytes())
                        .unwrap_or(file.as_bytes());
                    glob_filter.file_allowed(basename)
                }
                _ => true,
            })
            .collect();
        (filtered, false)
    } else {
        (raw_inputs, false)
    };

    let multi_file = match options.filename_mode {
        FgrepFilenameMode::Always => true,
        FgrepFilenameMode::Never => false,
        FgrepFilenameMode::Auto => inputs.len() > 1 || any_dir_expanded,
    };
    let mut out = stdout_buf_writer()?;
    let mut matched_any = false;
    let started_at = std::time::Instant::now();
    let mut total_bytes = 0_u64;
    let mut config = None;
    fgrep_reset_context_output_state();
    for input in &inputs {
        match input {
            StreamInput::File(file) => {
                let kind = match fgrep_input_path_kind(file) {
                    Ok(k) => k,
                    Err(e) => {
                        fgrep_report_input_error(file, &e, options);
                        saw_error = true;
                        continue;
                    }
                };
                match kind {
                    FgrepInputPathKind::Directory => match options.directory_policy {
                        FgrepDirectoryPolicy::Skip
                        | FgrepDirectoryPolicy::Recurse
                        | FgrepDirectoryPolicy::RecurseDereference => continue,
                        FgrepDirectoryPolicy::Read => {
                            let e = io::Error::new(io::ErrorKind::Other, "Is a directory");
                            fgrep_report_input_error(file, &e, options);
                            saw_error = true;
                            if options.max_count == Some(0) {
                                continue;
                            }
                            let matched = handle_loaded_match_result(
                                &mut out,
                                Some(file.as_str()),
                                &[],
                                pattern,
                                &patterns,
                                regular_file_path,
                                None,
                                multi_file,
                                options,
                            )?;
                            if matched {
                                matched_any = true;
                            }
                            if options.files_with_matches && matched {
                                write_filename_result(&mut out, Some(file.as_str()), options)?;
                            } else if options.files_without_match && !matched {
                                write_filename_result(&mut out, Some(file.as_str()), options)?;
                            }
                            continue;
                        }
                    },
                    FgrepInputPathKind::Device | FgrepInputPathKind::Other => {
                        match options.device_policy {
                            FgrepDevicePolicy::Skip => continue,
                            FgrepDevicePolicy::Read => {
                                if options.max_count == Some(0) {
                                    continue;
                                }
                                let result = if patterns.len() == 1 {
                                    write_matching_stream_lines(
                                        &mut out,
                                        Some(file.as_str()),
                                        input,
                                        io_mode,
                                        pattern.raw.as_slice(),
                                        pattern.normalized.as_slice(),
                                        multi_file,
                                        options,
                                    )
                                } else {
                                    write_matching_stream_lines_multi(
                                        &mut out,
                                        Some(file.as_str()),
                                        input,
                                        io_mode,
                                        &patterns,
                                        multi_file,
                                        options,
                                    )
                                };
                                match result {
                                    Ok((matched, bytes)) => {
                                        total_bytes += bytes;
                                        if matched {
                                            matched_any = true;
                                        }
                                        if options.files_with_matches && matched {
                                            write_filename_result(
                                                &mut out,
                                                Some(file.as_str()),
                                                options,
                                            )?;
                                        } else if options.files_without_match && !matched {
                                            write_filename_result(
                                                &mut out,
                                                Some(file.as_str()),
                                                options,
                                            )?;
                                        }
                                    }
                                    Err(e) => {
                                        fgrep_report_input_error(file, &e, options);
                                        saw_error = true;
                                    }
                                }
                                continue;
                            }
                        }
                    }
                    FgrepInputPathKind::Regular => {
                        if options.max_count == Some(0) {
                            continue;
                        }
                        let matched = 'regular: {
                            let small_data = match try_load_small_regular_file_bytes(file, io_mode)
                            {
                                Ok(data) => data,
                                Err(e) => {
                                    fgrep_report_input_error(file, &e, options);
                                    saw_error = true;
                                    break 'regular false;
                                }
                            };
                            if let Some(data) = small_data {
                                total_bytes += data.len() as u64;
                                handle_loaded_match_result(
                                    &mut out,
                                    Some(file.as_str()),
                                    data.as_slice(),
                                    pattern,
                                    &patterns,
                                    regular_file_path,
                                    None,
                                    multi_file,
                                    options,
                                )?
                            } else if matches!(
                                regular_file_path,
                                FgrepRegularFilePath::LiteralSearchOffsets
                            ) && !fgrep_context_enabled(options)
                            {
                                let cfg = config.get_or_insert_with(|| load_config(None));
                                let file_len = match fs::metadata(file) {
                                    Ok(metadata) => metadata.len(),
                                    Err(e) => {
                                        fgrep_report_input_error(file, &e, options);
                                        saw_error = true;
                                        break 'regular false;
                                    }
                                };
                                total_bytes += file_len;
                                let (matches, _) = match grep_match_offsets_for_mode(
                                    cfg,
                                    "grep",
                                    file,
                                    internal_io_mode(io_mode),
                                    pattern.raw.as_slice(),
                                ) {
                                    Ok(result) => result,
                                    Err(e) => {
                                        fgrep_report_input_error(file, &e, options);
                                        saw_error = true;
                                        break 'regular false;
                                    }
                                };
                                if matches.is_empty() && !options.invert_match {
                                    if options.count_only {
                                        write_count_line(
                                            &mut out,
                                            Some(file.as_str()),
                                            0,
                                            options,
                                            multi_file,
                                        )?;
                                    }
                                    break 'regular false;
                                }
                                let data = match load_file_to_memory_for_mode(
                                    cfg,
                                    "read_to_memory",
                                    file,
                                    internal_io_mode(io_mode),
                                ) {
                                    Ok(data) => data,
                                    Err(e) => {
                                        fgrep_report_input_error(file, &e, options);
                                        saw_error = true;
                                        break 'regular false;
                                    }
                                };
                                handle_loaded_match_result(
                                    &mut out,
                                    Some(file.as_str()),
                                    data.data.as_slice(),
                                    pattern,
                                    &patterns,
                                    regular_file_path,
                                    Some(&matches),
                                    multi_file,
                                    options,
                                )?
                            } else {
                                let data = match load_file_bytes(file, io_mode, "read_to_memory") {
                                    Ok(data) => data,
                                    Err(e) => {
                                        fgrep_report_input_error(file, &e, options);
                                        saw_error = true;
                                        break 'regular false;
                                    }
                                };
                                total_bytes += data.data.len() as u64;
                                handle_loaded_match_result(
                                    &mut out,
                                    Some(file.as_str()),
                                    data.data.as_slice(),
                                    pattern,
                                    &patterns,
                                    regular_file_path,
                                    None,
                                    multi_file,
                                    options,
                                )?
                            }
                        };
                        if matched {
                            matched_any = true;
                        }
                        if options.files_with_matches && matched {
                            write_filename_result(&mut out, Some(file.as_str()), options)?;
                        } else if options.files_without_match && !matched {
                            write_filename_result(&mut out, Some(file.as_str()), options)?;
                        }
                        continue;
                    }
                }
            }
            StreamInput::Stdin { label } => {
                let effective = stdin_label.as_deref().or(label.as_deref());
                if options.max_count == Some(0) {
                    continue;
                }
                let mut stream_options = options;
                if stream_options.initial_tab
                    && (multi_file
                        || matches!(stream_options.filename_mode, FgrepFilenameMode::Always))
                {
                    stream_options.offset_width = 20;
                }
                let result = if patterns.len() == 1 {
                    write_matching_stream_lines(
                        &mut out,
                        effective,
                        input,
                        io_mode,
                        pattern.raw.as_slice(),
                        pattern.normalized.as_slice(),
                        multi_file,
                        stream_options,
                    )
                } else {
                    write_matching_stream_lines_multi(
                        &mut out,
                        effective,
                        input,
                        io_mode,
                        &patterns,
                        multi_file,
                        stream_options,
                    )
                };
                match result {
                    Ok((matched, bytes)) => {
                        total_bytes += bytes;
                        if matched {
                            matched_any = true;
                        }
                        if options.files_with_matches && matched {
                            write_filename_result(&mut out, effective, stream_options)?;
                        } else if options.files_without_match && !matched {
                            write_filename_result(&mut out, effective, stream_options)?;
                        }
                    }
                    Err(e) => {
                        fgrep_report_input_error(fgrep_display_label(effective), &e, options);
                        saw_error = true;
                    }
                }
            }
        }
    }
    out.into_inner()?;
    if options.report_gbps {
        report_gbps("fgrep", total_bytes, started_at);
    }
    Ok(fgrep_exit_code(matched_any, saw_error, options))
}

#[cfg(kani)]
#[path = "fgrep/kani_proofs.rs"]
mod kani_proofs;

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "fgrep/tests_null_data.rs"]
mod tests_null_data;
