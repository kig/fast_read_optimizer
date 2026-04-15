use super::*;

pub(super) fn disk_usage_kib(blocks: u64) -> u64 {
    blocks.div_ceil(2)
}

pub(super) fn du_bytes_kib(bytes: u64) -> u64 {
    bytes.div_ceil(1024)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DuUsageMode {
    DiskBlocks,
    ApparentBytes,
}

pub(super) fn du_usage_bytes(amount: u64, usage_mode: DuUsageMode) -> u64 {
    match usage_mode {
        DuUsageMode::DiskBlocks => amount.saturating_mul(512),
        DuUsageMode::ApparentBytes => amount,
    }
}

pub(super) fn du_usage_display_units(amount: u64, usage_mode: DuUsageMode, block_size: u64) -> u64 {
    (du_usage_bytes(amount, usage_mode) as u128).div_ceil(block_size as u128) as u64
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DuDisplayFormat {
    Kib,
    HumanReadableIec,
    HumanReadableSi,
    BlockSize(u64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DuLineTerminator {
    Newline,
    Nul,
}

impl DuLineTerminator {
    pub(super) fn byte(self) -> u8 {
        match self {
            Self::Newline => b'\n',
            Self::Nul => b'\0',
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DuDereferenceMode {
    None,
    Args,
    All,
}

impl DuDereferenceMode {
    pub(super) fn follow_root(self) -> bool {
        !matches!(self, Self::None)
    }

    pub(super) fn follow_children(self) -> bool {
        matches!(self, Self::All)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DuThreshold {
    Min(u64),
    Max(u64),
}

pub(super) fn du_format_human_bytes(bytes: u64, unit_base: u64, units: &[&str]) -> String {
    if bytes < unit_base {
        return bytes.to_string();
    }
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= unit_base as f64 && unit + 1 < units.len() - 1 {
        value /= unit_base as f64;
        unit += 1;
    }
    if value >= 10.0 {
        format!("{}{}", value.ceil() as u64, units[unit])
    } else {
        let rounded_up = (value * 10.0).ceil() / 10.0;
        if rounded_up >= 10.0 {
            format!("{}{}", rounded_up as u64, units[unit])
        } else {
            format!("{rounded_up:.1}{}", units[unit])
        }
    }
}

pub(super) fn du_format_usage(
    amount: u64,
    usage_mode: DuUsageMode,
    display_format: DuDisplayFormat,
) -> String {
    match display_format {
        DuDisplayFormat::Kib => match usage_mode {
            DuUsageMode::DiskBlocks => disk_usage_kib(amount).to_string(),
            DuUsageMode::ApparentBytes => du_bytes_kib(amount).to_string(),
        },
        DuDisplayFormat::HumanReadableIec => du_format_human_bytes(
            du_usage_bytes(amount, usage_mode),
            1024,
            &["", "K", "M", "G", "T", "P", "E", "Z", "Y"],
        ),
        DuDisplayFormat::HumanReadableSi => du_format_human_bytes(
            du_usage_bytes(amount, usage_mode),
            1000,
            &["", "k", "M", "G", "T", "P", "E", "Z", "Y"],
        ),
        DuDisplayFormat::BlockSize(block_size) => {
            du_usage_display_units(amount, usage_mode, block_size).to_string()
        }
    }
}

pub(super) fn append_du_line(
    chunk: &mut Vec<u8>,
    amount: u64,
    path: &Path,
    usage_mode: DuUsageMode,
    display_format: DuDisplayFormat,
    line_terminator: DuLineTerminator,
) {
    chunk.extend_from_slice(du_format_usage(amount, usage_mode, display_format).as_bytes());
    chunk.push(b'\t');
    chunk.extend_from_slice(path.as_os_str().as_bytes());
    chunk.push(line_terminator.byte());
}

pub(super) fn du_depth_included(depth: usize, max_depth: Option<usize>) -> bool {
    max_depth.is_none_or(|limit| depth <= limit)
}

pub(super) fn du_display_total_blocks(
    separate_dirs: bool,
    exclusive_blocks: u64,
    subtree_total_blocks: u64,
) -> u64 {
    if separate_dirs {
        exclusive_blocks
    } else {
        subtree_total_blocks
    }
}

pub(super) fn parse_du_max_depth(value: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|_| format!("invalid maximum depth ‘{value}’"))
}

pub(super) fn parse_du_block_size(value: &str) -> Result<u64, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(format!("invalid --block-size argument '{value}'"));
    }
    let lower = trimmed.to_ascii_lowercase();
    let split = lower
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(lower.len());
    if split == 0 {
        return Err(format!("invalid --block-size argument '{value}'"));
    }
    let amount = lower[..split]
        .parse::<u64>()
        .map_err(|_| format!("invalid --block-size argument '{value}'"))?;
    let multiplier = match lower[split..].trim() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024_u64.pow(2),
        "g" | "gb" | "gib" => 1024_u64.pow(3),
        "t" | "tb" | "tib" => 1024_u64.pow(4),
        _ => return Err(format!("invalid --block-size argument '{value}'")),
    };
    amount
        .checked_mul(multiplier)
        .filter(|size| *size > 0)
        .ok_or_else(|| format!("invalid --block-size argument '{value}'"))
}

pub(super) fn parse_du_threshold(value: &str) -> Result<DuThreshold, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(format!("invalid --threshold argument '{value}'"));
    }
    let (negative, magnitude) = if let Some(rest) = trimmed.strip_prefix('-') {
        (true, rest)
    } else if let Some(rest) = trimmed.strip_prefix('+') {
        (false, rest)
    } else {
        (false, trimmed)
    };
    let threshold = parse_du_block_size(magnitude)
        .map_err(|_| format!("invalid --threshold argument '{value}'"))?;
    Ok(if negative {
        DuThreshold::Max(threshold)
    } else {
        DuThreshold::Min(threshold)
    })
}

pub(super) fn du_threshold_includes(
    amount: u64,
    usage_mode: DuUsageMode,
    threshold: Option<DuThreshold>,
) -> bool {
    let bytes = du_usage_bytes(amount, usage_mode);
    match threshold {
        Some(DuThreshold::Min(minimum)) => bytes >= minimum,
        Some(DuThreshold::Max(maximum)) => bytes <= maximum,
        None => true,
    }
}

pub(super) fn write_du_stderr_line(message: &str) {
    let mut stderr = fro::command_io::stderr_buf_writer(4096).unwrap();
    let _ = writeln!(stderr, "du: {message}");
}

pub(super) fn write_du_try_help() {
    let mut stderr = fro::command_io::stderr_buf_writer(4096).unwrap();
    let _ = writeln!(stderr, "Try 'du --help' for more information.");
}

pub(super) fn du_apply_short_flag(
    summarize: bool,
    all: bool,
    display_format: DuDisplayFormat,
    usage_mode: DuUsageMode,
    total: bool,
    separate_dirs: bool,
    dereference_mode: DuDereferenceMode,
    line_terminator: DuLineTerminator,
    flag: u8,
) -> io::Result<(
    bool,
    bool,
    DuDisplayFormat,
    DuUsageMode,
    bool,
    bool,
    DuDereferenceMode,
    DuLineTerminator,
)> {
    match flag {
        b's' => Ok((
            true,
            all,
            display_format,
            usage_mode,
            total,
            separate_dirs,
            dereference_mode,
            line_terminator,
        )),
        b'a' => Ok((
            summarize,
            true,
            display_format,
            usage_mode,
            total,
            separate_dirs,
            dereference_mode,
            line_terminator,
        )),
        b'h' => Ok((
            summarize,
            all,
            DuDisplayFormat::HumanReadableIec,
            usage_mode,
            total,
            separate_dirs,
            dereference_mode,
            line_terminator,
        )),
        b'k' => Ok((
            summarize,
            all,
            DuDisplayFormat::BlockSize(1024),
            usage_mode,
            total,
            separate_dirs,
            dereference_mode,
            line_terminator,
        )),
        b'm' => Ok((
            summarize,
            all,
            DuDisplayFormat::BlockSize(1024_u64.pow(2)),
            usage_mode,
            total,
            separate_dirs,
            dereference_mode,
            line_terminator,
        )),
        b'c' => Ok((
            summarize,
            all,
            display_format,
            usage_mode,
            true,
            separate_dirs,
            dereference_mode,
            line_terminator,
        )),
        b'S' => Ok((
            summarize,
            all,
            display_format,
            usage_mode,
            total,
            true,
            dereference_mode,
            line_terminator,
        )),
        b'b' => Ok((
            summarize,
            all,
            DuDisplayFormat::BlockSize(1),
            DuUsageMode::ApparentBytes,
            total,
            separate_dirs,
            dereference_mode,
            line_terminator,
        )),
        b'D' | b'H' => Ok((
            summarize,
            all,
            display_format,
            usage_mode,
            total,
            separate_dirs,
            DuDereferenceMode::Args,
            line_terminator,
        )),
        b'L' => Ok((
            summarize,
            all,
            display_format,
            usage_mode,
            total,
            separate_dirs,
            DuDereferenceMode::All,
            line_terminator,
        )),
        b'P' => Ok((
            summarize,
            all,
            display_format,
            usage_mode,
            total,
            separate_dirs,
            DuDereferenceMode::None,
            line_terminator,
        )),
        b'0' => Ok((
            summarize,
            all,
            display_format,
            usage_mode,
            total,
            separate_dirs,
            dereference_mode,
            DuLineTerminator::Nul,
        )),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unsupported du flag: -{}", other as char),
        )),
    }
}
