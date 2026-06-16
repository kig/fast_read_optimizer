use rand::RngExt;
use std::fs::File;
use std::io::{Read, Write};
use stringzilla::stringzilla as sz;

use self::compare::{compare_line_refs, debug_key_selections, same_sort_key};

const SORT_WRITE_BUFFER_SIZE: usize = 2 << 20;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct SortLineRef {
    start: usize,
    len: usize,
    sequence: u64,
}

impl SortLineRef {
    fn bytes<'a>(self, storage: &'a [u8]) -> &'a [u8] {
        &storage[self.start..self.start + self.len]
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SortMode {
    Bytewise,
    Numeric,
    GeneralNumeric,
    HumanNumeric,
    Month,
    Random,
    Version,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SortKeyPosition {
    field: usize,
    char_offset: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SortKeyEnd {
    EndOfLine,
    FieldEnd { field: usize },
    Char { field: usize, char_end: usize },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SortKeySpec {
    start: SortKeyPosition,
    end: SortKeyEnd,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SortComparator {
    mode: SortMode,
    keys: Vec<SortKeySpec>,
    field_separator: Option<u8>,
    stable: bool,
    dictionary_order: bool,
    ignore_case: bool,
    ignore_leading_blanks: bool,
    ignore_nonprinting: bool,
    random_seed: [u8; 16],
}

impl SortComparator {
    fn new(
        mode: SortMode,
        keys: Vec<SortKeySpec>,
        field_separator: Option<u8>,
        stable: bool,
        dictionary_order: bool,
        ignore_case: bool,
        ignore_leading_blanks: bool,
        ignore_nonprinting: bool,
    ) -> Self {
        Self {
            mode,
            keys,
            field_separator,
            stable,
            dictionary_order,
            ignore_case,
            ignore_leading_blanks,
            ignore_nonprinting,
            random_seed: [0; 16],
        }
    }

    fn with_random_seed(mut self, random_seed: [u8; 16]) -> Self {
        self.random_seed = random_seed;
        self
    }

    fn has_key_selection(&self) -> bool {
        !self.keys.is_empty()
    }

    fn preserves_input_order_on_equal_keys(&self, unique: bool) -> bool {
        self.stable || unique
    }

    fn uses_last_resort_line_order(&self, unique: bool) -> bool {
        !self.preserves_input_order_on_equal_keys(unique)
            && (self.mode != SortMode::Bytewise
                || self.has_key_selection()
                || self.dictionary_order
                || self.ignore_case
                || self.ignore_leading_blanks
                || self.ignore_nonprinting)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum RecordTerminator {
    #[default]
    Newline,
    Nul,
}

impl RecordTerminator {
    fn byte(self) -> u8 {
        match self {
            Self::Newline => b'\n',
            Self::Nul => b'\0',
        }
    }
}

fn sort_input_label(input: &StreamInput) -> &str {
    match input {
        StreamInput::File(path) => path.as_str(),
        StreamInput::Stdin { label } => label.as_deref().unwrap_or("-"),
    }
}

fn append_input_lines(
    storage: &mut Vec<u8>,
    lines: &mut Vec<SortLineRef>,
    input: &[u8],
    next_sequence: &mut u64,
    terminator: RecordTerminator,
) -> io::Result<()> {
    let base = storage.len();
    storage.extend_from_slice(input);

    let mut line_start = 0usize;
    let terminator = terminator.byte();
    for (idx, &byte) in input.iter().enumerate() {
        if byte == terminator {
            lines.push(SortLineRef {
                start: base + line_start,
                len: idx - line_start,
                sequence: *next_sequence,
            });
            *next_sequence = next_sequence
                .checked_add(1)
                .ok_or_else(|| io::Error::other("sort line sequence overflow"))?;
            line_start = idx + 1;
        }
    }
    if line_start < input.len() {
        lines.push(SortLineRef {
            start: base + line_start,
            len: input.len() - line_start,
            sequence: *next_sequence,
        });
        *next_sequence = next_sequence
            .checked_add(1)
            .ok_or_else(|| io::Error::other("sort line sequence overflow"))?;
    }

    Ok(())
}

fn render_sort_debug_message(offset: usize) -> Vec<u8> {
    let mut line = vec![b' '; offset];
    line.extend_from_slice(b"^ no match for key");
    line
}

fn render_sort_debug_underline(offset: usize, visual_len: usize) -> Vec<u8> {
    let mut line = vec![b' '; offset.saturating_add(visual_len)];
    let start = offset.min(line.len());
    let end = offset.saturating_add(visual_len).min(line.len());
    for byte in &mut line[start..end] {
        *byte = b'_';
    }
    line
}

fn emit_sort_debug_preamble(comparator: &SortComparator) -> io::Result<()> {
    let mut stderr = fro::command_io::stderr_buf_writer(4096)?;
    stderr.write_all(b"sort: text ordering performed using simple byte comparison\n")?;
    if comparator.keys.is_empty()
        || comparator.ignore_leading_blanks
        || comparator.field_separator.is_some()
    {
        return stderr.flush();
    }
    for (index, _) in comparator.keys.iter().enumerate() {
        writeln!(
            stderr,
            "sort: leading blanks are significant in key {}; consider also specifying 'b'",
            index + 1
        )?;
    }
    stderr.flush()
}

fn buffered_write_sort_debug_line<W: Write + ?Sized>(
    out: &mut W,
    buffer: &mut Vec<u8>,
    line: &[u8],
    comparator: &SortComparator,
    unique: bool,
    terminator: RecordTerminator,
) -> io::Result<()> {
    buffered_write_sort_line(out, buffer, line, terminator)?;
    for key in debug_key_selections(line, comparator) {
        let annotation = if key.matched {
            if key.preserve_offsets {
                render_sort_debug_underline(key.offset, key.visual_len)
            } else {
                vec![b'_'; key.visual_len]
            }
        } else {
            render_sort_debug_message(if key.preserve_offsets { key.offset } else { 0 })
        };
        buffered_write_sort_line(out, buffer, &annotation, terminator)?;
    }
    if comparator.uses_last_resort_line_order(unique) {
        let annotation = vec![b'_'; line.len()];
        buffered_write_sort_line(out, buffer, &annotation, terminator)?;
    }
    Ok(())
}

fn write_sorted_lines_with_debug<W: Write>(
    mut out: W,
    lines: &[SortLineRef],
    storage: &[u8],
    comparator: &SortComparator,
    unique: bool,
    terminator: RecordTerminator,
) -> io::Result<()> {
    let mut buffer = Vec::with_capacity(SORT_WRITE_BUFFER_SIZE);
    for line in lines {
        buffered_write_sort_debug_line(
            &mut out,
            &mut buffer,
            line.bytes(storage),
            comparator,
            unique,
            terminator,
        )?;
    }
    flush_sort_output_buffer(&mut out, &mut buffer)?;
    out.flush()
}

fn sort_line_refs(
    lines: &mut [SortLineRef],
    storage: &[u8],
    comparator: &SortComparator,
    unique: bool,
    reverse: bool,
) -> io::Result<()> {
    if lines.len() < 2 {
        return Ok(());
    }
    match comparator.mode {
        SortMode::Bytewise
            if !comparator.has_key_selection()
                && !comparator.dictionary_order
                && !comparator.ignore_case
                && !comparator.ignore_leading_blanks
                && !comparator.ignore_nonprinting =>
        {
            let snapshot = lines.to_vec();
            let mut order = vec![0 as sz::SortedIdx; snapshot.len()];
            sz::argsort_permutation_by(|idx| snapshot[idx].bytes(storage), &mut order).map_err(
                |status| io::Error::other(format!("StringZilla sort failed: {status:?}")),
            )?;
            for (dst, sorted_idx) in lines.iter_mut().zip(order.into_iter()) {
                *dst = snapshot[sorted_idx];
            }
            if reverse {
                lines.reverse();
            }
        }
        SortMode::Numeric
        | SortMode::GeneralNumeric
        | SortMode::HumanNumeric
        | SortMode::Month
        | SortMode::Random
        | SortMode::Version
        | SortMode::Bytewise => {
            lines.sort_unstable_by(|left, right| {
                compare_line_refs(*left, *right, storage, comparator, unique, reverse)
            });
        }
    }
    Ok(())
}

fn finalize_sorted_lines(
    lines: &mut Vec<SortLineRef>,
    storage: &[u8],
    comparator: &SortComparator,
    unique: bool,
    reverse: bool,
) -> io::Result<()> {
    sort_line_refs(lines, storage, comparator, unique, reverse)?;
    if unique {
        lines.dedup_by(|left, right| {
            same_sort_key(left.bytes(storage), right.bytes(storage), comparator)
        });
    }
    Ok(())
}

fn write_sorted_lines<W: Write>(
    mut out: W,
    lines: &[SortLineRef],
    storage: &[u8],
    terminator: RecordTerminator,
) -> io::Result<()> {
    let mut buffer = Vec::with_capacity(SORT_WRITE_BUFFER_SIZE);
    for line in lines {
        buffered_write_sort_line(&mut out, &mut buffer, line.bytes(storage), terminator)?;
    }
    flush_sort_output_buffer(&mut out, &mut buffer)?;
    out.flush()
}

fn buffered_write_sort_line<W: Write + ?Sized>(
    out: &mut W,
    buffer: &mut Vec<u8>,
    line: &[u8],
    terminator: RecordTerminator,
) -> io::Result<()> {
    let terminator = terminator.byte();
    if buffer.len() + line.len() + 1 > SORT_WRITE_BUFFER_SIZE && !buffer.is_empty() {
        flush_sort_output_buffer(out, buffer)?;
    }
    if line.len() + 1 >= SORT_WRITE_BUFFER_SIZE {
        out.write_all(line)?;
        out.write_all(&[terminator])?;
        return Ok(());
    }
    buffer.extend_from_slice(line);
    buffer.push(terminator);
    Ok(())
}

fn report_sort_disorder(
    label: &str,
    disorder: &external::SortCheckFailure,
    terminator: RecordTerminator,
) -> io::Result<()> {
    let mut stderr = fro::command_io::stderr_buf_writer(4096)?;
    write!(stderr, "sort: {label}:{}: disorder: ", disorder.line_number)?;
    stderr.write_all(String::from_utf8_lossy(&disorder.line).as_bytes())?;
    if terminator == RecordTerminator::Nul {
        stderr.write_all(b"\0")
    } else {
        stderr.write_all(b"\n")
    }
}

fn flush_sort_output_buffer<W: Write + ?Sized>(
    out: &mut W,
    buffer: &mut Vec<u8>,
) -> io::Result<()> {
    if !buffer.is_empty() {
        out.write_all(buffer)?;
        buffer.clear();
    }
    Ok(())
}

fn read_sort_files0_inputs<R: Read>(
    reader: &mut R,
    source_label: &str,
) -> io::Result<Vec<StreamInput>> {
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).map_err(|err| {
        io::Error::new(
            err.kind(),
            format!("read failed: {source_label}: {}", sort_os_error_text(&err)),
        )
    })?;

    let mut inputs = Vec::new();
    let mut start = 0usize;
    let mut entry_number = 1usize;
    while start < bytes.len() {
        let end = bytes[start..]
            .iter()
            .position(|&byte| byte == 0)
            .map(|offset| start + offset)
            .unwrap_or(bytes.len());
        let name = &bytes[start..end];
        if name.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{source_label}:{entry_number}: invalid zero-length file name"),
            ));
        }
        if name == b"-" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "when reading file names from stdin, no file name of '-' allowed",
            ));
        }
        inputs.push(StreamInput::File(
            String::from_utf8_lossy(name).into_owned(),
        ));
        start = end.saturating_add(1);
        entry_number = entry_number
            .checked_add(1)
            .ok_or_else(|| io::Error::other("sort files0 entry number overflow"))?;
    }

    if inputs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("no input from '{source_label}'"),
        ));
    }
    Ok(inputs)
}

fn sort_inputs_from_files0(files0_from: &str) -> io::Result<Vec<StreamInput>> {
    if files0_from == "-" {
        return read_sort_files0_inputs(&mut fro::command_io::stdin_file()?, files0_from);
    }

    let mut list_file = File::open(files0_from).map_err(|err| {
        io::Error::new(
            err.kind(),
            format!("open failed: {files0_from}: {}", sort_os_error_text(&err)),
        )
    })?;
    read_sort_files0_inputs(&mut list_file, files0_from)
}

fn random_seed_from_source(random_source: Option<&str>) -> io::Result<[u8; 16]> {
    let mut seed = [0u8; 16];
    let Some(path) = random_source else {
        rand::rng().fill(&mut seed);
        return Ok(seed);
    };

    let mut file = File::open(path).map_err(|err| {
        io::Error::new(
            err.kind(),
            format!("open failed: {path}: {}", sort_os_error_text(&err)),
        )
    })?;
    if let Err(err) = file.read_exact(&mut seed) {
        return Err(if err.kind() == io::ErrorKind::UnexpectedEof {
            io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("'{path}': end of file"),
            )
        } else {
            io::Error::new(
                err.kind(),
                format!("read failed: {path}: {}", sort_os_error_text(&err)),
            )
        });
    }
    Ok(seed)
}

fn sort_os_error_text(err: &io::Error) -> String {
    err.raw_os_error()
        .map(|code| unsafe { std::ffi::CStr::from_ptr(libc::strerror(code)) })
        .map(|cstr| cstr.to_string_lossy().into_owned())
        .unwrap_or_else(|| err.to_string())
}
