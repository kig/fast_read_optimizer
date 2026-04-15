pub(super) fn sort_inputs_streamed(
    inputs: &[StreamInput],
    io_mode: IOMode,
    comparator: &SortComparator,
    unique: bool,
    reverse: bool,
    debug: bool,
    terminator: RecordTerminator,
    output_path: Option<&str>,
    memory_budget: u64,
    temporary_directory: Option<&Path>,
    compress_program: Option<&str>,
    batch_size: Option<usize>,
) -> io::Result<u64> {
    let chunk_target = spill_chunk_target_bytes(memory_budget);
    let mut spill = SpillSorter::new(
        comparator,
        unique,
        reverse,
        debug,
        terminator,
        chunk_target,
        temporary_directory,
        compress_program,
        batch_size,
    );
    let mut total_bytes = 0u64;
    for input in inputs {
        let bytes = visit_ordered_input_counted(input, io_mode, |block| spill.push_block(block))
            .map_err(|err| {
                io::Error::new(
                    err.kind(),
                    format!("cannot read '{}': {err}", sort_input_label(input)),
                )
            })?;
        total_bytes = total_bytes
            .checked_add(bytes)
            .ok_or_else(|| io::Error::other("sort input byte count overflow"))?;
    }
    spill.finish(output_path, io_mode)?;
    Ok(total_bytes)
}

pub(super) fn merge_presorted_inputs(
    inputs: &[StreamInput],
    io_mode: IOMode,
    comparator: &SortComparator,
    unique: bool,
    reverse: bool,
    debug: bool,
    terminator: RecordTerminator,
    output_path: Option<&str>,
    temporary_directory: Option<&Path>,
    compress_program: Option<&str>,
    batch_size: Option<usize>,
    _parallel_override: Option<usize>,
) -> io::Result<u64> {
    let mut temp_files = SpillTempFiles::new(temporary_directory)?;
    let mut next_sequence = 0u64;
    let mut total_bytes = 0u64;
    for input in inputs {
        let path = temp_files.next_chunk_path();
        total_bytes = total_bytes
            .checked_add(
                write_presorted_input_chunk(
                    &path,
                    input,
                    io_mode,
                    &mut next_sequence,
                    terminator,
                    compress_program,
                )
                .map_err(|err| {
                    io::Error::new(
                        err.kind(),
                        format!("cannot read '{}': {err}", sort_input_label(input)),
                    )
                })?,
            )
            .ok_or_else(|| io::Error::other("sort merge input byte count overflow"))?;
        temp_files.paths.push(path);
    }
    let paths = temp_files.paths.clone();
    if debug {
        emit_sort_debug_preamble(comparator)?;
    }
    with_output_writer(output_path, io_mode, |out| {
        merge_sorted_chunks_batched(
            out,
            paths.clone(),
            comparator,
            unique,
            reverse,
            debug,
            terminator,
            compress_program,
            batch_size,
            &mut temp_files,
        )
    })?;
    Ok(total_bytes)
}

struct SpillSorter {
    comparator: SortComparator,
    unique: bool,
    reverse: bool,
    debug: bool,
    terminator: RecordTerminator,
    chunk_target: usize,
    storage: Vec<u8>,
    lines: Vec<SortLineRef>,
    carry: Vec<u8>,
    next_sequence: u64,
    temporary_directory: Option<PathBuf>,
    compress_program: Option<String>,
    batch_size: Option<usize>,
    temp_files: Option<SpillTempFiles>,
}

impl SpillSorter {
    fn new(
        comparator: &SortComparator,
        unique: bool,
        reverse: bool,
        debug: bool,
        terminator: RecordTerminator,
        chunk_target: usize,
        temporary_directory: Option<&Path>,
        compress_program: Option<&str>,
        batch_size: Option<usize>,
    ) -> Self {
        Self {
            comparator: comparator.clone(),
            unique,
            reverse,
            debug,
            terminator,
            chunk_target,
            storage: Vec::new(),
            lines: Vec::new(),
            carry: Vec::new(),
            next_sequence: 0,
            temporary_directory: temporary_directory.map(Path::to_path_buf),
            compress_program: compress_program.map(str::to_owned),
            batch_size,
            temp_files: None,
        }
    }

    fn push_block(&mut self, block: &[u8]) -> io::Result<()> {
        let mut consumed = 0usize;
        for separator in memchr_iter(self.terminator.byte(), block) {
            self.carry.extend_from_slice(&block[consumed..separator]);
            self.push_complete_line()?;
            consumed = separator + 1;
        }
        self.carry.extend_from_slice(&block[consumed..]);
        Ok(())
    }

    fn push_complete_line(&mut self) -> io::Result<()> {
        let start = self.storage.len();
        self.storage.extend_from_slice(&self.carry);
        self.lines.push(SortLineRef {
            start,
            len: self.carry.len(),
            sequence: self.next_sequence,
        });
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| io::Error::other("sort line sequence overflow"))?;
        self.carry.clear();
        if self.storage.len() >= self.chunk_target {
            self.flush_chunk()?;
        }
        Ok(())
    }

    fn flush_chunk(&mut self) -> io::Result<()> {
        if self.lines.is_empty() {
            return Ok(());
        }
        finalize_sorted_lines(
            &mut self.lines,
            &self.storage,
            &self.comparator,
            self.unique,
            self.reverse,
        )?;
        if self.temp_files.is_none() {
            self.temp_files = Some(SpillTempFiles::new(self.temporary_directory.as_deref())?);
        }
        let temp_files = self
            .temp_files
            .as_mut()
            .ok_or_else(|| io::Error::other("missing sort spill temp files"))?;
        let path = temp_files.next_chunk_path();
        write_chunk_file(
            &path,
            &self.lines,
            &self.storage,
            self.compress_program.as_deref(),
        )?;
        temp_files.paths.push(path);
        self.storage.clear();
        self.lines.clear();
        Ok(())
    }

    fn finish(mut self, output_path: Option<&str>, io_mode: IOMode) -> io::Result<()> {
        if !self.carry.is_empty() {
            self.push_complete_line()?;
        }
        if self.temp_files.is_none() {
            finalize_sorted_lines(
                &mut self.lines,
                &self.storage,
                &self.comparator,
                self.unique,
                self.reverse,
            )?;
            if self.debug {
                emit_sort_debug_preamble(&self.comparator)?;
            }
            return with_output_writer(output_path, io_mode, |out| {
                if self.debug {
                    write_sorted_lines_with_debug(
                        out,
                        &self.lines,
                        &self.storage,
                        &self.comparator,
                        self.unique,
                        RecordTerminator::Newline,
                    )
                } else {
                    write_sorted_lines(out, &self.lines, &self.storage, self.terminator)
                }
            });
        }
        self.flush_chunk()?;
        let mut temp_files = self
            .temp_files
            .take()
            .ok_or_else(|| io::Error::other("missing sort spill temp files"))?;
        let paths = temp_files.paths.clone();
        if self.debug {
            emit_sort_debug_preamble(&self.comparator)?;
        }
        with_output_writer(output_path, io_mode, |out| {
            merge_sorted_chunks_batched(
                out,
                paths.clone(),
                &self.comparator,
                self.unique,
                self.reverse,
                self.debug,
                self.terminator,
                self.compress_program.as_deref(),
                self.batch_size,
                &mut temp_files,
            )
        })
    }
}

struct SpillTempFiles {
    dir: PathBuf,
    paths: Vec<PathBuf>,
    next_index: usize,
}

impl SpillTempFiles {
    fn new(temporary_directory: Option<&Path>) -> io::Result<Self> {
        let base_dir = if let Some(path) = temporary_directory {
            match fs::metadata(path) {
                Ok(metadata) if metadata.is_dir() => path.to_path_buf(),
                Ok(_) => {
                    return Err(io::Error::other(format!(
                        "cannot create temporary file in '{}': Not a directory",
                        path.display()
                    )))
                }
                Err(err) => {
                    return Err(io::Error::new(
                        err.kind(),
                        format!(
                            "cannot create temporary file in '{}': {err}",
                            path.display()
                        ),
                    ))
                }
            }
        } else {
            std::env::temp_dir()
        };
        let mut dir = base_dir;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        dir.push(format!("fro-sort-{}-{timestamp}", std::process::id()));
        fs::create_dir(&dir).map_err(|err| {
            let parent =
                temporary_directory.unwrap_or_else(|| dir.parent().unwrap_or(Path::new(".")));
            io::Error::new(
                err.kind(),
                format!(
                    "cannot create temporary file in '{}': {err}",
                    parent.display()
                ),
            )
        })?;
        Ok(Self {
            dir,
            paths: Vec::new(),
            next_index: 0,
        })
    }

    fn next_chunk_path(&mut self) -> PathBuf {
        let path = self.dir.join(format!("chunk-{:06}.bin", self.next_index));
        self.next_index += 1;
        path
    }
}

impl Drop for SpillTempFiles {
    fn drop(&mut self) {
        for path in &self.paths {
            let _ = fs::remove_file(path);
        }
        let _ = fs::remove_dir(&self.dir);
    }
}

fn write_chunk_record<W: Write>(writer: &mut W, sequence: u64, line: &[u8]) -> io::Result<()> {
    writer.write_all(&sequence.to_le_bytes())?;
    writer.write_all(&(line.len() as u64).to_le_bytes())?;
    writer.write_all(line)
}

fn spawn_sort_chunk_command(
    program: &str,
    decompress: bool,
    stdin: Stdio,
    stdout: Stdio,
) -> io::Result<Child> {
    let command = if decompress {
        format!("{program} -d")
    } else {
        program.to_owned()
    };
    Command::new("sh")
        .arg("-c")
        .arg(command)
        .stdin(stdin)
        .stdout(stdout)
        .stderr(Stdio::inherit())
        .spawn()
}

fn sort_chunk_command_failed(program: &str, decompress: bool) -> io::Error {
    let suffix = if decompress { " [-d]" } else { "" };
    io::Error::other(format!("'{program}'{suffix} terminated abnormally"))
}

enum ChunkWriter {
    Plain(StdBufWriter<File>),
    Compressed {
        writer: StdBufWriter<ChildStdin>,
        child: Child,
        program: String,
    },
}

impl ChunkWriter {
    fn create(path: &Path, compress_program: Option<&str>) -> io::Result<Self> {
        match compress_program {
            Some(program) => {
                let output = File::create(path)?;
                let mut child =
                    spawn_sort_chunk_command(program, false, Stdio::piped(), Stdio::from(output))?;
                let stdin = child
                    .stdin
                    .take()
                    .ok_or_else(|| io::Error::other("missing sort chunk compressor stdin"))?;
                Ok(Self::Compressed {
                    writer: StdBufWriter::new(stdin),
                    child,
                    program: program.to_owned(),
                })
            }
            None => Ok(Self::Plain(StdBufWriter::new(File::create(path)?))),
        }
    }

    fn finish(self) -> io::Result<()> {
        match self {
            Self::Plain(mut writer) => writer.flush(),
            Self::Compressed {
                mut writer,
                mut child,
                program,
            } => {
                let flush_result = writer.flush();
                drop(writer);
                let wait_result = child.wait();
                match (flush_result, wait_result) {
                    (Err(err), _) => Err(err),
                    (_, Err(err)) => Err(err),
                    (Ok(()), Ok(status)) if status.success() => Ok(()),
                    (Ok(()), Ok(_)) => Err(sort_chunk_command_failed(&program, false)),
                }
            }
        }
    }
}

impl Write for ChunkWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Plain(writer) => writer.write(buf),
            Self::Compressed { writer, .. } => writer.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Plain(writer) => writer.flush(),
            Self::Compressed { writer, .. } => writer.flush(),
        }
    }
}

fn write_presorted_input_chunk(
    path: &Path,
    input: &StreamInput,
    io_mode: IOMode,
    next_sequence: &mut u64,
    terminator: RecordTerminator,
    compress_program: Option<&str>,
) -> io::Result<u64> {
    let mut writer = ChunkWriter::create(path, compress_program)?;
    let mut carry = Vec::new();
    let total_bytes = visit_ordered_input_counted(input, io_mode, |block| {
        let mut consumed = 0usize;
        for separator in memchr_iter(terminator.byte(), block) {
            carry.extend_from_slice(&block[consumed..separator]);
            write_chunk_record(&mut writer, *next_sequence, &carry)?;
            *next_sequence = next_sequence
                .checked_add(1)
                .ok_or_else(|| io::Error::other("sort line sequence overflow"))?;
            carry.clear();
            consumed = separator + 1;
        }
        carry.extend_from_slice(&block[consumed..]);
        Ok(())
    })?;
    if !carry.is_empty() {
        write_chunk_record(&mut writer, *next_sequence, &carry)?;
        *next_sequence = next_sequence
            .checked_add(1)
            .ok_or_else(|| io::Error::other("sort line sequence overflow"))?;
    }
    writer.finish()?;
    Ok(total_bytes)
}

fn write_chunk_file(
    path: &Path,
    lines: &[SortLineRef],
    storage: &[u8],
    compress_program: Option<&str>,
) -> io::Result<()> {
    let mut writer = ChunkWriter::create(path, compress_program)?;
    for line in lines {
        write_chunk_record(&mut writer, line.sequence, line.bytes(storage))?;
    }
    writer.finish()
}

struct ChunkReader {
    reader: ChunkReaderInner,
}

enum ChunkReaderInner {
    Plain(StdBufReader<File>),
    Compressed {
        reader: StdBufReader<ChildStdout>,
        child: Child,
        program: String,
        finished: bool,
    },
}

impl ChunkReader {
    fn open(path: &Path, compress_program: Option<&str>) -> io::Result<Self> {
        let reader = match compress_program {
            Some(program) => {
                let input = File::open(path)?;
                let mut child =
                    spawn_sort_chunk_command(program, true, Stdio::from(input), Stdio::piped())?;
                let stdout = child
                    .stdout
                    .take()
                    .ok_or_else(|| io::Error::other("missing sort chunk decompressor stdout"))?;
                ChunkReaderInner::Compressed {
                    reader: StdBufReader::with_capacity(SORT_STREAM_BLOCK_SIZE, stdout),
                    child,
                    program: program.to_owned(),
                    finished: false,
                }
            }
            None => ChunkReaderInner::Plain(StdBufReader::with_capacity(
                SORT_STREAM_BLOCK_SIZE,
                File::open(path)?,
            )),
        };
        Ok(Self { reader })
    }

    fn next_record(&mut self) -> io::Result<Option<ChunkRecord>> {
        match &mut self.reader {
            ChunkReaderInner::Plain(reader) => read_chunk_record(reader),
            ChunkReaderInner::Compressed {
                reader,
                child,
                program,
                finished,
            } => match read_chunk_record(reader)? {
                Some(record) => Ok(Some(record)),
                None => {
                    if !*finished {
                        *finished = true;
                        if !child.wait()?.success() {
                            return Err(sort_chunk_command_failed(program, true));
                        }
                    }
                    Ok(None)
                }
            },
        }
    }
}

impl Drop for ChunkReader {
    fn drop(&mut self) {
        if let ChunkReaderInner::Compressed {
            child, finished, ..
        } = &mut self.reader
        {
            if !*finished {
                let _ = child.wait();
            }
        }
    }
}

fn read_chunk_record<R: Read>(reader: &mut R) -> io::Result<Option<ChunkRecord>> {
    let mut header = [0u8; 16];
    let mut filled = 0usize;
    while filled < header.len() {
        let read = reader.read(&mut header[filled..])?;
        if read == 0 {
            if filled == 0 {
                return Ok(None);
            }
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated sort spill header",
            ));
        }
        filled += read;
    }
    let sequence = u64::from_le_bytes(header[..8].try_into().unwrap());
    let len = u64::from_le_bytes(header[8..].try_into().unwrap());
    let len = usize::try_from(len)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "sort spill line too large"))?;
    let mut line = vec![0u8; len];
    reader.read_exact(&mut line)?;
    Ok(Some(ChunkRecord { line, sequence }))
}

#[derive(Debug, Eq, PartialEq)]
struct ChunkRecord {
    line: Vec<u8>,
    sequence: u64,
}

#[derive(Debug, Eq, PartialEq)]
struct HeapItem {
    record: ChunkRecord,
    chunk_index: usize,
    comparator: SortComparator,
    unique: bool,
    reverse: bool,
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_output_lines(
            &self.record.line,
            self.record.sequence,
            &other.record.line,
            other.record.sequence,
            &self.comparator,
            self.unique,
            self.reverse,
        )
        .reverse()
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn merge_sorted_chunks(
    out: &mut dyn Write,
    paths: &[PathBuf],
    comparator: &SortComparator,
    unique: bool,
    reverse: bool,
    debug: bool,
    terminator: RecordTerminator,
    compress_program: Option<&str>,
) -> io::Result<()> {
    let mut output_buffer = Vec::with_capacity(SORT_WRITE_BUFFER_SIZE);
    merge_sorted_chunk_records(
        paths,
        comparator,
        unique,
        reverse,
        compress_program,
        |record| {
            if debug {
                buffered_write_sort_debug_line(
                    out,
                    &mut output_buffer,
                    &record.line,
                    comparator,
                    unique,
                    RecordTerminator::Newline,
                )
            } else {
                buffered_write_sort_line(out, &mut output_buffer, &record.line, terminator)
            }
        },
    )?;
    flush_sort_output_buffer(out, &mut output_buffer)?;
    out.flush()
}

fn merge_sorted_chunks_to_path(
    path: &Path,
    paths: &[PathBuf],
    comparator: &SortComparator,
    unique: bool,
    reverse: bool,
    compress_program: Option<&str>,
) -> io::Result<()> {
    let mut writer = ChunkWriter::create(path, compress_program)?;
    merge_sorted_chunk_records(
        paths,
        comparator,
        unique,
        reverse,
        compress_program,
        |record| write_chunk_record(&mut writer, record.sequence, &record.line),
    )?;
    writer.finish()
}

fn merge_sorted_chunks_batched(
    out: &mut dyn Write,
    mut paths: Vec<PathBuf>,
    comparator: &SortComparator,
    unique: bool,
    reverse: bool,
    debug: bool,
    terminator: RecordTerminator,
    compress_program: Option<&str>,
    batch_size: Option<usize>,
    temp_files: &mut SpillTempFiles,
) -> io::Result<()> {
    if let Some(limit) = batch_size {
        while paths.len() > limit {
            let mut next_paths = Vec::with_capacity(paths.len().div_ceil(limit));
            for group in paths.chunks(limit) {
                let path = temp_files.next_chunk_path();
                merge_sorted_chunks_to_path(
                    &path,
                    group,
                    comparator,
                    unique,
                    reverse,
                    compress_program,
                )?;
                temp_files.paths.push(path.clone());
                next_paths.push(path);
            }
            paths = next_paths;
        }
    }
    merge_sorted_chunks(
        out,
        &paths,
        comparator,
        unique,
        reverse,
        debug,
        terminator,
        compress_program,
    )
}

fn merge_sorted_chunk_records<F>(
    paths: &[PathBuf],
    comparator: &SortComparator,
    unique: bool,
    reverse: bool,
    compress_program: Option<&str>,
    mut write_record: F,
) -> io::Result<()>
where
    F: FnMut(&ChunkRecord) -> io::Result<()>,
{
    let mut readers = paths
        .iter()
        .map(|path| ChunkReader::open(path, compress_program))
        .collect::<io::Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    for (chunk_index, reader) in readers.iter_mut().enumerate() {
        if let Some(record) = reader.next_record()? {
            heap.push(HeapItem {
                record,
                chunk_index,
                comparator: comparator.clone(),
                unique,
                reverse,
            });
        }
    }

    let mut last_written: Option<Vec<u8>> = None;
    while let Some(item) = heap.pop() {
        let should_write = match &last_written {
            Some(previous) if unique => !same_sort_key(previous, &item.record.line, comparator),
            _ => true,
        };
        if should_write {
            write_record(&item.record)?;
            if unique {
                last_written = Some(item.record.line.clone());
            }
        }
        if let Some(record) = readers[item.chunk_index].next_record()? {
            heap.push(HeapItem {
                record,
                chunk_index: item.chunk_index,
                comparator: comparator.clone(),
                unique,
                reverse,
            });
        }
    }
    Ok(())
}
