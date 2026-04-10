#![allow(dead_code)]

use std::cell::RefCell;
use std::fmt;
use std::fs::File;
use std::io::{self, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::rc::Rc;

pub struct CommandIo {
    stdin_fd: OwnedFd,
    stdout_fd: OwnedFd,
    stderr_fd: OwnedFd,
}

impl CommandIo {
    pub fn new(stdin_fd: OwnedFd, stdout_fd: OwnedFd, stderr_fd: OwnedFd) -> Self {
        Self {
            stdin_fd,
            stdout_fd,
            stderr_fd,
        }
    }
}

thread_local! {
    static CURRENT_COMMAND_IO: RefCell<Option<Rc<CommandIo>>> = const { RefCell::new(None) };
}

enum StandardStream {
    Stdin,
    Stdout,
    Stderr,
}

fn default_raw_fd(stream: StandardStream) -> RawFd {
    match stream {
        StandardStream::Stdin => libc::STDIN_FILENO,
        StandardStream::Stdout => libc::STDOUT_FILENO,
        StandardStream::Stderr => libc::STDERR_FILENO,
    }
}

fn duplicate_fd(raw_fd: RawFd) -> io::Result<OwnedFd> {
    let duplicated = unsafe { libc::dup(raw_fd) };
    if duplicated < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { OwnedFd::from_raw_fd(duplicated) })
    }
}

fn duplicate_stream(stream: StandardStream) -> io::Result<OwnedFd> {
    CURRENT_COMMAND_IO.with(|current| {
        let current = current.borrow();
        let raw_fd = match current.as_ref() {
            Some(command_io) => match stream {
                StandardStream::Stdin => command_io.stdin_fd.as_raw_fd(),
                StandardStream::Stdout => command_io.stdout_fd.as_raw_fd(),
                StandardStream::Stderr => command_io.stderr_fd.as_raw_fd(),
            },
            None => default_raw_fd(stream),
        };
        duplicate_fd(raw_fd)
    })
}

fn current_raw_fd(stream: StandardStream) -> RawFd {
    CURRENT_COMMAND_IO.with(|current| {
        let current = current.borrow();
        match current.as_ref() {
            Some(command_io) => match stream {
                StandardStream::Stdin => command_io.stdin_fd.as_raw_fd(),
                StandardStream::Stdout => command_io.stdout_fd.as_raw_fd(),
                StandardStream::Stderr => command_io.stderr_fd.as_raw_fd(),
            },
            None => default_raw_fd(stream),
        }
    })
}

pub fn with_command_io<T>(command_io: CommandIo, f: impl FnOnce() -> T) -> T {
    CURRENT_COMMAND_IO.with(|current| {
        let previous = current.replace(Some(Rc::new(command_io)));
        let result = f();
        current.replace(previous);
        result
    })
}

pub fn stdin_fd() -> RawFd {
    current_raw_fd(StandardStream::Stdin)
}

pub fn stdout_fd() -> RawFd {
    current_raw_fd(StandardStream::Stdout)
}

pub fn stderr_fd() -> RawFd {
    current_raw_fd(StandardStream::Stderr)
}

pub fn stdin_file() -> io::Result<File> {
    duplicate_stream(StandardStream::Stdin).map(File::from)
}

pub fn stdout_file() -> io::Result<File> {
    duplicate_stream(StandardStream::Stdout).map(File::from)
}

pub fn stderr_file() -> io::Result<File> {
    duplicate_stream(StandardStream::Stderr).map(File::from)
}

pub fn stdout_buf_writer(capacity: usize) -> io::Result<io::BufWriter<File>> {
    Ok(io::BufWriter::with_capacity(capacity, stdout_file()?))
}

pub fn stderr_buf_writer(capacity: usize) -> io::Result<io::BufWriter<File>> {
    Ok(io::BufWriter::with_capacity(capacity, stderr_file()?))
}

pub fn write_stdout_fmt(args: fmt::Arguments<'_>) -> io::Result<()> {
    let mut stdout = stdout_file()?;
    stdout.write_fmt(args)?;
    stdout.flush()
}

pub fn write_stderr_fmt(args: fmt::Arguments<'_>) -> io::Result<()> {
    let mut stderr = stderr_file()?;
    stderr.write_fmt(args)?;
    stderr.flush()
}

#[macro_export]
macro_rules! cio_print {
    ($($arg:tt)*) => {{
        $crate::command_io::write_stdout_fmt(format_args!($($arg)*)).expect("failed printing to stdout");
    }};
}

#[macro_export]
macro_rules! cio_println {
    () => {{
        $crate::command_io::write_stdout_fmt(format_args!("\n")).expect("failed printing to stdout");
    }};
    ($($arg:tt)*) => {{
        $crate::command_io::write_stdout_fmt(format_args!("{}\n", format_args!($($arg)*)))
            .expect("failed printing to stdout");
    }};
}

#[macro_export]
macro_rules! cio_eprint {
    ($($arg:tt)*) => {{
        $crate::command_io::write_stderr_fmt(format_args!($($arg)*)).expect("failed printing to stderr");
    }};
}

#[macro_export]
macro_rules! cio_eprintln {
    () => {{
        $crate::command_io::write_stderr_fmt(format_args!("\n")).expect("failed printing to stderr");
    }};
    ($($arg:tt)*) => {{
        $crate::command_io::write_stderr_fmt(format_args!("{}\n", format_args!($($arg)*)))
            .expect("failed printing to stderr");
    }};
}
