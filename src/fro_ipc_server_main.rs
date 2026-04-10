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
mod uring_util;
mod verified_copy;
mod writer;

use std::env;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::mem::{size_of, zeroed};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process;
use std::time::Instant;

const DEFAULT_SOCKET_PATH: &str = "/tmp/fro-ipc.sock";
const MAX_REQUEST_BYTES: usize = 16384;
const EXPECTED_FD_COUNT: usize = 4;
const CONTROL_LEN: usize = 32;
const CMSG_HEADER_LEN: usize = 16;

fn usage(program: &str) {
    fro::cio_eprintln!("USAGE: {program} [socket_path]");
}

fn use_seqpacket_socket() -> bool {
    env::var_os("FRO_IPC_SOCKET_TYPE").is_some_and(|value| value == "seqpacket")
}

fn bind_listener(socket_path: &Path) -> io::Result<UnixListener> {
    if !use_seqpacket_socket() {
        return UnixListener::bind(socket_path);
    }

    let bytes = socket_path.as_os_str().as_bytes();
    if bytes.len() >= 108 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("socket path too long: {}", socket_path.display()),
        ));
    }

    let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_SEQPACKET | libc::SOCK_CLOEXEC, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }

    let mut addr: libc::sockaddr_un = unsafe { zeroed() };
    addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
    unsafe {
        std::ptr::copy_nonoverlapping(
            bytes.as_ptr().cast(),
            addr.sun_path.as_mut_ptr(),
            bytes.len(),
        );
    }
    addr.sun_path[bytes.len()] = 0;
    let addr_len = (size_of::<libc::sa_family_t>() + bytes.len() + 1) as libc::socklen_t;

    let bind_result =
        unsafe { libc::bind(fd, (&addr as *const libc::sockaddr_un).cast(), addr_len) };
    if bind_result != 0 {
        let err = io::Error::last_os_error();
        unsafe {
            libc::close(fd);
        }
        return Err(err);
    }
    if unsafe { libc::listen(fd, 128) } != 0 {
        let err = io::Error::last_os_error();
        unsafe {
            libc::close(fd);
        }
        return Err(err);
    }
    Ok(unsafe { UnixListener::from_raw_fd(fd) })
}

struct SocketGuard {
    path: PathBuf,
}

impl Drop for SocketGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

struct Request {
    args: Vec<String>,
    stdin_fd: OwnedFd,
    stdout_fd: OwnedFd,
    stderr_fd: OwnedFd,
    cwd_fd: OwnedFd,
}

fn recv_request(stream: &UnixStream) -> io::Result<Option<Request>> {
    let mut payload = [0u8; MAX_REQUEST_BYTES];
    let mut control = [0u8; CONTROL_LEN];
    let mut iov = libc::iovec {
        iov_base: payload.as_mut_ptr().cast(),
        iov_len: payload.len(),
    };
    let mut header: libc::msghdr = unsafe { zeroed() };
    header.msg_iov = &mut iov;
    header.msg_iovlen = 1;
    header.msg_control = control.as_mut_ptr().cast();
    header.msg_controllen = control.len();

    let read = unsafe { libc::recvmsg(stream.as_raw_fd(), &mut header, 0) };
    if read < 0 {
        return Err(io::Error::last_os_error());
    }
    let read = read as usize;
    if read == 0 {
        return Ok(None);
    }
    if read < size_of::<u64>() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "request too short",
        ));
    }

    let mut received = Vec::<RawFd>::new();
    unsafe {
        let mut cmsg = libc::CMSG_FIRSTHDR(&header);
        while !cmsg.is_null() {
            if (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SCM_RIGHTS {
                let data = libc::CMSG_DATA(cmsg).cast::<RawFd>();
                let data_len = ((*cmsg).cmsg_len as usize).saturating_sub(CMSG_HEADER_LEN);
                let count = data_len / size_of::<RawFd>();
                for index in 0..count {
                    received.push(*data.add(index));
                }
            }
            cmsg = libc::CMSG_NXTHDR(&header, cmsg);
        }
    }
    if received.len() != EXPECTED_FD_COUNT {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected three passed fds",
        ));
    }

    let argc = u64::from_ne_bytes(payload[..8].try_into().unwrap()) as usize;
    let mut args = Vec::with_capacity(argc);
    let mut cursor = 8usize;
    for _ in 0..argc {
        if cursor >= read {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "argv payload truncated",
            ));
        }
        let end = payload[cursor..read]
            .iter()
            .position(|&byte| byte == 0)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing arg terminator"))?;
        let next = cursor + end;
        args.push(
            String::from_utf8(payload[cursor..next].to_vec())
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "arg utf8"))?,
        );
        cursor = next + 1;
    }
    if args.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "missing argv[0]",
        ));
    }

    Ok(Some(Request {
        args,
        stdin_fd: unsafe { OwnedFd::from_raw_fd(received[0]) },
        stdout_fd: unsafe { OwnedFd::from_raw_fd(received[1]) },
        stderr_fd: unsafe { OwnedFd::from_raw_fd(received[2]) },
        cwd_fd: unsafe { OwnedFd::from_raw_fd(received[3]) },
    }))
}

fn broken_pipe_panic(payload: &(dyn std::any::Any + Send)) -> bool {
    payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&'static str>().copied())
        .is_some_and(|message| message.contains("Broken pipe"))
}

fn append_log_line(var_name: &str, line: &str) {
    let Some(path) = env::var_os(var_name) else {
        return;
    };
    let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) else {
        return;
    };
    let _ = file.write_all(line.as_bytes());
    let _ = file.write_all(b"\n");
}

fn format_call_log(args: &[String]) -> String {
    let command = Path::new(args[0].as_str())
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| args[0].clone());
    if args.len() == 1 {
        command
    } else {
        format!("{command} {}", args[1..].join(" "))
    }
}

fn cwd_path_from_fd(fd: RawFd) -> io::Result<PathBuf> {
    fs::read_link(format!("/proc/self/fd/{fd}"))
}

fn normalize_ipc_path(path: &str, cwd: &Path) -> String {
    if path == "-" {
        return path.to_string();
    }
    let path = Path::new(path);
    if path.is_absolute() {
        path.to_string_lossy().into_owned()
    } else {
        cwd.join(path).to_string_lossy().into_owned()
    }
}

fn normalize_flag_value_path(
    normalized: &mut Vec<String>,
    args: &[String],
    index: &mut usize,
    cwd: &Path,
) -> io::Result<()> {
    normalized.push(args[*index].clone());
    *index += 1;
    let value = args
        .get(*index)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing path flag value"))?;
    normalized.push(normalize_ipc_path(value, cwd));
    Ok(())
}

fn normalize_find_args(args: &[String], cwd: &Path) -> Vec<String> {
    let mut normalized = vec![args[0].clone()];
    let mut saw_expression = false;
    for arg in args.iter().skip(1) {
        let expression_start = arg.starts_with('-') || matches!(arg.as_str(), "!" | "(" | ")");
        if !saw_expression && !expression_start {
            normalized.push(normalize_ipc_path(arg, cwd));
        } else {
            saw_expression = true;
            normalized.push(arg.clone());
        }
    }
    normalized
}

fn normalize_fgrep_args(args: &[String], cwd: &Path) -> io::Result<Vec<String>> {
    let mut normalized = vec![args[0].clone()];
    let mut end_flags = false;
    let mut saw_pattern = false;
    let mut i = 1usize;
    while i < args.len() {
        let arg = &args[i];
        if !end_flags && arg == "--" {
            end_flags = true;
            normalized.push(arg.clone());
        } else if !end_flags && matches!(arg.as_str(), "-e" | "-f") {
            normalized.push(arg.clone());
            i += 1;
            let value = args.get(i).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "missing fgrep value")
            })?;
            if arg == "-f" {
                normalized.push(normalize_ipc_path(value, cwd));
            } else {
                normalized.push(value.clone());
            }
            saw_pattern = true;
        } else if !end_flags && arg.starts_with('-') {
            normalized.push(arg.clone());
        } else if !saw_pattern {
            normalized.push(arg.clone());
            saw_pattern = true;
        } else {
            normalized.push(normalize_ipc_path(arg, cwd));
        }
        i += 1;
    }
    Ok(normalized)
}

fn normalize_head_tail_args(args: &[String], cwd: &Path) -> io::Result<Vec<String>> {
    let mut normalized = vec![args[0].clone()];
    let mut end_flags = false;
    let mut i = 1usize;
    while i < args.len() {
        let arg = &args[i];
        if !end_flags && arg == "--" {
            end_flags = true;
            normalized.push(arg.clone());
        } else if !end_flags && matches!(arg.as_str(), "-n" | "-c" | "--lines" | "--bytes") {
            normalized.push(arg.clone());
            i += 1;
            normalized.push(
                args.get(i)
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing count"))?
                    .clone(),
            );
        } else if !end_flags && (arg.starts_with("--lines=") || arg.starts_with("--bytes=")) {
            normalized.push(arg.clone());
        } else if !end_flags && arg.starts_with('-') {
            normalized.push(arg.clone());
        } else {
            normalized.push(normalize_ipc_path(arg, cwd));
        }
        i += 1;
    }
    Ok(normalized)
}

fn normalize_basic_path_args(
    command: &str,
    args: &[String],
    cwd: &Path,
) -> io::Result<Vec<String>> {
    let mut normalized = vec![args[0].clone()];
    let mut end_flags = false;
    let mut i = 1usize;
    while i < args.len() {
        let arg = &args[i];
        if !end_flags && arg == "--" {
            end_flags = true;
            normalized.push(arg.clone());
        } else if command == "cp"
            && !end_flags
            && matches!(arg.as_str(), "-t" | "--target-directory")
        {
            normalize_flag_value_path(&mut normalized, args, &mut i, cwd)?;
        } else if command == "cp" && !end_flags && arg.starts_with("--target-directory=") {
            normalized.push(format!(
                "--target-directory={}",
                normalize_ipc_path(&arg["--target-directory=".len()..], cwd)
            ));
        } else if !end_flags && arg.starts_with('-') {
            normalized.push(arg.clone());
        } else {
            normalized.push(normalize_ipc_path(arg, cwd));
        }
        i += 1;
    }
    Ok(normalized)
}

fn normalize_request_args(args: &[String], cwd: &Path) -> io::Result<Vec<String>> {
    let Some(command) = Path::new(args.first().map(String::as_str).unwrap_or_default())
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
    else {
        return Ok(args.to_vec());
    };
    match command.as_str() {
        "tar" => {
            let mut normalized = Vec::with_capacity(args.len() + 1);
            normalized.push(args[0].clone());
            normalized.push(format!("--fro-cwd={}", cwd.display()));
            normalized.extend(args.iter().skip(1).cloned());
            Ok(normalized)
        }
        "fgrep" => normalize_fgrep_args(args, cwd),
        "find" => Ok(normalize_find_args(args, cwd)),
        "head" | "tail" => normalize_head_tail_args(args, cwd),
        "cat" | "cksum" | "cp" | "mv" | "rm" | "sort" | "wc" => {
            normalize_basic_path_args(command.as_str(), args, cwd)
        }
        _ => Ok(args.to_vec()),
    }
}

fn handle_request(request: Request) -> u8 {
    let Request {
        args,
        stdin_fd,
        stdout_fd,
        stderr_fd,
        cwd_fd,
    } = request;
    let call_line = format_call_log(&args);
    append_log_line("FRO_CALL_LOG", &call_line);

    let cwd = match cwd_path_from_fd(cwd_fd.as_raw_fd()) {
        Ok(cwd) => cwd,
        Err(err) => {
            fro::cio_eprintln!("Error: {err}");
            return 1;
        }
    };
    let dispatch_args = match normalize_request_args(&args, &cwd) {
        Ok(args) => args,
        Err(err) => {
            fro::cio_eprintln!("Error: {err}");
            return 1;
        }
    };
    let started = Instant::now();
    let command_io = fro::command_io::CommandIo::new(stdin_fd, stdout_fd, stderr_fd);
    let code = fro::command_io::with_command_io(command_io, || {
        match std::panic::catch_unwind(|| main_app::run_with_args(dispatch_args.clone())) {
            Ok(Ok(code)) => code,
            Ok(Err(err)) if err.kind() == io::ErrorKind::BrokenPipe => 0,
            Ok(Err(err)) => {
                fro::cio_eprintln!("Error: {err}");
                1
            }
            Err(payload) if broken_pipe_panic(payload.as_ref()) => 0,
            Err(payload) => std::panic::resume_unwind(payload),
        }
    });
    let elapsed = started.elapsed().as_nanos();
    let command = Path::new(args[0].as_str())
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| args[0].clone());
    append_log_line("FRO_IPC_TIMING_LOG", &format!("{command} {elapsed} {code}"));
    code as u8
}

fn handle_client(mut stream: UnixStream) -> io::Result<()> {
    loop {
        let Some(request) = recv_request(&stream)? else {
            return Ok(());
        };
        let status = handle_request(request);
        stream.write_all(&[status])?;
    }
}

fn spawn_client_thread(stream: UnixStream) {
    let Ok(thread_stream) = stream.try_clone() else {
        let _ = handle_client(stream);
        return;
    };
    if std::thread::Builder::new()
        .name("fro-ipc-client".to_string())
        .spawn(move || {
            let _ = handle_client(thread_stream);
        })
        .is_err()
    {
        let _ = handle_client(stream);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_request_args_rewrites_cp_paths_against_cwd() {
        let cwd = Path::new("/tmp/fro-ipc");
        let args = vec![
            "cp".to_string(),
            "-a".to_string(),
            "src/file.txt".to_string(),
            "dst/file.txt".to_string(),
        ];
        let normalized = normalize_request_args(&args, cwd).unwrap();
        assert_eq!(normalized[2], "/tmp/fro-ipc/src/file.txt");
        assert_eq!(normalized[3], "/tmp/fro-ipc/dst/file.txt");
    }

    #[test]
    fn normalize_request_args_injects_tar_cwd_flag() {
        let cwd = Path::new("/tmp/fro-ipc");
        let args = vec![
            "tar".to_string(),
            "-cf".to_string(),
            "out.tar".to_string(),
            ".".to_string(),
        ];
        let normalized = normalize_request_args(&args, cwd).unwrap();
        assert_eq!(normalized[1], "--fro-cwd=/tmp/fro-ipc");
        assert_eq!(normalized[2], "-cf");
        assert_eq!(normalized[4], ".");
    }
}

fn main() {
    let mut args = env::args_os();
    let program = args
        .next()
        .unwrap_or_else(|| "fro-ipc-server".into())
        .to_string_lossy()
        .into_owned();
    let socket_path = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_SOCKET_PATH));
    if args.next().is_some() {
        usage(&program);
        process::exit(1);
    }

    if let Some(parent) = socket_path.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            fro::cio_eprintln!(
                "{program}: parent directory does not exist: {}",
                parent.display()
            );
            process::exit(1);
        }
    }
    if Path::new(&socket_path).exists() && fs::remove_file(&socket_path).is_err() {
        fro::cio_eprintln!("{program}: failed to remove existing socket");
        process::exit(1);
    }

    let listener = match bind_listener(&socket_path) {
        Ok(listener) => listener,
        Err(err) => {
            fro::cio_eprintln!("{program}: {err}");
            process::exit(1);
        }
    };
    let _guard = SocketGuard {
        path: socket_path.clone(),
    };
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                spawn_client_thread(stream);
            }
            Err(_) => continue,
        }
    }
}
