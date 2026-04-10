use fro::{copy_fd_range_to_fd_with_progress, copy_path_range_to_fd_with_progress, ByteRange};
use std::env;
use std::fs;
use std::io::{self, Write};
use std::mem::{size_of, zeroed};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process;

const DEFAULT_SOCKET_PATH: &str = "/tmp/fro-cat.sock";
const MAX_REQUEST_BYTES: usize = 4096;
const EXPECTED_FD_COUNT: usize = 3;
const CONTROL_LEN: usize = 32;
const CMSG_HEADER_LEN: usize = 16;

fn usage(program: &str) {
    eprintln!("USAGE: {program} [socket_path]");
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
}

fn recv_request(stream: &UnixStream) -> io::Result<Request> {
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
    if argc > 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "prototype supports at most one file operand",
        ));
    }
    let args = if argc == 0 {
        Vec::new()
    } else {
        let rest = &payload[8..read];
        let nul = rest
            .iter()
            .position(|&byte| byte == 0)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing arg terminator"))?;
        vec![String::from_utf8(rest[..nul].to_vec())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "arg utf8"))?]
    };

    Ok(Request {
        args,
        stdin_fd: unsafe { OwnedFd::from_raw_fd(received[0]) },
        stdout_fd: unsafe { OwnedFd::from_raw_fd(received[1]) },
        stderr_fd: unsafe { OwnedFd::from_raw_fd(received[2]) },
    })
}

fn copy_stdin_to_stdout(stdin_fd: RawFd, stdout_fd: RawFd) -> io::Result<()> {
    let mut noop = |_bytes: u64| Ok(());
    if copy_fd_range_to_fd_with_progress(stdin_fd, stdout_fd, ByteRange::default(), &mut noop)?
        .is_some()
    {
        return Ok(());
    }

    let mut input = unsafe { fs::File::from_raw_fd(stdin_fd) };
    let mut output = unsafe { fs::File::from_raw_fd(stdout_fd) };
    io::copy(&mut input, &mut output)?;
    std::mem::forget(input);
    std::mem::forget(output);
    Ok(())
}

fn copy_file_to_stdout(path: &str, stdout_fd: RawFd) -> io::Result<()> {
    let mut noop = |_bytes: u64| Ok(());
    if copy_path_range_to_fd_with_progress(path, stdout_fd, ByteRange::default(), &mut noop)?
        .is_some()
    {
        return Ok(());
    }

    let mut input = fs::File::open(path)?;
    let mut output = unsafe { fs::File::from_raw_fd(stdout_fd) };
    io::copy(&mut input, &mut output)?;
    std::mem::forget(output);
    Ok(())
}

fn handle_request(request: Request) -> u8 {
    let stdin_fd = request.stdin_fd.as_raw_fd();
    let stdout_fd = request.stdout_fd.as_raw_fd();
    let result = match request.args.as_slice() {
        [] => copy_stdin_to_stdout(stdin_fd, stdout_fd),
        [path] if path == "-" => copy_stdin_to_stdout(stdin_fd, stdout_fd),
        [path] => copy_file_to_stdout(path, stdout_fd),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "prototype supports at most one file operand",
        )),
    };
    match result {
        Ok(()) => 0,
        Err(err) => {
            if let Ok(dup_raw) = cvt_dup(request.stderr_fd.as_raw_fd()) {
                let mut stderr = unsafe { fs::File::from_raw_fd(dup_raw) };
                let _ = writeln!(stderr, "cat_ipc_server: {err}");
            }
            1
        }
    }
}

fn cvt_dup(fd: RawFd) -> io::Result<RawFd> {
    let duped = unsafe { libc::dup(fd) };
    if duped < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(duped)
    }
}

fn handle_client(mut stream: UnixStream) -> io::Result<()> {
    let request = recv_request(&stream)?;
    let status = handle_request(request);
    stream.write_all(&[status])?;
    Ok(())
}

fn main() {
    let mut args = env::args_os();
    let program = args
        .next()
        .unwrap_or_else(|| "cat_ipc_server".into())
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
            eprintln!(
                "{program}: parent directory does not exist: {}",
                parent.display()
            );
            process::exit(1);
        }
    }
    if Path::new(&socket_path).exists() && fs::remove_file(&socket_path).is_err() {
        eprintln!("{program}: failed to remove existing socket");
        process::exit(1);
    }

    let listener = match UnixListener::bind(&socket_path) {
        Ok(listener) => listener,
        Err(err) => {
            eprintln!("{program}: {err}");
            process::exit(1);
        }
    };
    let _guard = SocketGuard {
        path: socket_path.clone(),
    };

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let _ = handle_client(stream);
            }
            Err(_) => continue,
        }
    }
}
