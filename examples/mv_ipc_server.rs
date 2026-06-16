use std::env;
use std::fs;
use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process;

const DEFAULT_SOCKET_PATH: &str = "/tmp/fro-mv.sock";

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

fn handle_client(mut stream: UnixStream) -> std::io::Result<()> {
    let mut request = Vec::with_capacity(512);
    stream.read_to_end(&mut request)?;
    let mut parts = request.split(|&byte| byte == 0);
    let source = parts
        .next()
        .filter(|part| !part.is_empty())
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "missing source"))?;
    let target = parts
        .next()
        .filter(|part| !part.is_empty())
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "missing target"))?;

    let status = match fs::rename(
        std::str::from_utf8(source)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "bad source"))?,
        std::str::from_utf8(target)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "bad target"))?,
    ) {
        Ok(()) => 0u8,
        Err(_) => 1u8,
    };
    stream.write_all(&[status])?;
    Ok(())
}

fn main() {
    let mut args = env::args_os();
    let program = args
        .next()
        .unwrap_or_else(|| "mv_ipc_server".into())
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
                if handle_client(stream).is_err() {
                    continue;
                }
            }
            Err(_) => continue,
        }
    }
}
