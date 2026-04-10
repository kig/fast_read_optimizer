use std::env;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::process;

const DEFAULT_SOCKET_PATH: &str = "/tmp/fro-mv.sock";

fn usage(program: &str) {
    eprintln!("USAGE: {program} <source> <target>");
}

fn main() {
    let mut args = env::args_os();
    let program = args
        .next()
        .unwrap_or_else(|| "mv_ipc_client_rust".into())
        .to_string_lossy()
        .into_owned();
    let Some(source) = args.next() else {
        usage(&program);
        process::exit(1);
    };
    let Some(target) = args.next() else {
        usage(&program);
        process::exit(1);
    };
    if args.next().is_some() {
        usage(&program);
        process::exit(1);
    }

    let mut stream = match UnixStream::connect(DEFAULT_SOCKET_PATH) {
        Ok(stream) => stream,
        Err(err) => {
            eprintln!("{program}: {err}");
            process::exit(1);
        }
    };
    let source_bytes = source.to_string_lossy();
    let target_bytes = target.to_string_lossy();
    if stream.write_all(source_bytes.as_bytes()).is_err()
        || stream.write_all(&[0]).is_err()
        || stream.write_all(target_bytes.as_bytes()).is_err()
        || stream.write_all(&[0]).is_err()
        || stream.shutdown(std::net::Shutdown::Write).is_err()
    {
        process::exit(1);
    }
    let mut status = [1u8; 1];
    if stream.read_exact(&mut status).is_err() {
        process::exit(1);
    }
    process::exit(status[0] as i32);
}
