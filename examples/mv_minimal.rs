use std::env;
use std::fs;
use std::process;

fn usage(program: &str) {
    eprintln!("USAGE: {program} <source> <target>");
}

fn main() {
    let mut args = env::args_os();
    let program = args
        .next()
        .unwrap_or_else(|| "mv_minimal_rust".into())
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

    if let Err(err) = fs::rename(&source, &target) {
        eprintln!("{program}: {err}");
        process::exit(1);
    }
}
