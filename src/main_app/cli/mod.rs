use super::*;

mod args;
mod execute;

pub(super) fn try_main() -> io::Result<i32> {
    match args::parse_cli()? {
        args::ParseOutcome::Early(code) => Ok(code),
        args::ParseOutcome::Parsed(parsed) => execute::run(parsed),
    }
}

pub(super) fn main() {
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        if is_broken_pipe_panic(info.payload()) {
            return;
        }
        default_hook(info);
    }));

    match std::panic::catch_unwind(try_main) {
        Ok(Ok(code)) if code == 0 => {}
        Ok(Ok(code)) => std::process::exit(code),
        Ok(Err(err)) => {
            if is_broken_pipe_error(&err) {
                std::process::exit(0);
            }
            let _ = writeln!(io::stderr().lock(), "Error: {}", err);
            std::process::exit(1);
        }
        Err(payload) => {
            if is_broken_pipe_panic(payload.as_ref()) {
                std::process::exit(0);
            }
            std::panic::resume_unwind(payload);
        }
    }
}
