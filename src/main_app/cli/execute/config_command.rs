use super::*;

pub(super) fn run(
    config_subcommand: Option<&str>,
    config_target: Option<&str>,
    config_path: Option<&str>,
) -> io::Result<i32> {
    let config = config::load_config(config_path);
    match config_subcommand {
        Some("print") => {
            println!("{}", config.to_pretty_json()?);
            Ok(0)
        }
        Some("explain") => {
            let target = config_target.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "config explain requires --for <path>",
                )
            })?;
            println!(
                "{}",
                serde_json::to_string_pretty(&config.explain_for_path(target))
                    .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?
            );
            Ok(0)
        }
        Some(other) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unknown config subcommand: {other}"),
        )),
        None => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "missing config subcommand",
        )),
    }
}
