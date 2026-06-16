use super::*;

pub(super) fn handle_config_command(
    config_path: Option<&str>,
    config_subcommand: Option<&str>,
    config_target: Option<&str>,
) -> io::Result<Option<i32>> {
    let Some(subcommand) = config_subcommand else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "missing config subcommand",
        ));
    };
    let config = config::load_config(config_path);
    match subcommand {
        "print" => {
            fro::cio_println!("{}", config.to_pretty_json()?);
            Ok(Some(0))
        }
        "explain" => {
            let target = config_target.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "config explain requires --for <path>",
                )
            })?;
            fro::cio_println!(
                "{}",
                serde_json::to_string_pretty(&config.explain_for_path(target))
                    .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?
            );
            Ok(Some(0))
        }
        other => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unknown config subcommand: {other}"),
        )),
    }
}
