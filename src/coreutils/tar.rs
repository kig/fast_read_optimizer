use super::*;

fn parse_short_tar_flags(
    arg: &str,
    args: &[String],
    index: &mut usize,
    create: &mut bool,
    verbose: &mut bool,
    output: &mut Option<String>,
) -> io::Result<()> {
    let chars = arg[1..].chars().collect::<Vec<_>>();
    let mut pos = 0usize;
    while pos < chars.len() {
        match chars[pos] {
            'c' => *create = true,
            'v' => *verbose = true,
            'f' => {
                if pos + 1 < chars.len() {
                    *output = Some(chars[pos + 1..].iter().collect());
                    return Ok(());
                }
                *index += 1;
                *output = Some(
                    args.get(*index)
                        .ok_or_else(|| {
                            io::Error::new(io::ErrorKind::InvalidInput, "missing value for -f")
                        })?
                        .clone(),
                );
                return Ok(());
            }
            flag => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unsupported tar flag -{flag}"),
                ))
            }
        }
        pos += 1;
    }
    Ok(())
}

pub(super) fn run_tar(args: &[String]) -> io::Result<i32> {
    let program = args[0].as_str();
    let mut create = false;
    let mut verbose = false;
    let mut output: Option<String> = None;
    let mut paths = Vec::new();
    let mut end_flags = false;

    let mut i = 1usize;
    while i < args.len() {
        let arg = &args[i];
        if !end_flags && arg == "--" {
            end_flags = true;
        } else if !end_flags && arg == "--create" {
            create = true;
        } else if !end_flags && arg == "--verbose" {
            verbose = true;
        } else if !end_flags && arg == "--file" {
            i += 1;
            output = Some(
                args.get(i)
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, "missing value for --file")
                    })?
                    .clone(),
            );
        } else if !end_flags && arg.starts_with('-') && arg.len() > 1 {
            parse_short_tar_flags(arg, args, &mut i, &mut create, &mut verbose, &mut output)?;
        } else {
            paths.push(arg.clone());
        }
        i += 1;
    }

    if !create {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "tar currently supports only create mode (-c/--create)",
        ));
    }
    let output = output.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Usage: {program} -cf <archive.tar> <source>"),
        )
    })?;
    if paths.len() != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Usage: {program} -cf <archive.tar> <source>"),
        ));
    }
    crate::main_app::create_tar_archive(Path::new(&paths[0]), Path::new(&output), verbose)?;
    Ok(0)
}
