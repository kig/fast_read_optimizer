use super::*;

pub(super) fn rebuild_cp_fallback_args(raw_args: &[String]) -> Vec<String> {
    if raw_args.get(1).is_some_and(|arg| arg == "cp") {
        let mut rebuilt = Vec::with_capacity(raw_args.len().saturating_sub(1));
        rebuilt.push("cp".to_string());
        rebuilt.extend(
            raw_args
                .iter()
                .skip(2)
                .filter(|arg| arg.as_str() != "--no-fallback")
                .cloned(),
        );
        rebuilt
    } else if raw_args.get(1).is_some_and(|arg| arg == "copy") {
        let mut rebuilt = Vec::with_capacity(raw_args.len().saturating_sub(1));
        rebuilt.push("cp".to_string());
        rebuilt.extend(
            raw_args
                .iter()
                .skip(2)
                .filter(|arg| arg.as_str() != "--cp-compat" && arg.as_str() != "--no-fallback")
                .cloned(),
        );
        rebuilt
    } else {
        raw_args.to_vec()
    }
}

pub(super) fn wrapper_multicall_config_path(raw_args: &[String]) -> Option<&str> {
    let mut config_path: Option<&str> = None;
    let mut idx = 1usize;
    while idx < raw_args.len() {
        match raw_args[idx].as_str() {
            "--no-fallback" => {
                idx += 1;
            }
            "-c" | "--config" => {
                idx += 1;
                config_path = raw_args.get(idx).map(String::as_str);
                idx += 1;
            }
            "cp" => return config_path,
            other if coreutils::is_coreutils_command(other) => return config_path,
            _ => return None,
        }
    }
    None
}
