use super::{rebuild_cp_fallback_args, wrapper_multicall_config_path};

#[test]
fn rebuild_cp_fallback_args_normalizes_fro_cp_subcommand() {
    let raw_args = vec![
        "fro".to_string(),
        "cp".to_string(),
        "--backup=numbered".to_string(),
        "src".to_string(),
        "dst".to_string(),
    ];
    assert_eq!(
        rebuild_cp_fallback_args(&raw_args),
        vec![
            "cp".to_string(),
            "--backup=numbered".to_string(),
            "src".to_string(),
            "dst".to_string(),
        ]
    );
}

#[test]
fn rebuild_cp_fallback_args_strips_cp_compat_marker_for_copy_mode() {
    let raw_args = vec![
        "fro".to_string(),
        "copy".to_string(),
        "--cp-compat".to_string(),
        "--backup=numbered".to_string(),
        "src".to_string(),
        "dst".to_string(),
    ];
    assert_eq!(
        rebuild_cp_fallback_args(&raw_args),
        vec![
            "cp".to_string(),
            "--backup=numbered".to_string(),
            "src".to_string(),
            "dst".to_string(),
        ]
    );
}

#[test]
fn rebuild_cp_fallback_args_strips_no_fallback_wrapper_flag() {
    let raw_args = vec![
        "fro".to_string(),
        "cp".to_string(),
        "--no-fallback".to_string(),
        "--backup=numbered".to_string(),
        "src".to_string(),
        "dst".to_string(),
    ];
    assert_eq!(
        rebuild_cp_fallback_args(&raw_args),
        vec![
            "cp".to_string(),
            "--backup=numbered".to_string(),
            "src".to_string(),
            "dst".to_string(),
        ]
    );
}

#[test]
fn wrapper_multicall_config_path_extracts_parent_config_for_coreutils_subcommand() {
    let raw_args = vec![
        "fro".to_string(),
        "-c".to_string(),
        "/tmp/fro.json".to_string(),
        "cat".to_string(),
        "file.txt".to_string(),
    ];
    assert_eq!(
        wrapper_multicall_config_path(&raw_args),
        Some("/tmp/fro.json")
    );
}

#[test]
fn wrapper_multicall_config_path_handles_wrapper_flags_before_command() {
    let raw_args = vec![
        "fro".to_string(),
        "--no-fallback".to_string(),
        "--config".to_string(),
        "/tmp/fro.json".to_string(),
        "cp".to_string(),
        "src".to_string(),
        "dst".to_string(),
    ];
    assert_eq!(
        wrapper_multicall_config_path(&raw_args),
        Some("/tmp/fro.json")
    );
}

#[test]
fn wrapper_multicall_config_path_does_not_confuse_multicall_command_flags() {
    let raw_args = vec![
        "fro".to_string(),
        "wc".to_string(),
        "-c".to_string(),
        "file.txt".to_string(),
    ];
    assert_eq!(wrapper_multicall_config_path(&raw_args), None);
}
