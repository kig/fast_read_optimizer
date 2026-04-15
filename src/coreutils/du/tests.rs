use super::options::*;
use super::*;

#[test]
fn disk_usage_kib_rounds_512_byte_blocks_to_kib() {
    assert_eq!(disk_usage_kib(0), 0);
    assert_eq!(disk_usage_kib(1), 1);
    assert_eq!(disk_usage_kib(2), 1);
    assert_eq!(disk_usage_kib(3), 2);
}

#[test]
fn stat_is_dir_detects_directory_mode() {
    let mut stat = unsafe { std::mem::zeroed::<libc::stat>() };
    stat.st_mode = libc::S_IFDIR;
    assert!(stat_is_dir(&stat));
    stat.st_mode = libc::S_IFREG;
    assert!(!stat_is_dir(&stat));
}

#[test]
fn cstring_from_os_str_rejects_nul() {
    let value = std::ffi::OsString::from_vec(b"bad\0name".to_vec());
    let err = cstring_from_os_str(&value).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn du_format_human_bytes_matches_gnu_style_rounding() {
    let units = ["", "K", "M", "G", "T", "P", "E", "Z", "Y"];
    assert_eq!(du_format_human_bytes(1, 1024, &units), "1");
    assert_eq!(du_format_human_bytes(1023, 1024, &units), "1023");
    assert_eq!(du_format_human_bytes(1024, 1024, &units), "1.0K");
    assert_eq!(du_format_human_bytes(1025, 1024, &units), "1.1K");
    assert_eq!(du_format_human_bytes(1536, 1024, &units), "1.5K");
    assert_eq!(du_format_human_bytes(1024 * 1024, 1024, &units), "1.0M");
    assert_eq!(du_format_human_bytes(1024 * 1024 + 9, 1024, &units), "1.1M");
}

#[test]
fn du_format_human_bytes_supports_si_units() {
    let units = ["", "k", "M", "G", "T", "P", "E", "Z", "Y"];
    assert_eq!(du_format_human_bytes(999, 1000, &units), "999");
    assert_eq!(du_format_human_bytes(1000, 1000, &units), "1.0k");
    assert_eq!(du_format_human_bytes(4096, 1000, &units), "4.1k");
}

#[test]
fn du_apply_short_flag_accepts_combined_supported_flags() {
    let h = du_apply_short_flag(
        false,
        false,
        DuDisplayFormat::Kib,
        DuUsageMode::DiskBlocks,
        false,
        false,
        DuDereferenceMode::None,
        DuLineTerminator::Newline,
        b'h',
    )
    .unwrap();
    let hc = du_apply_short_flag(h.0, h.1, h.2, h.3, h.4, h.5, h.6, h.7, b'c').unwrap();
    let hcs = du_apply_short_flag(hc.0, hc.1, hc.2, hc.3, hc.4, hc.5, hc.6, hc.7, b's').unwrap();
    assert_eq!(
        hcs,
        (
            true,
            false,
            DuDisplayFormat::HumanReadableIec,
            DuUsageMode::DiskBlocks,
            true,
            false,
            DuDereferenceMode::None,
            DuLineTerminator::Newline
        )
    );
}

#[test]
fn du_display_total_blocks_respects_separate_dirs() {
    assert_eq!(du_display_total_blocks(false, 2, 9), 9);
    assert_eq!(du_display_total_blocks(true, 2, 9), 2);
}

#[test]
fn du_depth_included_respects_optional_limit() {
    assert!(du_depth_included(0, None));
    assert!(du_depth_included(1, Some(1)));
    assert!(!du_depth_included(2, Some(1)));
}

#[test]
fn parse_du_max_depth_accepts_non_negative_integers() {
    assert_eq!(parse_du_max_depth("0").unwrap(), 0);
    assert_eq!(parse_du_max_depth("17").unwrap(), 17);
    assert_eq!(
        parse_du_max_depth("bad").unwrap_err(),
        "invalid maximum depth ‘bad’"
    );
}

#[test]
fn parse_du_block_size_accepts_positive_sizes() {
    assert_eq!(parse_du_block_size("1").unwrap(), 1);
    assert_eq!(parse_du_block_size("2K").unwrap(), 2048);
    assert_eq!(parse_du_block_size("3MiB").unwrap(), 3 * 1024 * 1024);
    assert_eq!(
        parse_du_block_size("0").unwrap_err(),
        "invalid --block-size argument '0'"
    );
    assert_eq!(
        parse_du_block_size("bad").unwrap_err(),
        "invalid --block-size argument 'bad'"
    );
}

#[test]
fn parse_du_threshold_accepts_signed_sizes() {
    assert_eq!(parse_du_threshold("1").unwrap(), DuThreshold::Min(1));
    assert_eq!(parse_du_threshold("+2K").unwrap(), DuThreshold::Min(2048));
    assert_eq!(
        parse_du_threshold("-3MiB").unwrap(),
        DuThreshold::Max(3 * 1024 * 1024)
    );
    assert_eq!(
        parse_du_threshold("-0").unwrap_err(),
        "invalid --threshold argument '-0'"
    );
    assert_eq!(
        parse_du_threshold("bad").unwrap_err(),
        "invalid --threshold argument 'bad'"
    );
}

#[test]
fn du_threshold_includes_uses_measured_usage_bytes() {
    assert!(du_threshold_includes(
        2,
        DuUsageMode::DiskBlocks,
        Some(DuThreshold::Min(1024))
    ));
    assert!(!du_threshold_includes(
        1,
        DuUsageMode::DiskBlocks,
        Some(DuThreshold::Min(1024))
    ));
    assert!(du_threshold_includes(
        2048,
        DuUsageMode::ApparentBytes,
        Some(DuThreshold::Max(2048))
    ));
    assert!(!du_threshold_includes(
        2049,
        DuUsageMode::ApparentBytes,
        Some(DuThreshold::Max(2048))
    ));
}

#[test]
fn du_format_usage_supports_disk_and_apparent_sizes() {
    assert_eq!(
        du_format_usage(8, DuUsageMode::DiskBlocks, DuDisplayFormat::Kib),
        "4"
    );
    assert_eq!(
        du_format_usage(
            8,
            DuUsageMode::DiskBlocks,
            DuDisplayFormat::HumanReadableIec
        ),
        "4.0K"
    );
    assert_eq!(
        du_format_usage(8, DuUsageMode::DiskBlocks, DuDisplayFormat::HumanReadableSi),
        "4.1k"
    );
    assert_eq!(
        du_format_usage(8, DuUsageMode::DiskBlocks, DuDisplayFormat::BlockSize(512)),
        "8"
    );
    assert_eq!(
        du_format_usage(8, DuUsageMode::DiskBlocks, DuDisplayFormat::BlockSize(2048)),
        "2"
    );
    assert_eq!(
        du_format_usage(1536, DuUsageMode::ApparentBytes, DuDisplayFormat::Kib),
        "2"
    );
    assert_eq!(
        du_format_usage(
            1536,
            DuUsageMode::ApparentBytes,
            DuDisplayFormat::HumanReadableIec
        ),
        "1.5K"
    );
    assert_eq!(
        du_format_usage(
            1536,
            DuUsageMode::ApparentBytes,
            DuDisplayFormat::HumanReadableSi
        ),
        "1.6k"
    );
    assert_eq!(
        du_format_usage(
            1536,
            DuUsageMode::ApparentBytes,
            DuDisplayFormat::BlockSize(1)
        ),
        "1536"
    );
}

#[test]
fn du_bytes_short_flag_enables_apparent_byte_output() {
    let b = du_apply_short_flag(
        false,
        false,
        DuDisplayFormat::Kib,
        DuUsageMode::DiskBlocks,
        false,
        false,
        DuDereferenceMode::None,
        DuLineTerminator::Newline,
        b'b',
    )
    .unwrap();
    assert_eq!(
        b,
        (
            false,
            false,
            DuDisplayFormat::BlockSize(1),
            DuUsageMode::ApparentBytes,
            false,
            false,
            DuDereferenceMode::None,
            DuLineTerminator::Newline
        )
    );
}

#[test]
fn du_k_and_m_short_flags_override_only_display_units() {
    let k = du_apply_short_flag(
        false,
        false,
        DuDisplayFormat::HumanReadableIec,
        DuUsageMode::ApparentBytes,
        false,
        false,
        DuDereferenceMode::None,
        DuLineTerminator::Newline,
        b'k',
    )
    .unwrap();
    assert_eq!(k.2, DuDisplayFormat::BlockSize(1024));
    assert_eq!(k.3, DuUsageMode::ApparentBytes);

    let m = du_apply_short_flag(
        false,
        false,
        DuDisplayFormat::Kib,
        DuUsageMode::DiskBlocks,
        false,
        false,
        DuDereferenceMode::None,
        DuLineTerminator::Newline,
        b'm',
    )
    .unwrap();
    assert_eq!(m.2, DuDisplayFormat::BlockSize(1024_u64.pow(2)));
    assert_eq!(m.3, DuUsageMode::DiskBlocks);
}

#[test]
fn permission_denied_components_accepts_permission_kind_and_errnos() {
    assert!(permission_denied_components(
        io::ErrorKind::PermissionDenied,
        None
    ));
    assert!(permission_denied_components(
        io::ErrorKind::Other,
        Some(libc::EACCES)
    ));
    assert!(permission_denied_components(
        io::ErrorKind::Other,
        Some(libc::EPERM)
    ));
    assert!(!permission_denied_components(
        io::ErrorKind::NotFound,
        Some(libc::ENOENT)
    ));
}

#[test]
fn du_node_ready_requires_exact_completion_state() {
    assert!(du_node_ready(true, true, 0, 0, false));
    assert!(!du_node_ready(false, true, 0, 0, false));
    assert!(!du_node_ready(true, false, 0, 0, false));
    assert!(!du_node_ready(true, true, 1, 0, false));
    assert!(!du_node_ready(true, true, 0, 1, false));
    assert!(!du_node_ready(true, true, 0, 0, true));
}

#[test]
fn du_parallel_worker_count_is_bounded_for_tiny_walks() {
    assert_eq!(du_parallel_worker_count_for(0), 1);
    assert_eq!(du_parallel_worker_count_for(1), 1);
    assert_eq!(du_parallel_worker_count_for(4), 4);
    assert_eq!(du_parallel_worker_count_for(32), DU_MAX_WORKERS);
}
