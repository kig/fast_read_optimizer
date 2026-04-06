use super::*;

#[test]
fn parse_size_accepts_common_suffixes() {
    assert_eq!(parse_size("1024"), Some(1024));
    assert_eq!(parse_size("1KiB"), Some(1024));
    assert_eq!(parse_size("2MiB"), Some(2 * 1024 * 1024));
    assert_eq!(parse_size("4GiB"), Some(4 * 1024 * 1024 * 1024));
    assert_eq!(parse_size("1g"), Some(1024 * 1024 * 1024));
    assert_eq!(parse_size(""), None);
    assert_eq!(parse_size("nope"), None);
}

#[test]
fn parse_reported_summary_extracts_speed_without_params() {
    let sample = "copy 1073741824 bytes in 0.1076 s, 10.0 GB/s, [1, 2, 3]";
    let summary = parse_reported_summary(sample).unwrap();
    assert_eq!(summary.gbps, 10.0);
    assert_eq!(summary.params.unwrap(), vec![1, 2, 3]);
    let sampled = "recursive-copy sample t=0.010s bytes=123 items=4 window=1.230 GB/s avg=1.230 GB/s items/s=400.0\ncopy 1073741824 bytes in 0.1076 s, 10.0 GB/s, [1, 2, 3]";
    let summary = parse_reported_summary(sampled).unwrap();
    assert_eq!(summary.gbps, 10.0);
    assert_eq!(summary.params.unwrap(), vec![1, 2, 3]);
    assert_eq!(parse_reported_summary("no throughput here"), None);
}

#[test]
fn choose_test_size_respects_wear_cap() {
    let fs = FsStats {
        total_bytes: 1024_u64.pow(4),
        avail_bytes: 1024_u64.pow(4),
    };
    let size = choose_test_size(fs, 3, 13, 256 * 1024 * 1024, 4 * 1024 * 1024 * 1024, 0.01);
    assert!(size <= 900 * 1024 * 1024);
}

#[test]
fn parse_reported_summary_extracts_speed_and_params() {
    let sample = "read 1073741824 bytes in 0.1076 s, 10.0 GB/s, [31, 131072, 1, 16, 3145728, 2, 4, 524288, 4]";
    let summary = parse_reported_summary(sample).unwrap();
    assert_eq!(summary.gbps, 10.0);
    assert_eq!(
        summary.params.unwrap(),
        vec![31, 131072, 1, 16, 3145728, 2, 4, 524288, 4]
    );
}
