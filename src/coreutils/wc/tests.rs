use super::*;

fn wc_test_temp_file(name: &str) -> std::path::PathBuf {
    let base = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("test-tmp");
    std::fs::create_dir_all(&base).unwrap();
    base.join(format!(
        "{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

#[test]
fn reduce_wc_counts_merges_cross_block_words() {
    let blocks = [
        WcBlockCounts {
            lines: 0,
            words: 1,
            chars: 0,
            bytes: 3,
            max_line_length: 0,
            starts_in_word: true,
            ends_in_word: true,
        },
        WcBlockCounts {
            lines: 1,
            words: 1,
            chars: 0,
            bytes: 4,
            max_line_length: 0,
            starts_in_word: true,
            ends_in_word: false,
        },
    ];

    assert_eq!(
        reduce_wc_counts(&blocks),
        WcTotals {
            lines: 1,
            words: 1,
            chars: 0,
            bytes: 7,
            max_line_length: 0,
        }
    );
}

#[test]
fn wc_whitespace_matches_posix_ascii_set() {
    for byte in [b' ', b'\t', b'\n', 0x0b, 0x0c, b'\r'] {
        assert!(is_wc_whitespace(byte), "byte {byte:#x} should split words");
    }
    for byte in [0_u8, b'a', 0x1c, 0x7f, 0x80, 0xff] {
        assert!(
            !is_wc_whitespace(byte),
            "byte {byte:#x} should not split words"
        );
    }
}

#[test]
fn count_wc_block_counts_lines_words_and_bytes() {
    let counts = count_wc_block(
        b"one two\nthree\x0bfour\r\nfive",
        WcCountOptions {
            lines: true,
            words: true,
            chars: false,
            bytes: true,
            max_line_length: false,
        },
    );
    assert_eq!(counts.lines, 2);
    assert_eq!(counts.words, 5);
    assert_eq!(counts.bytes, 24);
    assert!(counts.starts_in_word);
    assert!(counts.ends_in_word);
}

#[test]
fn wc_short_flag_bundles_enable_multiple_counts() {
    let mut lines = false;
    let mut words = false;
    let mut chars = false;
    let mut bytes = false;
    let mut max_line_length = false;

    assert!(apply_wc_short_flag_bundle(
        "-lwc",
        &mut lines,
        &mut words,
        &mut chars,
        &mut bytes,
        &mut max_line_length,
    ));
    assert!(lines);
    assert!(words);
    assert!(!chars);
    assert!(bytes);
    assert!(!max_line_length);
}

#[test]
fn wc_short_flag_bundle_rejects_non_wc_flags() {
    let mut lines = false;
    let mut words = false;
    let mut chars = false;
    let mut bytes = false;
    let mut max_line_length = false;

    assert!(!apply_wc_short_flag_bundle(
        "-lz",
        &mut lines,
        &mut words,
        &mut chars,
        &mut bytes,
        &mut max_line_length,
    ));
    assert!(!lines);
    assert!(!words);
    assert!(!chars);
    assert!(!bytes);
    assert!(!max_line_length);
}

#[test]
fn wc_parallel_totals_match_sequential_totals() {
    let bytes = b"alpha beta\ngamma\r\ndelta\x0bepsilon zeta".repeat(1024);
    let options = WcCountOptions {
        lines: true,
        words: true,
        chars: false,
        bytes: true,
        max_line_length: false,
    };
    let sequential =
        wc_totals_from_reader(&mut std::io::Cursor::new(bytes.as_slice()), options).unwrap();
    let parallel =
        wc_totals_from_reader_parallel(&mut std::io::Cursor::new(bytes.as_slice()), options)
            .unwrap();
    assert_eq!(parallel, sequential);
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[test]
fn wc_avx2_block_counts_match_scalar_counts() {
    if !std::arch::is_x86_feature_detected!("avx2") {
        return;
    }
    let block = b"alpha beta\ngamma\r\ndelta\x0bepsilon zeta\x80\xfftail".repeat(257);
    let options = WcCountOptions {
        lines: true,
        words: true,
        chars: false,
        bytes: true,
        max_line_length: false,
    };
    let scalar = count_wc_block_scalar(&block, options);
    let avx2 = unsafe { count_wc_block_avx2(&block, options) };
    assert_eq!(avx2.lines, scalar.lines);
    assert_eq!(avx2.words, scalar.words);
    assert_eq!(avx2.bytes, scalar.bytes);
    assert_eq!(avx2.starts_in_word, scalar.starts_in_word);
    assert_eq!(avx2.ends_in_word, scalar.ends_in_word);
}

#[test]
fn wc_metadata_totals_only_applies_to_byte_only_regular_files() {
    let tmp = wc_test_temp_file("fro-wc-metadata");
    std::fs::write(&tmp, b"abcdef").unwrap();

    let byte_only = WcCountOptions {
        lines: false,
        words: false,
        chars: false,
        bytes: true,
        max_line_length: false,
    };
    assert_eq!(
        wc_metadata_totals(&StreamInput::File(tmp.display().to_string()), byte_only).unwrap(),
        Some(WcTotals {
            lines: 0,
            words: 0,
            chars: 0,
            bytes: 6,
            max_line_length: 0,
        })
    );

    let combined = WcCountOptions {
        lines: true,
        words: false,
        chars: false,
        bytes: true,
        max_line_length: false,
    };
    assert_eq!(
        wc_metadata_totals(&StreamInput::File(tmp.display().to_string()), combined).unwrap(),
        None
    );
    assert_eq!(
        wc_metadata_totals(&StreamInput::Stdin { label: None }, byte_only).unwrap(),
        None
    );
    let max_line_length = WcCountOptions {
        lines: false,
        words: false,
        chars: false,
        bytes: true,
        max_line_length: true,
    };
    assert_eq!(
        wc_metadata_totals(
            &StreamInput::File(tmp.display().to_string()),
            max_line_length
        )
        .unwrap(),
        None
    );

    let _ = std::fs::remove_file(tmp);
}

#[test]
fn wc_character_count_matches_gnu_style_utf8_handling() {
    let options = WcCountOptions {
        lines: true,
        words: false,
        chars: true,
        bytes: true,
        max_line_length: false,
    };
    let totals = wc_totals_from_reader(
        &mut std::io::Cursor::new(b"\xff\x80a\n\xe2\x82".as_slice()),
        options,
    )
    .unwrap();
    assert_eq!(
        totals,
        WcTotals {
            lines: 1,
            words: 0,
            chars: 2,
            bytes: 6,
            max_line_length: 0,
        }
    );
}

#[test]
fn wc_max_line_length_matches_gnu_style_width_rules() {
    let options = WcCountOptions {
        lines: false,
        words: false,
        chars: false,
        bytes: false,
        max_line_length: true,
    };
    let totals = wc_totals_from_reader(
        &mut std::io::Cursor::new("e\u{0301}\n中\n1234567\tX\n".as_bytes()),
        options,
    )
    .unwrap();
    assert_eq!(
        totals,
        WcTotals {
            lines: 0,
            words: 0,
            chars: 0,
            bytes: 0,
            max_line_length: 9,
        }
    );
}

#[test]
fn wc_max_line_length_ignores_invalid_utf8_bytes() {
    let options = WcCountOptions {
        lines: false,
        words: false,
        chars: false,
        bytes: false,
        max_line_length: true,
    };
    let totals = wc_totals_from_reader(
        &mut std::io::Cursor::new(b"\xffa\n\x80bc".as_slice()),
        options,
    )
    .unwrap();
    assert_eq!(
        totals,
        WcTotals {
            lines: 0,
            words: 0,
            chars: 0,
            bytes: 0,
            max_line_length: 2,
        }
    );
}

#[test]
fn read_files0_inputs_preserves_dash_from_list_files() {
    let parsed = read_files0_inputs(&mut std::io::Cursor::new(b"alpha\0-\0"), false).unwrap();
    assert_eq!(parsed.requested_count, 2);
    assert_eq!(parsed.exit_code, 0);
    assert_eq!(
        parsed.inputs,
        vec![
            StreamInput::File("alpha".to_string()),
            StreamInput::Stdin {
                label: Some("-".to_string())
            }
        ]
    );
}

#[test]
fn read_files0_inputs_rejects_dash_from_stdin_sources() {
    let parsed = read_files0_inputs(&mut std::io::Cursor::new(b"alpha\0-\0"), true).unwrap();
    assert_eq!(parsed.requested_count, 2);
    assert_eq!(parsed.exit_code, 1);
    assert_eq!(parsed.inputs, vec![StreamInput::File("alpha".to_string())]);
}
