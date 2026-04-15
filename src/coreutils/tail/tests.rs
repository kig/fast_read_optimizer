use super::windowed::{write_tail_windowed, ByteTailPipeWindow};
use super::*;

#[test]
fn reverse_tail_line_scan_handles_trailing_newline() {
    let tmp = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("test-tmp")
        .join(format!("fro-tail-unit-{}", std::process::id()));
    std::fs::create_dir_all(&tmp).unwrap();
    let path = tmp.join("tail-lines.txt");
    std::fs::write(&path, b"alpha\nbeta\ngamma\n").unwrap();

    assert_eq!(
        regular_tail_line_start(path.to_str().unwrap(), 1, RecordTerminator::Newline).unwrap(),
        11
    );
    assert_eq!(
        regular_tail_line_start(path.to_str().unwrap(), 2, RecordTerminator::Newline).unwrap(),
        6
    );
}

#[test]
fn reverse_tail_line_scan_handles_missing_trailing_newline() {
    let tmp = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("test-tmp")
        .join(format!("fro-tail-unit-no-nl-{}", std::process::id()));
    std::fs::create_dir_all(&tmp).unwrap();
    let path = tmp.join("tail-lines.txt");
    std::fs::write(&path, b"alpha\nbeta\ngamma").unwrap();

    assert_eq!(
        regular_tail_line_start(path.to_str().unwrap(), 1, RecordTerminator::Newline).unwrap(),
        11
    );
    assert_eq!(
        regular_tail_line_start(path.to_str().unwrap(), 2, RecordTerminator::Newline).unwrap(),
        6
    );
}

#[test]
fn byte_tail_pipe_window_uses_64k_floor_for_small_counts() {
    let window = ByteTailPipeWindow::new(65_536).unwrap();
    assert_eq!(window.capacity(), TAIL_PIPE_WINDOW_MIN_CAPACITY);
}

#[test]
fn byte_tail_pipe_window_rounds_large_counts_to_4k_multiple() {
    let window = ByteTailPipeWindow::new(65_537).unwrap();
    assert_eq!(window.capacity(), 69_632);
}

#[test]
fn byte_tail_pipe_window_wraps_and_keeps_requested_suffix() {
    let mut window = ByteTailPipeWindow::new(65_536).unwrap();
    let input = (0..(TAIL_PIPE_WINDOW_SIZE + 32_768))
        .map(|idx| (idx % 251) as u8)
        .collect::<Vec<_>>();
    window.push_bytes(&input);

    let mut out = Vec::new();
    window.write_last(&mut out, 65_536).unwrap();
    assert_eq!(out, input[input.len() - 65_536..]);
}

#[test]
fn tail_windowed_lines_drop_older_complete_lines() {
    let input = StreamInput::File(
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-tmp")
            .join(format!("fro-tail-window-lines-{}", std::process::id()))
            .join("lines.txt")
            .to_string_lossy()
            .into_owned(),
    );
    let path = match &input {
        StreamInput::File(path) => path,
        _ => unreachable!(),
    };
    let path_buf = std::path::PathBuf::from(path);
    std::fs::create_dir_all(path_buf.parent().unwrap()).unwrap();
    std::fs::write(path, b"alpha\nbeta\ngamma").unwrap();

    let mut out = Vec::new();
    write_tail_windowed(
        &mut out,
        &input,
        IOMode::PageCache,
        TailMode::Lines(TailCount::FromEnd(2)),
        RecordTerminator::Newline,
    )
    .unwrap();
    assert_eq!(out, b"beta\ngamma");
}
