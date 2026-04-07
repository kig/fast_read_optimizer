use super::process::bytes::{
    append_sanitized_base64_bytes, decode_base64_bytes_with_detect_fallback,
    encode_base64_bytes_via_wrapped_writer_for_test,
};
use super::process::wrapped::append_wrapped_base64_bytes;
use super::*;

#[test]
fn base64_encode_ascii_scalar_glsl_matches_table() {
    for sextet in 0_u8..64 {
        assert_eq!(
            base64_encode_ascii_scalar_glsl(sextet),
            BASE64_ENCODE[sextet as usize]
        );
    }
}

#[test]
fn decode_base64_quartet_matches_expected_lengths() {
    assert_eq!(decode_base64_quartet(*b"Zg=="), Some(([b'f', 0, 0], 1)));
    assert_eq!(decode_base64_quartet(*b"Zm8="), Some(([b'f', b'o', 0], 2)));
    assert_eq!(
        decode_base64_quartet(*b"Zm9v"),
        Some(([b'f', b'o', b'o'], 3))
    );
}

#[test]
fn append_sanitized_base64_bytes_skips_newlines_and_tracks_padding() {
    let mut pending = Vec::new();
    let pad = append_sanitized_base64_bytes(b"Zm9v\r\nYg==\n", 0, &mut pending).unwrap();
    assert_eq!(pending, b"Zm9vYg==");
    assert_eq!(pad, Some(6));
}

#[test]
fn decode_base64_block_into_scalar_rejects_bytes_after_padding() {
    let mut out = [0u8; 6];
    let err = decode_base64_block_into_scalar(b"YQ==Yg==", &mut out).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}

#[test]
fn write_base64_encoded_bytes_wraps_at_requested_columns() {
    let mut out = Vec::new();
    let mut current_line_len = 0usize;
    append_wrapped_base64_bytes(&mut out, b"YWJjZGVm", 5, &mut current_line_len);
    assert_eq!(out, b"YWJjZ\nGVm");
    assert_eq!(current_line_len, 3);
}

#[test]
fn append_wrapped_base64_bytes_batches_newlines_without_tiny_writes() {
    let mut out = Vec::new();
    let mut current_line_len = 2usize;
    append_wrapped_base64_bytes(&mut out, b"ABCDEFGH", 5, &mut current_line_len);
    assert_eq!(out, b"ABC\nDEFGH\n");
    assert_eq!(current_line_len, 0);
}

#[test]
fn write_base64_encoded_vec_wraps_like_slice_path() {
    let mut out = Vec::new();
    let mut current_line_len = 2usize;
    append_wrapped_base64_bytes(&mut out, b"ABCDEFGH", 5, &mut current_line_len);
    assert_eq!(out, b"ABC\nDEFGH\n");
    assert_eq!(current_line_len, 0);
}

#[test]
fn wrapped_writer_batches_small_wrap_output_into_few_writes() {
    let bytes = (0..(128 * 1024 + 7))
        .map(|i| ((i * 13 + 3) % 251) as u8)
        .collect::<Vec<_>>();
    let expected = super::process::bytes::encode_base64_bytes_via_wrapped_path(&bytes, 5).unwrap();
    let (actual, writes) =
        encode_base64_bytes_via_wrapped_writer_for_test(&bytes, 5, 32 * 1024).unwrap();
    assert_eq!(actual, expected);
    assert!(writes <= 4, "expected batched writes, saw {writes}");
}

#[test]
fn base64_parallel_encode_block_size_rounds_to_3page_multiple() {
    assert_eq!(
        base64_parallel_encode_block_size(4096),
        BASE64_ENCODE_INPUT_ALIGN
    );
    assert_eq!(
        base64_parallel_encode_block_size(BASE64_ENCODE_INPUT_ALIGN),
        BASE64_ENCODE_INPUT_ALIGN
    );
    assert_eq!(
        base64_parallel_encode_block_size((10 * 4096) + 17),
        3 * BASE64_ENCODE_INPUT_ALIGN
    );
}

#[test]
fn encode_base64_block_matches_known_output() {
    assert_eq!(encode_base64_block(b""), b"");
    assert_eq!(encode_base64_block(b"foobar"), b"Zm9vYmFy");
    assert_eq!(encode_base64_block(b"fooba"), b"Zm9vYmE=");
}

#[test]
fn encode_base64_block_into_matches_known_output() {
    let mut out = [0_u8; 8];
    let written = encode_base64_block_into(b"foobar", &mut out);
    assert_eq!(written, 8);
    assert_eq!(&out[..written], b"Zm9vYmFy");
}

#[test]
fn encode_base64_block_scalar_matches_known_output() {
    assert_eq!(encode_base64_block_scalar(b""), b"");
    assert_eq!(encode_base64_block_scalar(b"foobar"), b"Zm9vYmFy");
    assert_eq!(encode_base64_block_scalar(b"fooba"), b"Zm9vYmE=");
}

#[test]
fn base64_avx2_block_matches_scalar_block() {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if !std::arch::is_x86_feature_detected!("avx2") {
            return;
        }
        let bytes = (0..(24 * 7 + 5))
            .map(|i| ((i * 29 + 7) % 251) as u8)
            .collect::<Vec<_>>();
        let scalar = encode_base64_block_scalar(&bytes);
        let mut out = vec![0_u8; encoded_base64_len(bytes.len())];
        let written = unsafe { encode_base64_block_into_avx2_spmd(&bytes, &mut out) };
        out.truncate(written);
        assert_eq!(out, scalar);

        let mut out = vec![0_u8; encoded_base64_len(bytes.len())];
        let written = unsafe { encode_base64_block_into_avx2_shuffle(&bytes, &mut out) };
        out.truncate(written);
        assert_eq!(out, scalar);
    }
}

#[test]
fn decode_detect_fallback_decodes_clean_input() {
    let bytes = (0..(12 * 1024))
        .map(|i| ((i * 29 + 7) % 251) as u8)
        .collect::<Vec<_>>();
    let mut encoded = vec![0u8; encoded_base64_len(bytes.len())];
    let written = encode_base64_block_into(&bytes, &mut encoded);
    encoded.truncate(written);
    let decoded =
        decode_base64_bytes_with_detect_fallback(&encoded, false, Base64DecodeKernel::Auto)
            .unwrap();
    assert_eq!(decoded, bytes);
}

#[test]
fn decode_detect_fallback_decodes_wrapped_input() {
    let bytes = (0..(12 * 1024))
        .map(|i| ((i * 19 + 11) % 251) as u8)
        .collect::<Vec<_>>();
    let encoded = super::process::bytes::encode_base64_bytes_via_wrapped_path(&bytes, 76).unwrap();
    let decoded =
        decode_base64_bytes_with_detect_fallback(&encoded, false, Base64DecodeKernel::Auto)
            .unwrap();
    assert_eq!(decoded, bytes);
}

#[test]
fn decode_detect_fallback_rejects_invalid_input() {
    let err = decode_base64_bytes_with_detect_fallback(b"Zm9v!!", false, Base64DecodeKernel::Auto)
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}
