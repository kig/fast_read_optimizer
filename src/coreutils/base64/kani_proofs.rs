use super::*;

#[kani::proof]
fn decoded_len_from_padding_only_accepts_zero_to_two() {
    let padding: u8 = kani::any();
    let result = base64_decoded_len_from_padding(padding);
    if padding <= 2 {
        assert!(matches!(result, Some(1..=3)));
    } else {
        assert!(result.is_none());
    }
}

#[kani::proof]
fn parallel_encode_block_size_stays_aligned() {
    let block_size: u64 = kani::any();
    let result = base64_parallel_encode_block_size(block_size);
    assert!(result >= BASE64_ENCODE_INPUT_ALIGN);
    assert_eq!(result % BASE64_ENCODE_INPUT_ALIGN, 0);
}

#[kani::proof]
fn base64_encode_ascii_scalar_glsl_matches_table() {
    let sextet: u8 = kani::any();
    kani::assume(sextet < 64);
    assert_eq!(
        base64_encode_ascii_scalar_glsl(sextet),
        BASE64_ENCODE[sextet as usize]
    );
}
