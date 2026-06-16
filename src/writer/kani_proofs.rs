use super::copy_ops::to_off_t;
use std::io;

#[kani::proof]
fn to_off_t_accepts_i64_range_values() {
    let value: u64 = kani::any();
    kani::assume(value <= i64::MAX as u64);
    assert_eq!(to_off_t(value, "value").unwrap(), value as i64);
}

#[kani::proof]
fn to_off_t_rejects_values_past_i64_max() {
    let value: u64 = kani::any();
    kani::assume(value > i64::MAX as u64);
    let err = to_off_t(value, "value").unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
}
