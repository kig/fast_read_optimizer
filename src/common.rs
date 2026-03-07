#[repr(align(4096))]
#[derive(Clone, Copy)]
#[allow(dead_code)]
pub struct PageAligned(pub [u8; 4096]);
