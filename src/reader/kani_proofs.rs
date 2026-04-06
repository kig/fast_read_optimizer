use super::auto_lift_mode_for_residency;
use crate::common::IOMode;

#[kani::proof]
fn auto_lift_mode_matches_first_page_residency() {
    let first_page_resident: bool = kani::any();
    let mode = auto_lift_mode_for_residency(first_page_resident);
    if first_page_resident {
        assert_eq!(mode, IOMode::PageCache);
    } else {
        assert_eq!(mode, IOMode::Direct);
    }
}
