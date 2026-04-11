use super::auto_lift_mode_for_residency;
use crate::common::IOMode;

#[kani::proof]
fn auto_lift_mode_matches_edge_page_residency() {
    let edge_pages_resident: bool = kani::any();
    let mode = auto_lift_mode_for_residency(edge_pages_resident);
    if edge_pages_resident {
        assert_eq!(mode, IOMode::PageCache);
    } else {
        assert_eq!(mode, IOMode::Direct);
    }
}
