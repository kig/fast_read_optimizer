use super::super::hash::{
    hash_check_should_print_result, hash_check_untagged_kind, HashCheckLineKind,
};
use super::options::{DuDereferenceMode, DuDisplayFormat, DuLineTerminator, DuUsageMode};
use super::{
    du_apply_short_flag, du_display_total_blocks, du_node_ready, permission_denied_components,
};
use std::io;

#[kani::proof]
fn permission_denied_components_accepts_permission_cases() {
    let use_permission_kind: bool = kani::any();
    let errno_is_permission: bool = kani::any();
    let kind = if use_permission_kind {
        io::ErrorKind::PermissionDenied
    } else {
        io::ErrorKind::Other
    };
    let raw = if errno_is_permission {
        Some(if kani::any() {
            libc::EACCES
        } else {
            libc::EPERM
        })
    } else {
        None
    };
    assert_eq!(
        permission_denied_components(kind, raw),
        use_permission_kind || errno_is_permission
    );
}

#[kani::proof]
fn permission_denied_components_rejects_non_permission_cases() {
    let kind = if kani::any() {
        io::ErrorKind::NotFound
    } else {
        io::ErrorKind::Other
    };
    let raw = if kani::any() {
        Some(libc::ENOENT)
    } else {
        None
    };
    assert!(!permission_denied_components(kind, raw));
}

#[kani::proof]
fn du_node_ready_matches_completion_formula() {
    let scanned: bool = kani::any();
    let own_stat_done: bool = kani::any();
    let pending_children: usize = kani::any();
    let pending_file_stats: usize = kani::any();
    let completed: bool = kani::any();

    assert_eq!(
        du_node_ready(
            scanned,
            own_stat_done,
            pending_children,
            pending_file_stats,
            completed
        ),
        scanned && own_stat_done && pending_children == 0 && pending_file_stats == 0 && !completed
    );
}

#[kani::proof]
fn du_short_flag_hcs_sets_expected_state() {
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
    assert!(hcs.0);
    assert!(!hcs.1);
    assert_eq!(hcs.2, DuDisplayFormat::HumanReadableIec);
    assert_eq!(hcs.3, DuUsageMode::DiskBlocks);
    assert!(hcs.4);
    assert!(!hcs.5);
    assert_eq!(hcs.6, DuDereferenceMode::None);
}

#[kani::proof]
fn du_display_total_blocks_matches_flag_formula() {
    let separate_dirs: bool = kani::any();
    let exclusive_blocks: u64 = kani::any();
    let subtree_total_blocks: u64 = kani::any();
    assert_eq!(
        du_display_total_blocks(separate_dirs, exclusive_blocks, subtree_total_blocks),
        if separate_dirs {
            exclusive_blocks
        } else {
            subtree_total_blocks
        }
    );
}

#[kani::proof]
fn hash_check_untagged_kind_matches_separator_contract() {
    let separator: u8 = kani::any();
    let has_filename: bool = kani::any();
    let expected = if !has_filename {
        HashCheckLineKind::Invalid
    } else {
        match separator {
            b' ' => HashCheckLineKind::UntaggedText,
            b'*' => HashCheckLineKind::UntaggedBinary,
            _ => HashCheckLineKind::Invalid,
        }
    };
    assert_eq!(hash_check_untagged_kind(separator, has_filename), expected);
}

#[kani::proof]
fn hash_check_print_policy_matches_flag_formula() {
    let success: bool = kani::any();
    let quiet: bool = kani::any();
    let status_only: bool = kani::any();
    assert_eq!(
        hash_check_should_print_result(success, quiet, status_only),
        !status_only && (!success || !quiet)
    );
}
