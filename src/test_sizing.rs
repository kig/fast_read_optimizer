#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FsSizingStats {
    pub total_bytes: u64,
    pub avail_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TestSizingPolicy {
    pub explicit_test_size: Option<u64>,
    pub file_count: u64,
    pub num_full_writes: u64,
    pub fixed_write_bytes: u64,
    pub min_test_size: u64,
    pub max_test_size: u64,
    pub max_drive_writes: f64,
    pub fallback_test_size: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestSizeSource {
    Explicit,
    Auto,
    Fallback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedTestSize {
    pub size_bytes: u64,
    pub source: TestSizeSource,
}

pub fn align_down(bytes: u64, align: u64) -> u64 {
    if align == 0 {
        bytes
    } else {
        bytes / align * align
    }
}

pub fn estimate_alloc_bytes(test_size: u64, file_count: u64) -> u64 {
    test_size.saturating_mul(file_count)
}

pub fn estimate_user_writes(test_size: u64, num_full_writes: u64, fixed_write_bytes: u64) -> u64 {
    test_size
        .saturating_mul(num_full_writes)
        .saturating_add(fixed_write_bytes)
}

pub fn choose_auto_test_size(fs: FsSizingStats, policy: TestSizingPolicy) -> u64 {
    if policy.file_count == 0 {
        return 0;
    }

    let space_cap = ((fs.avail_bytes as f64) * 0.60 / (policy.file_count as f64)) as u64;
    let wear_cap = if policy.num_full_writes == 0 {
        u64::MAX
    } else {
        let total_write_budget = ((fs.total_bytes as f64) * policy.max_drive_writes) as u64;
        let variable_write_budget = total_write_budget.saturating_sub(policy.fixed_write_bytes);
        variable_write_budget / policy.num_full_writes
    };

    let mut size = policy.max_test_size.min(space_cap).min(wear_cap);
    size = align_down(size, 4096).max(4096);

    size
}

pub fn resolve_test_size(fs: Option<FsSizingStats>, policy: TestSizingPolicy) -> ResolvedTestSize {
    if let Some(size) = policy.explicit_test_size {
        return ResolvedTestSize {
            size_bytes: align_down(size, 4096).max(4096),
            source: TestSizeSource::Explicit,
        };
    }

    if let Some(fs) = fs {
        return ResolvedTestSize {
            size_bytes: choose_auto_test_size(fs, policy),
            source: TestSizeSource::Auto,
        };
    }

    ResolvedTestSize {
        size_bytes: policy.fallback_test_size,
        source: TestSizeSource::Fallback,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_test_size_wins_over_auto_policy() {
        let resolved = resolve_test_size(
            Some(FsSizingStats {
                total_bytes: 1024_u64.pow(4),
                avail_bytes: 1024_u64.pow(3),
            }),
            TestSizingPolicy {
                explicit_test_size: Some(64 * 1024 * 1024),
                file_count: 3,
                num_full_writes: 100,
                fixed_write_bytes: 8 * 1024 * 1024 * 1024,
                min_test_size: 256 * 1024 * 1024,
                max_test_size: 4 * 1024 * 1024 * 1024,
                max_drive_writes: 0.001,
                fallback_test_size: 4 * 1024 * 1024 * 1024,
            },
        );
        assert_eq!(resolved.size_bytes, 64 * 1024 * 1024);
        assert_eq!(resolved.source, TestSizeSource::Explicit);
    }

    #[test]
    fn fixed_write_bytes_reduce_remaining_wear_budget() {
        let gib = 1024_u64.pow(3);
        let fs = FsSizingStats {
            total_bytes: 1024_u64.pow(4),
            avail_bytes: 1024_u64.pow(4),
        };
        let policy = TestSizingPolicy {
            explicit_test_size: None,
            file_count: 1,
            num_full_writes: 10,
            fixed_write_bytes: 8 * gib,
            min_test_size: 256 * 1024 * 1024,
            max_test_size: 4 * gib,
            max_drive_writes: 0.01,
            fallback_test_size: 4 * gib,
        };

        let total_write_budget = ((fs.total_bytes as f64) * policy.max_drive_writes) as u64;
        let expected = align_down(
            total_write_budget.saturating_sub(policy.fixed_write_bytes) / policy.num_full_writes,
            4096,
        );
        assert_eq!(choose_auto_test_size(fs, policy), expected);
    }

    #[test]
    fn fixed_write_bytes_can_force_minimum_aligned_size() {
        let mib = 1024_u64.pow(2);
        let fs = FsSizingStats {
            total_bytes: 64 * mib,
            avail_bytes: 64 * mib,
        };
        let size = choose_auto_test_size(
            fs,
            TestSizingPolicy {
                explicit_test_size: None,
                file_count: 1,
                num_full_writes: 4,
                fixed_write_bytes: 64 * mib,
                min_test_size: 16 * mib,
                max_test_size: 64 * mib,
                max_drive_writes: 0.5,
                fallback_test_size: 4 * 1024 * 1024 * 1024,
            },
        );
        assert_eq!(size, 4096);
    }

    #[test]
    fn fixed_write_bytes_do_not_shrink_runs_without_full_write_passes() {
        let gib = 1024_u64.pow(3);
        let fs = FsSizingStats {
            total_bytes: 1024_u64.pow(4),
            avail_bytes: 3 * gib,
        };
        let size = choose_auto_test_size(
            fs,
            TestSizingPolicy {
                explicit_test_size: None,
                file_count: 2,
                num_full_writes: 0,
                fixed_write_bytes: 512 * gib,
                min_test_size: 256 * 1024 * 1024,
                max_test_size: 4 * gib,
                max_drive_writes: 0.0001,
                fallback_test_size: 4 * gib,
            },
        );
        assert_eq!(size, align_down((3 * gib * 60 / 100) / 2, 4096));
    }
}
