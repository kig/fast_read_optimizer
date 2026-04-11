use libc::{mincore, mmap, munmap, sysconf, MAP_SHARED, PROT_READ, _SC_PAGESIZE};
use std::fs::File;
use std::os::unix::io::{AsRawFd, RawFd};
use std::ptr;

pub fn is_edge_pages_resident(file_path: &str) -> Result<bool, String> {
    let file = File::open(file_path).map_err(|e| e.to_string())?;
    let file_len = file.metadata().map_err(|e| e.to_string())?.len();
    if file_len == 0 {
        return Ok(true);
    }
    let page_size = unsafe { sysconf(_SC_PAGESIZE) as usize };
    let (first_offset, last_offset) = resident_probe_offsets(file_len, page_size);
    let fd = file.as_raw_fd();
    let first = is_page_resident(fd, page_size, first_offset)?;
    if !first {
        return Ok(false);
    }
    if let Some(last_offset) = last_offset {
        return is_page_resident(fd, page_size, last_offset);
    }
    Ok(true)
}

fn resident_probe_offsets(file_len: u64, page_size: usize) -> (u64, Option<u64>) {
    let last_offset = ((file_len - 1) / page_size as u64) * page_size as u64;
    if last_offset == 0 {
        (0, None)
    } else {
        (0, Some(last_offset))
    }
}

fn is_page_resident(fd: RawFd, page_size: usize, offset: u64) -> Result<bool, String> {
    unsafe {
        let offset = i64::try_from(offset).map_err(|_| "mmap offset overflow".to_string())?;
        let addr = mmap(ptr::null_mut(), page_size, PROT_READ, MAP_SHARED, fd, offset);
        if addr == libc::MAP_FAILED {
            return Err("mmap failed".to_string());
        }
        let mut vec: u8 = 0;
        let result = mincore(addr, page_size, &mut vec as *mut u8);
        munmap(addr, page_size);
        if result == 0 {
            Ok((vec & 1) != 0)
        } else {
            Err("mincore failed".to_string())
        }
    }
}

#[allow(dead_code)]
pub fn is_range_in_page_cache(file: &File, offset: u64, len: usize) -> bool {
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };
    let offset_aligned = (offset / page_size as u64) * page_size as u64;
    let offset_diff = offset - offset_aligned;
    let len_aligned = (len + offset_diff as usize + page_size - 1) / page_size * page_size;

    unsafe {
        let ptr = libc::mmap(
            std::ptr::null_mut(),
            len_aligned,
            libc::PROT_NONE,
            libc::MAP_SHARED,
            file.as_raw_fd(),
            offset_aligned as i64,
        );

        if ptr == libc::MAP_FAILED {
            return false;
        }

        let num_pages = len_aligned / page_size;
        let mut vec = vec![0u8; num_pages];
        let res = libc::mincore(ptr, len_aligned, vec.as_mut_ptr());
        libc::munmap(ptr, len_aligned);

        if res != 0 {
            return false;
        }

        // Check if all pages in the range are resident
        vec.iter().all(|&b| (b & 1) != 0)
    }
}

#[cfg(test)]
mod tests {
    use super::{is_edge_pages_resident, resident_probe_offsets};
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_file(prefix: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        path.push(format!("{}-{}-{}", prefix, std::process::id(), nanos));
        path
    }

    #[test]
    fn empty_file_is_treated_as_page_cached() {
        let path = unique_temp_file("fro-mincore-empty");
        fs::write(&path, b"").unwrap();
        assert_eq!(is_edge_pages_resident(path.to_str().unwrap()), Ok(true));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn resident_probe_offsets_skip_duplicate_single_page_probe() {
        assert_eq!(resident_probe_offsets(4096, 4096), (0, None));
        assert_eq!(resident_probe_offsets(1, 4096), (0, None));
    }

    #[test]
    fn resident_probe_offsets_sample_first_and_last_pages() {
        assert_eq!(resident_probe_offsets(4097, 4096), (0, Some(4096)));
        assert_eq!(resident_probe_offsets(8192, 4096), (0, Some(4096)));
        assert_eq!(resident_probe_offsets(9000, 4096), (0, Some(8192)));
    }
}
