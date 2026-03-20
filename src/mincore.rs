use libc::{mincore, mmap, munmap, sysconf, MAP_SHARED, PROT_READ, _SC_PAGESIZE};
use std::fs::File;
use std::os::unix::io::AsRawFd;
use std::ptr;

pub fn is_first_page_resident(file_path: &str) -> Result<bool, String> {
    unsafe {
        let file = File::open(file_path).map_err(|e| e.to_string())?;
        if file.metadata().map_err(|e| e.to_string())?.len() == 0 {
            return Ok(true);
        }
        let fd = file.as_raw_fd();
        let page_size = sysconf(_SC_PAGESIZE) as usize;

        // 1. Map the first page of the file
        let addr = mmap(ptr::null_mut(), page_size, PROT_READ, MAP_SHARED, fd, 0);

        if addr == libc::MAP_FAILED {
            return Err("mmap failed".to_string());
        }

        // 2. Prepare the status vector (1 byte per page)
        let mut vec: u8 = 0;

        // 3. Call mincore
        let result = mincore(addr, page_size, &mut vec as *mut u8);

        // Clean up mapping
        munmap(addr, page_size);

        if result == 0 {
            // 4. Check the least significant bit (LSB)
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
    use super::is_first_page_resident;
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
        assert_eq!(is_first_page_resident(path.to_str().unwrap()), Ok(true));
        let _ = fs::remove_file(path);
    }
}
