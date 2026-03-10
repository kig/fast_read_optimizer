use libc::{mmap, munmap, mincore, sysconf, _SC_PAGESIZE, MAP_SHARED, PROT_READ};
use std::fs::File;
use std::os::unix::io::AsRawFd;
use std::ptr;

pub fn is_first_page_resident(file_path: &str) -> Result<bool, String> {
    unsafe {
        let file = File::open(file_path).map_err(|e| e.to_string())?;
        let fd = file.as_raw_fd();
        let page_size = sysconf(_SC_PAGESIZE) as usize;

        // 1. Map the first page of the file
        let addr = mmap(
            ptr::null_mut(),
            page_size,
            PROT_READ,
            MAP_SHARED,
            fd,
            0,
        );

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

