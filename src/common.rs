use std::sync::atomic::AtomicI32;

pub static ACTIVE_PAGE_CACHE_READS: AtomicI32 = AtomicI32::new(0);

#[repr(align(4096))]
#[derive(Clone, Copy)]
#[allow(dead_code)]
pub struct PageAligned(pub [u8; 4096]);

pub struct AlignedBuffer {
    allocation: Box<[PageAligned]>,
    ptr: *mut u8,
    len: usize,
}

pub struct MmapCache {
    ptr: *mut libc::c_void,
    len: usize,
}

impl MmapCache {
    pub fn new(file: &std::fs::File) -> Self {
        use std::os::unix::io::AsRawFd;
        let len = file.metadata().unwrap().len() as usize;
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_NONE,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            Self { ptr: std::ptr::null_mut(), len: 0 }
        } else {
            Self { ptr, len }
        }
    }

    pub fn is_range_in_page_cache(&self, offset: u64, len: usize) -> bool {
        if self.ptr.is_null() || offset as usize >= self.len {
            return false;
        }
        let page_size = 4096;
        let offset_aligned = (offset / page_size as u64) * page_size as u64;
        let offset_diff = (offset - offset_aligned) as usize;
        let len_aligned = (len + offset_diff + page_size - 1) / page_size * page_size;
        let check_len = len_aligned.min(self.len - offset_aligned as usize);

        unsafe {
            let num_pages = (check_len + page_size - 1) / page_size;
            let mut vec = vec![0u8; num_pages];
            let res = libc::mincore(self.ptr.add(offset_aligned as usize), check_len, vec.as_mut_ptr());
            if res != 0 { return false; }
            vec.iter().all(|&b| (b & 1) != 0)
        }
    }
}

impl Drop for MmapCache {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { libc::munmap(self.ptr, self.len); }
        }
    }
}

unsafe impl Send for MmapCache {}
unsafe impl Sync for MmapCache {}

pub fn is_range_in_page_cache(file: &std::fs::File, offset: u64, len: usize) -> bool {
    let page_size = 4096;
    let offset_aligned = (offset / page_size as u64) * page_size as u64;
    let offset_diff = (offset - offset_aligned) as usize;
    let len_aligned = (len + offset_diff + page_size - 1) / page_size * page_size;

    unsafe {
        let ptr = libc::mmap(
            std::ptr::null_mut(),
            len_aligned,
            libc::PROT_NONE,
            libc::MAP_SHARED,
            std::os::unix::io::AsRawFd::as_raw_fd(file),
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

impl AlignedBuffer {
    pub fn new(len: usize) -> Self {
        let num_pages = (len + 4095) / 4096;
        let mut allocation = vec![PageAligned([0; 4096]); num_pages].into_boxed_slice();
        let ptr = allocation.as_mut_ptr() as *mut u8;
        
        Self { allocation, ptr, len }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
    
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

// Memory is owned by Box, dropping AlignedBuffer drops the Box normally.
unsafe impl Send for AlignedBuffer {}
unsafe impl Sync for AlignedBuffer {}
