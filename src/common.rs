use bytemuck::{Pod, Zeroable};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::{Deref, DerefMut};

#[repr(C, align(4096))]
#[derive(Copy, Clone, Pod, Zeroable)]
#[allow(dead_code)]
pub struct PageAligned(pub [u8; 4096]);

pub struct AlignedBuffer {
    storage: AlignedBufferStorage,
    ptr: *mut u8,
    len: usize,
}

enum AlignedBufferStorage {
    #[allow(dead_code)]
    Pages(Box<[PageAligned]>),
    Mapping {
        ptr: *mut libc::c_void,
        map_len: usize,
    },
}

impl AlignedBuffer {
    pub fn new(len: usize) -> Self {
        let num_pages = (len + 4095) / 4096;
        let mut allocation = vec![PageAligned([0; 4096]); num_pages].into_boxed_slice();
        let ptr = allocation.as_mut_ptr() as *mut u8;

        Self {
            storage: AlignedBufferStorage::Pages(allocation),
            ptr,
            len,
        }
    }

    pub fn new_uninit(len: usize) -> std::io::Result<Self> {
        let map_len = len.max(1).div_ceil(4096) * 4096;
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                map_len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Self {
            storage: AlignedBufferStorage::Mapping { ptr, map_len },
            ptr: ptr.cast(),
            len,
        })
    }

    #[allow(unused)]
    pub fn truncate(&mut self, new_len: usize) -> usize {
        if new_len < self.len {
            self.len = new_len
        }
        return self.len;
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

impl Deref for AlignedBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl DerefMut for AlignedBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

impl fmt::Debug for AlignedBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AlignedBuffer")
            .field("len", &self.len)
            .finish()
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        if let AlignedBufferStorage::Mapping { ptr, map_len } = &self.storage {
            unsafe {
                let _ = libc::munmap(*ptr, *map_len);
            }
        }
    }
}

unsafe impl Send for AlignedBuffer {}
unsafe impl Sync for AlignedBuffer {}

#[derive(PartialEq, Copy, Clone)]
pub enum IOMode {
    Auto,
    Direct,
    PageCache,
}

#[derive(Debug, PartialEq, Eq, Copy, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CopyStrategy {
    Auto,
    Threaded,
    CopyFileRange,
    CopyFileRangeSingle,
    Reflink,
}

#[derive(Debug, PartialEq, Eq, Copy, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CopyAutoMode {
    Heuristic,
    PageCache,
    Direct,
    CopyFileRange,
}

#[derive(Debug, PartialEq, Eq, Copy, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadPathKind {
    SimplePageCache,
    SimpleDirect,
    IoUringPageCache,
    ThreadedPageCache,
    ThreadedDirect,
}

#[derive(Debug, PartialEq, Eq, Copy, Clone, Serialize, Deserialize)]
pub struct ReadAutoStrategy {
    pub hot_large_min_bytes: u64,
    pub cold_large_min_bytes: u64,
    pub hot_small_path: ReadPathKind,
    pub hot_large_path: ReadPathKind,
    pub cold_small_path: ReadPathKind,
    pub cold_large_path: ReadPathKind,
}

#[cfg(test)]
mod tests {
    use super::AlignedBuffer;

    #[test]
    fn aligned_buffer_page_storage_is_aligned_and_mutable() {
        let mut buffer = AlignedBuffer::new(8193);
        let ptr = buffer.as_slice().as_ptr() as usize;

        assert_eq!(ptr % 4096, 0);
        assert_eq!(buffer.len(), 8193);

        buffer.as_mut_slice()[0] = 0x11;
        buffer.as_mut_slice()[4096] = 0x22;
        buffer.as_mut_slice()[8192] = 0x33;

        assert_eq!(buffer.as_slice()[0], 0x11);
        assert_eq!(buffer.as_slice()[4096], 0x22);
        assert_eq!(buffer.as_slice()[8192], 0x33);
    }
}
