#[repr(align(4096))]
#[derive(Clone, Copy)]
#[allow(dead_code)]
pub struct PageAligned(pub [u8; 4096]);

pub struct AlignedBuffer {
    #[allow(dead_code)]
    allocation: Box<[PageAligned]>,
    ptr: *mut u8,
    len: usize,
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

#[derive(PartialEq, Copy, Clone)]
pub enum IOMode { Auto, Direct, PageCache, }
