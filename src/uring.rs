// Cross-platform wrapper for io_uring. On Linux re-export the real type from the iou crate.
// On non-Linux platforms provide a lightweight synchronous stub that implements the
// small subset of the iou::IoUring API used by this repository.

#[cfg(target_os = "linux")]
pub use iou::IoUring;
#[cfg(target_os = "linux")]
pub use iou::sqe::SpliceFlags;

#[cfg(not(target_os = "linux"))]
mod stub {
    use std::collections::VecDeque;
    use std::io;
    use std::os::raw::c_void;
    use std::os::unix::io::RawFd;
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::Duration;

    #[derive(Default)]
    struct SqeData {
        user_data: u64,
        op: Option<SqeOp>,
    }

    #[derive(Default)]
    enum SqeOp {
        Read { fd: RawFd, buf: *mut u8, len: usize, offset: u64 },
        Write { fd: RawFd, buf: *const u8, len: usize, offset: u64 },
        Splice { in_fd: RawFd, out_fd: RawFd, len: usize, flags: u32 },
        #[default]
        None,
    }

    struct CqeData {
        user_data: u64,
        res: isize,
    }

    struct Inner {
        max_entries: usize,
        sqes: Vec<SqeData>,
        cqes: VecDeque<CqeData>,
    }

    struct InnerSync {
        inner: Mutex<Inner>,
        cv: Condvar,
    }

    #[derive(Clone)]
    pub struct IoUring {
        inner: Arc<InnerSync>,
    }

    impl IoUring {
        pub fn new(entries: u32) -> io::Result<IoUring> {
            let inner = Inner {
                max_entries: entries as usize,
                sqes: Vec::with_capacity(entries as usize),
                cqes: VecDeque::new(),
            };
            Ok(IoUring {
                inner: Arc::new(InnerSync {
                    inner: Mutex::new(inner),
                    cv: Condvar::new(),
                }),
            })
        }

        pub fn prepare_sqe(&mut self) -> Option<Sqe> {
            let mut guard = self.inner.inner.lock().unwrap();
            if guard.sqes.len() >= guard.max_entries {
                return None;
            }
            guard.sqes.push(SqeData::default());
            let idx = guard.sqes.len() - 1;
            // Return a handle that modifies the shared sqe slot by index.
            Some(Sqe {
                inner: Arc::downgrade(&self.inner),
                idx,
            })
        }

        pub fn submit_sqes(&mut self) -> io::Result<()> {
            // Drain all prepared SQEs and execute them synchronously.
            let mut to_exec = Vec::new();
            {
                let mut guard = self.inner.inner.lock().unwrap();
                to_exec.append(&mut guard.sqes);
            }

            for sqe in to_exec.into_iter() {
                // Execute op synchronously and push CQE.
                let (user_data, res) = match sqe.op {
                    Some(SqeOp::Read { fd, buf, len, offset }) => unsafe {
                        if buf.is_null() {
                            (sqe.user_data, -1)
                        } else {
                            // Use pread to read into the provided buffer.
                            let ret = libc::pread(fd, buf as *mut c_void, len, offset as libc::off_t);
                            if ret < 0 {
                                (sqe.user_data, -1)
                            } else {
                                (sqe.user_data, ret as isize)
                            }
                        }
                    },
                    Some(SqeOp::Write { fd, buf, len, offset }) => unsafe {
                        if buf.is_null() {
                            (sqe.user_data, -1)
                        } else {
                            let ret = libc::pwrite(fd, buf as *const c_void, len, offset as libc::off_t);
                            if ret < 0 {
                                (sqe.user_data, -1)
                            } else {
                                (sqe.user_data, ret as isize)
                            }
                        }
                    },
                    Some(SqeOp::Splice { in_fd, out_fd, len, flags }) => unsafe {
                        // Fallback splice implementation: perform a looped read/write and return a (user_data, res)
                        let res = 'splice_block: {
                            let mut total_moved: isize = 0;
                            let mut buf = vec![0u8; 64 * 1024];
                            let mut remaining = len;
                            while remaining > 0 {
                                let to_read = remaining.min(buf.len());
                                let nread = libc::read(in_fd, buf.as_mut_ptr() as *mut libc::c_void, to_read);
                                if nread < 0 {
                                    let err = io::Error::last_os_error();
                                    if let Some(libc::EINTR) = err.raw_os_error() {
                                        continue;
                                    }
                                    break 'splice_block (sqe.user_data, -1);
                                }
                                if nread == 0 {
                                    break;
                                }
                                let mut write_ptr = 0usize;
                                while write_ptr < nread as usize {
                                    let nw = libc::write(
                                        out_fd,
                                        buf[write_ptr..nread as usize].as_ptr() as *const libc::c_void,
                                        (nread as usize - write_ptr) as libc::size_t,
                                    );
                                    if nw < 0 {
                                        break 'splice_block (sqe.user_data, -1);
                                    }
                                    write_ptr += nw as usize;
                                }
                                total_moved += nread as isize;
                                remaining = remaining.saturating_sub(nread as usize);
                            }
                            (sqe.user_data, total_moved)
                        };
                        res
                    },
                    _ => (sqe.user_data, 0),
                };
                let mut guard = self.inner.inner.lock().unwrap();
                guard.cqes.push_back(CqeData { user_data, res });
                // Notify any waiter.
                self.inner.cv.notify_all();
            }
            Ok(())
        }

        pub fn wait_for_cqe(&mut self) -> io::Result<Cqe> {
            let mut guard = self.inner.inner.lock().unwrap();
            while guard.cqes.is_empty() {
                guard = self.inner.cv.wait(guard).unwrap();
            }
            let c = guard.cqes.pop_front().unwrap();
            Ok(Cqe { user_data: c.user_data, res: c.res })
        }

        pub fn peek_for_cqe(&mut self) -> Option<Cqe> {
            let mut guard = self.inner.inner.lock().unwrap();
            guard.cqes.pop_front().map(|c| Cqe { user_data: c.user_data, res: c.res })
        }

        pub fn cq_ready(&self) -> usize {
            let guard = self.inner.inner.lock().unwrap();
            guard.cqes.len()
        }
    }

    use std::sync::Weak;

    pub struct Sqe {
        inner: Weak<InnerSync>,
        idx: usize,
    }

    impl Sqe {
        pub fn prep_read(&mut self, fd: RawFd, buf: &mut [u8], offset: u64) {
            if let Some(inner) = self.inner.upgrade() {
                let mut guard = inner.inner.lock().unwrap();
                if let Some(slot) = guard.sqes.get_mut(self.idx) {
                    slot.op = Some(SqeOp::Read { fd, buf: buf.as_mut_ptr(), len: buf.len(), offset });
                }
            }
        }

        pub fn prep_write(&mut self, fd: RawFd, buf: &[u8], offset: u64) {
            if let Some(inner) = self.inner.upgrade() {
                let mut guard = inner.inner.lock().unwrap();
                if let Some(slot) = guard.sqes.get_mut(self.idx) {
                    slot.op = Some(SqeOp::Write { fd, buf: buf.as_ptr(), len: buf.len(), offset });
                }
            }
        }

        pub fn prep_splice(&mut self, in_fd: RawFd, _in_off: i64, out_fd: RawFd, _out_off: i64, len: usize, _flags: SpliceFlags) {
            if let Some(inner) = self.inner.upgrade() {
                let mut guard = inner.inner.lock().unwrap();
                if let Some(slot) = guard.sqes.get_mut(self.idx) {
                    slot.op = Some(SqeOp::Splice { in_fd, out_fd, len, flags: 0 });
                }
            }
        }

        pub fn set_user_data(&mut self, user_data: u64) {
            if let Some(inner) = self.inner.upgrade() {
                let mut guard = inner.inner.lock().unwrap();
                if let Some(slot) = guard.sqes.get_mut(self.idx) {
                    slot.user_data = user_data;
                }
            }
        }
    }

    pub struct Cqe {
        user_data: u64,
        res: isize,
    }

    impl Cqe {
        pub fn user_data(&self) -> u64 {
            self.user_data
        }

        pub fn result(&self) -> io::Result<i32> {
            if self.res >= 0 {
                Ok(self.res as i32)
            } else {
                Err(io::Error::last_os_error())
            }
        }

        /// Return the raw isize result (negative for error). Matches the liburing/io_uring CQE raw result.
        pub fn raw_result(&self) -> isize {
            self.res
        }
    }

    impl std::fmt::Debug for IoUring {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "IoUring(stub)")
        }
    }

    #[derive(Clone, Copy, Debug, Default)]
    pub struct SpliceFlags {
        bits: u32,
    }
    impl SpliceFlags {
        pub fn empty() -> Self { Self { bits: 0 } }
    }

}


#[cfg(not(target_os = "linux"))]
pub use stub::IoUring;
#[cfg(not(target_os = "linux"))]
pub use stub::Cqe;
#[cfg(not(target_os = "linux"))]
pub use stub::SpliceFlags;