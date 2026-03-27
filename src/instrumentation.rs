use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

static SPLICE_COUNT: AtomicU64 = AtomicU64::new(0);
static VMSPLICE_COUNT: AtomicU64 = AtomicU64::new(0);
static URING_WRITE_COUNT: AtomicU64 = AtomicU64::new(0);
static THREADS_CONFIGURED: AtomicUsize = AtomicUsize::new(0);
static INFLIGHT_WRITES_MAX: AtomicUsize = AtomicUsize::new(0);
static INFLIGHT_WRITES_CUR: AtomicUsize = AtomicUsize::new(0);

pub fn inc_splice() {
    SPLICE_COUNT.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_vmsplice() {
    VMSPLICE_COUNT.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_uring_write() {
    URING_WRITE_COUNT.fetch_add(1, Ordering::Relaxed);
}

#[allow(unused)]
pub fn set_threads(n: usize) {
    THREADS_CONFIGURED.store(n, Ordering::Relaxed);
}

pub fn note_inflight_inc() {
    let cur = INFLIGHT_WRITES_CUR.fetch_add(1, Ordering::SeqCst) + 1;
    loop {
        let max = INFLIGHT_WRITES_MAX.load(Ordering::Relaxed);
        if cur as usize <= max {
            break;
        }
        if INFLIGHT_WRITES_MAX
            .compare_exchange(max, cur as usize, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            break;
        }
    }
}

pub fn note_inflight_dec() {
    INFLIGHT_WRITES_CUR.fetch_sub(1, Ordering::SeqCst);
}

pub fn print() {
    eprintln!(
        "Instrumentation: threads={}, splice={}, vmsplice={}, uring_writes={}, inflight_cur={}, inflight_max={}",
        THREADS_CONFIGURED.load(Ordering::Relaxed),
        SPLICE_COUNT.load(Ordering::Relaxed),
        VMSPLICE_COUNT.load(Ordering::Relaxed),
        URING_WRITE_COUNT.load(Ordering::Relaxed),
        INFLIGHT_WRITES_CUR.load(Ordering::Relaxed),
        INFLIGHT_WRITES_MAX.load(Ordering::Relaxed)
    );
}
