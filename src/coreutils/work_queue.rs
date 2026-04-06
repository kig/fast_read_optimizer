use super::*;

pub(crate) struct WorkQueue<T> {
    state: Mutex<WorkState<T>>,
    ready: Condvar,
}

struct WorkState<T> {
    queue: VecDeque<T>,
    active_workers: usize,
}

impl<T> Default for WorkQueue<T> {
    fn default() -> Self {
        Self {
            state: Mutex::new(WorkState {
                queue: VecDeque::new(),
                active_workers: 0,
            }),
            ready: Condvar::new(),
        }
    }
}

impl<T> WorkQueue<T> {
    pub(crate) fn enqueue(&self, items: impl IntoIterator<Item = T>) {
        let mut state = self.state.lock().unwrap();
        let mut added = false;
        for item in items {
            state.queue.push_back(item);
            added = true;
        }
        if added {
            self.ready.notify_all();
        }
    }

    pub(crate) fn enqueue_one(&self, item: T) {
        let mut state = self.state.lock().unwrap();
        state.queue.push_back(item);
        self.ready.notify_one();
    }

    pub(crate) fn claim(&self, stop: &AtomicBool) -> Option<T> {
        let mut state = self.state.lock().unwrap();
        loop {
            if stop.load(Ordering::SeqCst) {
                return None;
            }
            if let Some(item) = state.queue.pop_front() {
                state.active_workers += 1;
                return Some(item);
            }
            if state.active_workers == 0 {
                return None;
            }
            state = self.ready.wait(state).unwrap();
        }
    }

    pub(crate) fn complete_claim(&self) {
        let mut state = self.state.lock().unwrap();
        state.active_workers = state.active_workers.saturating_sub(1);
        self.ready.notify_all();
    }

    pub(crate) fn wake_all(&self) {
        self.ready.notify_all();
    }
}
pub(crate) fn run_parallel_work_queue<T, F>(
    queue: Arc<WorkQueue<T>>,
    stop: Arc<AtomicBool>,
    worker_count: usize,
    run_task: F,
) -> io::Result<()>
where
    T: Send + 'static,
    F: Fn(T, &WorkQueue<T>, &AtomicBool) -> io::Result<()> + Send + Sync + 'static,
{
    let run_task = Arc::new(run_task);
    let mut threads = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        let queue = queue.clone();
        let stop = stop.clone();
        let run_task = run_task.clone();
        threads.push(std::thread::spawn(move || -> io::Result<()> {
            while let Some(item) = queue.claim(&stop) {
                let result = run_task(item, &queue, &stop);
                queue.complete_claim();
                if let Err(err) = result {
                    stop.store(true, Ordering::SeqCst);
                    queue.wake_all();
                    return Err(err);
                }
            }
            Ok(())
        }));
    }

    let mut first_error = None;
    for thread in threads {
        match thread
            .join()
            .map_err(|_| io::Error::other("directory walk worker thread panicked"))?
        {
            Ok(()) => {}
            Err(err) if first_error.is_none() => first_error = Some(err),
            Err(_) => {}
        }
    }
    if let Some(err) = first_error {
        return Err(err);
    }
    Ok(())
}
