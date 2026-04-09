use super::*;

pub(crate) struct WorkQueue<T> {
    state: Mutex<WorkState<T>>,
    ready: Condvar,
}

struct WorkState<T> {
    queue: VecDeque<T>,
    active_workers: usize,
    waiting_workers: usize,
}

fn work_queue_wake_count(added_items: usize, waiting_workers: usize) -> usize {
    added_items.min(waiting_workers)
}

fn work_queue_should_broadcast_completion(active_workers: usize) -> bool {
    active_workers == 0
}

impl<T> Default for WorkQueue<T> {
    fn default() -> Self {
        Self {
            state: Mutex::new(WorkState {
                queue: VecDeque::new(),
                active_workers: 0,
                waiting_workers: 0,
            }),
            ready: Condvar::new(),
        }
    }
}

impl<T> WorkQueue<T> {
    pub(crate) fn enqueue(&self, items: impl IntoIterator<Item = T>) {
        let mut state = self.state.lock().unwrap();
        let mut added = 0usize;
        for item in items {
            state.queue.push_back(item);
            added += 1;
        }
        let wake_count = work_queue_wake_count(added, state.waiting_workers);
        drop(state);
        for _ in 0..wake_count {
            self.ready.notify_one();
        }
    }

    pub(crate) fn enqueue_one(&self, item: T) {
        let mut state = self.state.lock().unwrap();
        state.queue.push_back(item);
        let wake_count = work_queue_wake_count(1, state.waiting_workers);
        drop(state);
        for _ in 0..wake_count {
            self.ready.notify_one();
        }
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
            state.waiting_workers += 1;
            state = self.ready.wait(state).unwrap();
            state.waiting_workers = state.waiting_workers.saturating_sub(1);
        }
    }

    pub(crate) fn complete_claim(&self) {
        let mut state = self.state.lock().unwrap();
        state.active_workers = state.active_workers.saturating_sub(1);
        let should_notify = work_queue_should_broadcast_completion(state.active_workers);
        drop(state);
        if should_notify {
            self.ready.notify_all();
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn work_queue_wake_count_caps_batch_by_waiters() {
        assert_eq!(work_queue_wake_count(0, 4), 0);
        assert_eq!(work_queue_wake_count(1, 0), 0);
        assert_eq!(work_queue_wake_count(1, 4), 1);
        assert_eq!(work_queue_wake_count(8, 3), 3);
    }

    #[test]
    fn work_queue_only_broadcasts_when_last_worker_finishes() {
        assert!(!work_queue_should_broadcast_completion(3));
        assert!(!work_queue_should_broadcast_completion(1));
        assert!(work_queue_should_broadcast_completion(0));
    }

    #[test]
    fn run_parallel_work_queue_drains_batched_follow_on_work() {
        let queue = Arc::new(WorkQueue::default());
        let stop = Arc::new(AtomicBool::new(false));
        let seen = Arc::new(Mutex::new(Vec::new()));
        queue.enqueue_one(0usize);

        run_parallel_work_queue(queue, stop, 4, {
            let seen = seen.clone();
            move |task, queue, _| {
                seen.lock().unwrap().push(task);
                if task == 0 {
                    queue.enqueue([1usize, 2, 3]);
                }
                Ok(())
            }
        })
        .unwrap();

        let mut seen = Arc::into_inner(seen).unwrap().into_inner().unwrap();
        seen.sort_unstable();
        assert_eq!(seen, vec![0, 1, 2, 3]);
    }
}
