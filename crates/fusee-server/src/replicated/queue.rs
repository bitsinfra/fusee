//! Bounded hand-off from application threads to the protocol thread.
//!
//! Who waits on whom: the shard thread (flush batches, checkpoint barriers,
//! permit commands), the permit maintenance thread and server connection
//! tasks wait here for room. The protocol thread is the only consumer and
//! never sends into these queues. It takes a value off as soon as its loop
//! runs, without waiting for a producer, a peer or a commit: a proposal
//! leaves the queue when it is proposed, before its quorum round trip.
//! Socket readers feed a separate queue on the protocol thread's own runtime.
//! A full queue therefore delays a producer by at most one protocol
//! iteration, and no cycle of waits exists.
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering, fence};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tokio::sync::mpsc::{self, error::TrySendError};

thread_local! {
    static CONSUMER: Cell<bool> = const { Cell::new(false) };
}

/// Marks the calling thread as the protocol thread. It never waits for room
/// in its own queues: a full queue returns the value instead.
pub(super) fn mark_consumer() {
    CONSUMER.with(|c| c.set(true));
}

#[derive(Default)]
struct Room {
    waiting: AtomicUsize,
    lock: Mutex<()>,
    freed: Condvar,
}

impl Room {
    fn wake(&self) {
        // Pairs with the fence in `Sender::wait`: either the waiter's retry
        // sees the freed slot, or this load sees the waiter.
        fence(Ordering::SeqCst);
        if self.waiting.load(Ordering::SeqCst) > 0 {
            let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
            self.freed.notify_all();
        }
    }
}

pub(super) struct Sender<T> {
    tx: mpsc::Sender<T>,
    room: Arc<Room>,
}

pub(super) struct Receiver<T> {
    rx: mpsc::Receiver<T>,
    room: Arc<Room>,
}

pub(super) fn bounded<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = mpsc::channel(capacity);
    let room = Arc::new(Room::default());
    (
        Sender {
            tx,
            room: room.clone(),
        },
        Receiver { rx, room },
    )
}

impl<T> Sender<T> {
    /// Hands `value` to the protocol thread, blocking the calling thread
    /// while the queue is full. Returns the value if the protocol thread has
    /// stopped, or if the caller is the protocol thread and the queue is full.
    pub(super) fn send(&self, value: T) -> Result<(), T> {
        match self.tx.try_send(value) {
            Ok(()) => Ok(()),
            Err(TrySendError::Closed(value)) => Err(value),
            Err(TrySendError::Full(value)) if CONSUMER.with(Cell::get) => Err(value),
            Err(TrySendError::Full(value)) => self.wait(value),
        }
    }

    #[cold]
    fn wait(&self, mut value: T) -> Result<(), T> {
        self.room.waiting.fetch_add(1, Ordering::SeqCst);
        let mut guard = self.room.lock.lock().unwrap_or_else(|e| e.into_inner());
        let result = loop {
            fence(Ordering::SeqCst);
            match self.tx.try_send(value) {
                Ok(()) => break Ok(()),
                Err(TrySendError::Closed(v)) => break Err(v),
                Err(TrySendError::Full(v)) => value = v,
            }
            // The receiver wakes waiters whenever it takes a value. The
            // timeout only bounds the cost of a wake-up that raced a retry.
            guard = self
                .room
                .freed
                .wait_timeout(guard, Duration::from_millis(1))
                .unwrap_or_else(|e| e.into_inner())
                .0;
        };
        drop(guard);
        self.room.waiting.fetch_sub(1, Ordering::SeqCst);
        result
    }

    /// Like [`send`](Self::send), for async callers: waits without blocking
    /// the runtime thread.
    pub(super) async fn send_async(&self, value: T) -> Result<(), T> {
        self.tx.send(value).await.map_err(|e| e.0)
    }

    pub(super) fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    /// Nothing waits in the queue (a value may arrive at any time).
    pub(super) fn is_empty(&self) -> bool {
        self.tx.capacity() == self.tx.max_capacity()
    }
}

impl<T> Receiver<T> {
    /// Cancel safe.
    pub(super) async fn recv(&mut self) -> Option<T> {
        let value = self.rx.recv().await;
        self.room.wake();
        value
    }

    pub(super) fn is_empty(&self) -> bool {
        self.rx.is_empty()
    }

    pub(super) fn try_recv(&mut self) -> Option<T> {
        let value = self.rx.try_recv().ok();
        if value.is_some() {
            self.room.wake();
        }
        value
    }

    /// Refuses new values and releases blocked producers. Values already
    /// queued can still be received.
    pub(super) fn close(&mut self) {
        self.rx.close();
        self.room.wake();
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::time::Instant;

    #[test]
    fn a_burst_waits_for_room_and_arrives_whole_and_in_order() {
        const CAPACITY: usize = 8;
        const PRODUCERS: u64 = 6;
        const EACH: u64 = 500;
        let (tx, mut rx) = bounded::<(u64, u64)>(CAPACITY);
        let tx = Arc::new(tx);
        let sent = Arc::new(AtomicUsize::new(0));
        let producers: Vec<_> = (0..PRODUCERS)
            .map(|p| {
                let tx = tx.clone();
                let sent = sent.clone();
                std::thread::spawn(move || {
                    for i in 0..EACH {
                        assert!(tx.send((p, i)).is_ok());
                        sent.fetch_add(1, Ordering::SeqCst);
                    }
                })
            })
            .collect();
        // With nobody receiving, producers stop at the bound.
        let deadline = Instant::now() + Duration::from_secs(10);
        while sent.load(Ordering::SeqCst) < CAPACITY {
            assert!(
                Instant::now() < deadline,
                "producers never filled the queue"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(sent.load(Ordering::SeqCst), CAPACITY);
        let mut next = vec![0; PRODUCERS as usize];
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        runtime.block_on(async {
            for _ in 0..PRODUCERS * EACH {
                let (p, i) = if let Some(v) = rx.try_recv() {
                    v
                } else {
                    rx.recv().await.unwrap()
                };
                assert_eq!(next[p as usize], i, "producer {p} reordered");
                next[p as usize] += 1;
            }
        });
        for p in producers {
            p.join().unwrap();
        }
        assert!(next.iter().all(|&n| n == EACH));
        assert!(rx.try_recv().is_none());
    }

    #[test]
    fn stopping_the_consumer_releases_blocked_producers() {
        let (tx, rx) = bounded::<u32>(1);
        tx.send(0).unwrap();
        let blocked = Arc::new(AtomicBool::new(true));
        let producer = std::thread::spawn({
            let blocked = blocked.clone();
            move || {
                let result = tx.send(1);
                blocked.store(false, Ordering::SeqCst);
                result
            }
        });
        std::thread::sleep(Duration::from_millis(30));
        assert!(blocked.load(Ordering::SeqCst));
        drop(rx);
        assert_eq!(producer.join().unwrap(), Err(1));
    }

    #[test]
    fn the_protocol_thread_never_waits_for_its_own_queue() {
        let (tx, mut rx) = bounded::<u32>(1);
        std::thread::spawn(move || {
            mark_consumer();
            tx.send(0).unwrap();
            let started = Instant::now();
            assert_eq!(tx.send(1), Err(1));
            assert!(started.elapsed() < Duration::from_millis(500));
        })
        .join()
        .unwrap();
        assert_eq!(rx.try_recv(), Some(0));
    }
}
