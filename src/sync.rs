#[cfg(feature = "loom")]
pub(crate) use loom::alloc;
#[cfg(feature = "loom")]
pub(crate) use loom::sync;
#[cfg(feature = "loom")]
pub(crate) use loom::sync::Notify;
// #[cfg(feature = "loom")]
// pub(crate) use loom::sync::RwLock;

#[cfg(not(feature = "loom"))]
pub(crate) use std::alloc;
#[cfg(not(feature = "loom"))]
pub(crate) use std::sync;
#[cfg(not(feature = "loom"))]
pub(crate) use tokio::sync::Notify;
// #[cfg(not(feature = "loom"))]
pub(crate) use tokio::sync::RwLock;
pub(crate) use tokio::sync::RwLockWriteGuard;

#[cfg(feature = "loom")]
type Wake = sync::Mutex<std::collections::VecDeque<sync::Arc<Notify>>>;
#[cfg(not(feature = "loom"))]
type Wake = Notify;

pub(crate) struct Waker(Wake);

#[cfg(feature = "loom")]
impl Waker {
    pub(crate) fn new() -> Self {
        Self(sync::Mutex::new(std::collections::VecDeque::from_iter([sync::Arc::new(Notify::new())])))
    }

    pub(crate) fn notify_waiters(&self) {
        let lock = self.0.lock().unwrap();
        for notify in lock.iter() {
            notify.notify();
        }
    }

    pub(crate) fn notify_one(&self) {
        self.notify_waiters()
    }

    pub(crate) fn resubscribe(&self) -> &Self {
        self.0.lock().unwrap().push_back(sync::Arc::new(Notify::new()));
        self
    }

    pub(crate) async fn notified(&self) {
        let mut lock = self.0.lock().unwrap();
        let notify = lock.pop_front().unwrap();

        crate::debug!(len = lock.len(), "Retrieved notifier");

        lock.push_back(sync::Arc::clone(&notify));
        drop(lock);

        notify.wait();
    }
}

#[cfg(not(feature = "loom"))]
impl Waker {
    pub(crate) fn new() -> Self {
        Self(Notify::new())
    }

    pub(crate) fn notify_waiters(&self) {
        self.0.notify_waiters();
    }

    pub(crate) fn notify_one(&self) {
        self.0.notify_one();
    }

    pub(crate) fn resubscribe(&self) -> &Self {
        self
    }

    pub(crate) async fn notified(&self) {
        self.0.notified().await;
    }
}
