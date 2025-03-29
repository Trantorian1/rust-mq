#[cfg(feature = "loom")]
use loom::alloc;
#[cfg(feature = "loom")]
use loom::sync;
#[cfg(feature = "loom")]
use loom::sync::Notify;

#[cfg(not(feature = "loom"))]
use std::alloc;
#[cfg(not(feature = "loom"))]
use std::sync;
#[cfg(not(feature = "loom"))]
use tokio::sync::Notify;

const WRIT_INDX_MASK: u64 = 0xffff000000000000;
const WRIT_SIZE_MASK: u64 = 0x0000ffff00000000;
const READ_INDX_MASK: u64 = 0x00000000ffff0000;
const READ_SIZE_MASK: u64 = 0x000000000000ffff;

#[cfg(test)]
pub trait TBound: Send + Clone + std::fmt::Debug {}
#[cfg(test)]
impl<T: Send + Clone + std::fmt::Debug> TBound for T {}

macro_rules! debug {
    ($($arg:tt)+) => {
        #[cfg(test)]
        tracing::debug!($($arg)+)
    };
}

macro_rules! warn {
    ($($arg:tt)+) => {
        #[cfg(test)]
        tracing::warn!($($arg)+)
    };
}

macro_rules! error {
    ($($arg:tt)+) => {
        #[cfg(test)]
        tracing::error!($($arg)+)
    };
}

#[cfg(not(test))]
pub trait TBound: Send + Clone {}
#[cfg(not(test))]
impl<T: Send + Clone> TBound for T {}

pub struct MqSender<T: TBound> {
    queue: sync::Arc<MessageQueue<T>>,
    close: sync::Arc<sync::atomic::AtomicBool>,
    #[cfg(feature = "loom")]
    wake: sync::Arc<sync::Mutex<std::collections::VecDeque<sync::Arc<Notify>>>>,
    #[cfg(not(feature = "loom"))]
    wake: sync::Arc<Notify>,
}

pub struct MqReceiver<T: TBound> {
    queue: sync::Arc<MessageQueue<T>>,
    #[cfg(feature = "loom")]
    wake: sync::Arc<sync::Mutex<std::collections::VecDeque<sync::Arc<Notify>>>>,
    #[cfg(not(feature = "loom"))]
    wake: sync::Arc<Notify>,
}

#[must_use]
pub struct MqGuard<'a, T: TBound> {
    ack: bool,
    queue: sync::Arc<MessageQueue<T>>,
    cell: &'a mut AckCell<T>,
    _phantom: std::marker::PhantomData<&'a ()>,
}

struct MessageQueue<T: TBound> {
    ring: std::ptr::NonNull<AckCell<T>>,
    senders: sync::atomic::AtomicUsize,
    read_write: sync::atomic::AtomicU64,
    cap: u16,
}

/// An atomic acknowledge cell, use to ensure an element has been read.
struct AckCell<T: TBound> {
    elem: std::mem::MaybeUninit<T>,
    ack: sync::atomic::AtomicBool,
}

unsafe impl<T: TBound> Send for MqSender<T> {}
unsafe impl<T: TBound> Sync for MqSender<T> {}

unsafe impl<T: TBound> Send for MqReceiver<T> {}
unsafe impl<T: TBound> Sync for MqReceiver<T> {}

unsafe impl<T: TBound> Send for MqGuard<'_, T> {}
unsafe impl<T: TBound> Sync for MqGuard<'_, T> {}

#[cfg(test)]
impl<T: TBound> std::fmt::Debug for MqSender<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqSender").field("queue", &self.queue).finish()
    }
}

#[cfg(test)]
impl<T: TBound> std::fmt::Debug for MqReceiver<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqSender").field("queue", &self.queue).finish()
    }
}

#[cfg(test)]
impl<T: TBound> std::fmt::Debug for MqGuard<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqGuard").field("ack", &self.ack).field("elem", &self.read()).finish()
    }
}

#[cfg(test)]
impl<T: TBound> std::fmt::Debug for MessageQueue<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let senders = self.senders.load(sync::atomic::Ordering::Acquire);
        let read_write = self.read_write.load(sync::atomic::Ordering::Acquire);
        f.debug_struct("MessageQueue")
            .field("senders", &senders)
            .field("read_write", &read_write)
            .field("cap", &self.cap)
            .finish()
    }
}

impl<T: TBound> Drop for MqSender<T> {
    fn drop(&mut self) {
        if !self.close.load(sync::atomic::Ordering::Acquire) {
            self.queue.sender_unregister();

            #[cfg(feature = "loom")]
            {
                let lock = self.wake.lock().unwrap();
                for notify in lock.iter() {
                    notify.notify();
                }
            }
            #[cfg(not(feature = "loom"))]
            self.wake.notify_waiters();
        }
    }
}

impl<T: TBound> Drop for MqGuard<'_, T> {
    fn drop(&mut self) {
        // If the element was not acknowledged, we add it back to the queue to be picked up again,
        // taking special care to drop the element in the process. We do not need to do this if the
        // element was acknowledged since the drop logic is implemented in `elem_drop` and this is
        // handled by `acknowledge` if it is called (which also sets the `ack` flag so we don't
        // double free here).
        if !self.ack {
            let elem = self.elem_take();
            self.cell.ack.store(true, sync::atomic::Ordering::Release);

            // This can still fail, but without async drop there isn't really anything better we can
            // do. Should we panic?
            let res = self.queue.write(elem);
            debug_assert!(res.is_none());
        }
    }
}

impl<T: TBound> Drop for MessageQueue<T> {
    fn drop(&mut self) {
        warn!("Dropping message queue");

        let raw_bytes = self.read_write.swap(0, sync::atomic::Ordering::Release);

        // Cast to u64 to avoid overflow
        let read_indx = get_read_indx(raw_bytes) as u64;
        let read_size = get_read_size(raw_bytes) as u64;
        let stop = read_indx + read_size;

        debug!(read_indx, read_size, stop, "Dropping elements");

        for i in (read_indx..stop).map(|i| fast_mod(i, self.cap)) {
            unsafe { self.ring.add(i as usize).read() };
        }

        let layout = alloc::Layout::array::<AckCell<T>>(self.cap as usize).unwrap();
        unsafe { alloc::dealloc(self.ring.as_ptr() as *mut u8, layout) }
    }
}

#[cfg_attr(test, tracing::instrument)]
pub fn channel<T: TBound>(cap: u16) -> (MqSender<T>, MqReceiver<T>) {
    debug!("Creating new channel");

    let queue_s = sync::Arc::new(MessageQueue::new(cap));
    let queue_r = sync::Arc::clone(&queue_s);

    #[cfg(feature = "loom")]
    let wake_s = sync::Arc::new(sync::Mutex::new(std::collections::VecDeque::from_iter(
        [sync::Arc::new(Notify::new())].into_iter(),
    )));
    #[cfg(not(feature = "loom"))]
    let wake_s = sync::Arc::new(Notify::new());
    let wake_r = sync::Arc::clone(&wake_s);

    let sx = MqSender::new(queue_s, wake_s);
    let rx = MqReceiver::new(queue_r, wake_r);

    (sx, rx)
}

impl<T: TBound> MqSender<T> {
    fn new(
        queue: sync::Arc<MessageQueue<T>>,
        #[cfg(feature = "loom")] wake: sync::Arc<sync::Mutex<std::collections::VecDeque<sync::Arc<Notify>>>>,
        #[cfg(not(feature = "loom"))] wake: sync::Arc<Notify>,
    ) -> Self {
        queue.sender_register();
        Self { queue, close: sync::Arc::new(sync::atomic::AtomicBool::new(false)), wake }
    }

    fn resubscribe(&self) -> Self {
        self.queue.sender_register();
        Self {
            queue: sync::Arc::clone(&self.queue),
            close: sync::Arc::clone(&self.close),
            wake: sync::Arc::clone(&self.wake),
        }
    }

    #[cfg_attr(test, tracing::instrument(skip(self)))]
    pub fn send(&self, elem: T) -> Option<T> {
        debug!("Trying to send value");
        match self.queue.write(elem) {
            // Failed to write to the queue. This can happen if the next element right after the
            // write region has been read but not acknowledge yet.
            Some(elem) => {
                error!("Failed to send value");
                Some(elem)
            }
            None => {
                debug!("Value sent successfully");

                #[cfg(feature = "loom")]
                {
                    let lock = self.wake.lock().unwrap();
                    for notify in lock.iter() {
                        notify.notify();
                    }
                }
                #[cfg(not(feature = "loom"))]
                self.wake.notify_one();

                None
            }
        }
    }

    pub fn close(self) {
        warn!("Closing channel");

        self.queue.sender_unregister_all();
        self.close.swap(true, sync::atomic::Ordering::AcqRel);
        let raw_bytes = self.queue.read_write.swap(0, sync::atomic::Ordering::AcqRel);

        if raw_bytes != 0 {
            // Cast to u64 to avoid overflow
            let read_indx = get_read_indx(raw_bytes) as u64;
            let read_size = get_read_size(raw_bytes) as u64;
            let stop = read_indx + read_size;

            debug!(read_indx, read_size, stop, "Dropping elements");

            for i in (read_indx..stop).map(|i| fast_mod(i, self.queue.cap)) {
                unsafe { self.queue.ring.add(i as usize).read().elem.assume_init() };
            }

            #[cfg(feature = "loom")]
            {
                let lock = self.wake.lock().unwrap();
                for notify in lock.iter() {
                    notify.notify();
                }
            }
            #[cfg(not(feature = "loom"))]
            self.wake.notify_waiters();
        }
    }
}

impl<T: TBound> MqReceiver<T> {
    fn new(
        queue: sync::Arc<MessageQueue<T>>,
        #[cfg(feature = "loom")] wake: sync::Arc<sync::Mutex<std::collections::VecDeque<sync::Arc<Notify>>>>,
        #[cfg(not(feature = "loom"))] wake: sync::Arc<Notify>,
    ) -> Self {
        Self { queue, wake }
    }

    fn resubscribe(&self) -> Self {
        #[cfg(feature = "loom")]
        self.wake.lock().unwrap().push_back(sync::Arc::new(Notify::new()));

        Self { queue: sync::Arc::clone(&self.queue), wake: sync::Arc::clone(&self.wake) }
    }

    #[cfg_attr(test, tracing::instrument(skip(self)))]
    pub async fn recv(&self) -> Option<MqGuard<T>> {
        debug!("Trying to receive value");
        loop {
            match self.queue.read() {
                Some(cell_ptr) => {
                    let guard = MqGuard::new(sync::Arc::clone(&self.queue), cell_ptr);
                    debug!("Received value");
                    break Some(guard);
                }
                None => {
                    debug!("Failed to receive value");
                    if !self.queue.sender_available() && self.queue.read_size() == 0 {
                        debug!("No sender available");
                        break None;
                    } else {
                        debug!("Waiting for a send");

                        #[cfg(feature = "loom")]
                        {
                            let mut lock = self.wake.lock().unwrap();
                            let len = lock.len();
                            let notify = lock.pop_front().unwrap();

                            debug!(len, "Retrieved notifier");

                            lock.push_back(sync::Arc::clone(&notify));
                            drop(lock);

                            notify.wait();
                        }
                        #[cfg(not(feature = "loom"))]
                        self.wake.notified().await;

                        debug!("A send was detected");
                    }
                }
            }
        }
    }
}

impl<'a, T: TBound> MqGuard<'a, T> {
    fn new(queue: sync::Arc<MessageQueue<T>>, cell: &'a mut AckCell<T>) -> Self {
        MqGuard { ack: false, queue, cell, _phantom: std::marker::PhantomData }
    }

    pub fn read(&self) -> T {
        unsafe { self.cell.elem.assume_init_ref().clone() }
    }

    #[tracing::instrument(skip(self))]
    pub fn read_acknowledge(mut self) -> T {
        let elem = self.elem_take();
        debug!(?elem, "Acknowledging");
        self.acknowledge();
        elem
    }

    pub fn acknowledge(mut self) {
        self.ack = true;
        self.cell.ack.store(true, sync::atomic::Ordering::Release);
    }

    // Invariant: calling this twice will result in a double free
    fn elem_take(&mut self) -> T {
        let mut elem = std::mem::MaybeUninit::uninit();
        std::mem::swap(&mut self.cell.elem, &mut elem);
        unsafe { elem.assume_init() }
    }
}

impl<T: TBound> MessageQueue<T> {
    #[tracing::instrument]
    fn new(cap: u16) -> Self {
        assert!(cap > 0, "Tried to create a message queue with a capacity < 1");

        let cap = cap.checked_next_power_of_two().expect("failed to retrieve the next power of 2 to cap");
        debug!(cap, "Determining array layout");
        let layout = alloc::Layout::array::<AckCell<T>>(cap as usize).unwrap();

        // From the `Layout` docs: "All layouts have an associated size and a power-of-two alignment.
        // The size, when rounded up to the nearest multiple of align, does not overflow isize (i.e.
        // the rounded value will always be less than or equal to isize::MAX)."
        //
        // I could not find anything in the source code of this method that checks that so making
        // sure here, is this really necessary?
        assert!(layout.size() <= isize::MAX as usize);

        debug!(?layout, "Allocating layout");
        let ptr = unsafe { alloc::alloc(layout) };
        let ring = match std::ptr::NonNull::new(ptr as *mut AckCell<T>) {
            Some(p) => p,
            None => std::alloc::handle_alloc_error(layout),
        };

        let senders = sync::atomic::AtomicUsize::new(0);
        let read_write = sync::atomic::AtomicU64::new(get_raw_bytes(0, cap, 0, 0));

        Self { ring, cap, senders, read_write }
    }

    #[cfg_attr(test, tracing::instrument(skip(self)))]
    fn sender_register(&self) {
        let senders = self.senders.fetch_add(1, sync::atomic::Ordering::AcqRel);
        debug!(senders = senders + 1, "Increasing sender count");
        debug_assert_ne!(senders, usize::MAX);
    }

    #[cfg_attr(test, tracing::instrument(skip(self)))]
    fn sender_unregister(&self) {
        let senders = self.senders.fetch_sub(1, sync::atomic::Ordering::AcqRel);
        debug!(senders = senders - 1, "Decreasing sender count");
        debug_assert_ne!(senders, 0);
    }

    fn sender_unregister_all(&self) {
        self.senders.store(0, sync::atomic::Ordering::Release);
    }

    #[cfg_attr(test, tracing::instrument(skip(self)))]
    fn sender_available(&self) -> bool {
        let senders = self.senders.load(sync::atomic::Ordering::Acquire);
        debug!(senders, "Senders available");
        senders > 0
    }

    fn writ_indx(&self) -> u16 {
        get_writ_indx(self.read_write.load(sync::atomic::Ordering::Acquire))
    }

    fn writ_size(&self) -> u16 {
        get_writ_size(self.read_write.load(sync::atomic::Ordering::Acquire))
    }

    fn read_indx(&self) -> u16 {
        get_read_indx(self.read_write.load(sync::atomic::Ordering::Acquire))
    }

    fn read_size(&self) -> u16 {
        get_read_size(self.read_write.load(sync::atomic::Ordering::Acquire))
    }

    #[cfg_attr(test, tracing::instrument(skip(self)))]
    fn read(&self) -> Option<&mut AckCell<T>> {
        let mut raw_bytes = self.read_write.load(sync::atomic::Ordering::SeqCst);
        loop {
            let writ_indx = get_writ_indx(raw_bytes);
            let writ_size = get_writ_size(raw_bytes);
            let read_indx = get_read_indx(raw_bytes);
            let read_size = get_read_size(raw_bytes);
            debug!(writ_indx, writ_size, read_indx, read_size, "Trying to read from buffer");

            if read_size == 0 {
                // Note that we do not try to grow the read region in case there is nothing left to
                // read. This is because while cells have and `ack` state to attest if they have
                // been read, we do not store any extra information concerning their write status.
                // Instead, it is the responsibility of the queue to grow the read region whenever
                // it writes a new value.
                debug!("Failed to read from buffer");
                break None;
            } else {
                debug!(read_indx, "Reading from buffer");

                let read_indx_new = fast_mod(read_indx + 1, self.cap);
                let raw_bytes_new = get_raw_bytes(writ_indx, writ_size, read_indx_new, read_size - 1);

                // So, this is a bit complicated. The issue is that we are mixing atomic (`load`,
                // `store`) with non atomic (mod, decrement) operations. Why is this a problem?
                // Well, when performing a `fetch_add` for example, the operation takes place as a
                // single atomic transaction (the fetch and the add happen simultaneously, and its
                // changes can be seen across threads as long as you use `AcRel` ordering). This is
                // not the case here: we `load` an atomic, we compute a change and then we `store`
                // it. Critically, we can only guarantee the ordering of atomic operations across
                // threads. We cannot guarantee that our (non-atomic) computation of `strt_new` and
                // `size_new` will be synchronized with other threads. In other words, it is
                // possible for the value of `start_and_size` to _change_ between our `load` and
                // `store`. Atomic fences will _not_ solve this problem since they only guarantee
                // relative ordering between atomic operations.
                //
                // `compare_exchange` allows us to work around this problem by updating an atomic
                // _only if its value has not changed from what we expect_. In other words, we ask
                // it to update `strt_and_size` only if `strt_and_size` has not been changed by
                // another thread in the meantime. If this is not the case, we re-try the whole
                // operations (checking `size`, computing `strt_new`, `size_new`) with the updated
                // information.
                //
                // We are making two assumptions here:
                //
                // 1. We will not loop indefinitely.
                // 2. The time it takes us to loop is very small, such that there is a good chance
                //    we will only ever loop a very small number of times before settling on a
                //    decision.
                //
                // Assumption [1] is satisfied by the fact that if other readers or writers keep
                // updating the message queue, we will eventually reach the condition `size == 0` or
                // we will succeed in a write. We can assume this since the operations between loop
                // cycles are very simple (in the order of single instructions), therefore it is
                // reasonable to expect we will NOT keep missing the store, which satisfiesS
                // assumption [2].
                if let Err(bytes) = self.read_write.compare_exchange(
                    raw_bytes,
                    raw_bytes_new,
                    sync::atomic::Ordering::Release,
                    sync::atomic::Ordering::Acquire,
                ) {
                    debug!(bytes, "Inter-thread update on read region, trying again");
                    raw_bytes = bytes;
                    continue;
                };

                debug!(
                    writ_indx,
                    writ_size,
                    read_indx = read_indx_new,
                    read_size = read_size - 1,
                    "Updated read region"
                );
                break Some(unsafe { self.ring.add(read_indx as usize).as_mut() });
            }
        }
    }

    // TODO: we could make this async and wake it up as soon as an elem has been acknowledged so we
    // can try and grow this!
    #[cfg_attr(test, tracing::instrument(skip(self)))]
    fn write(&self, elem: T) -> Option<T> {
        let mut raw_bytes = self.read_write.load(sync::atomic::Ordering::Acquire);
        loop {
            let writ_indx = get_writ_indx(raw_bytes);
            let writ_size = get_writ_size(raw_bytes);
            let read_indx = get_read_indx(raw_bytes);
            let read_size = get_read_size(raw_bytes);
            debug!(writ_indx, writ_size, read_indx, read_size, "Trying to write to buffer");

            if writ_size == 0 {
                if let Ok(bytes) = self.grow_write(raw_bytes) {
                    raw_bytes = bytes;
                    continue;
                }

                debug!("Failed to grow write region");
                break Some(elem);
            } else {
                debug!(writ_indx, "Writing to buffer");

                // size - 1 is checked above and `grow` will increment size by 1 if it succeeds, so
                // whatever happens size > 0
                let writ_indx_new = fast_mod(writ_indx + 1, self.cap);
                let raw_bytes_new = get_raw_bytes(writ_indx_new, writ_size - 1, read_indx, read_size + 1);

                if let Err(bytes) = self.read_write.compare_exchange(
                    raw_bytes,
                    raw_bytes_new,
                    sync::atomic::Ordering::Release,
                    sync::atomic::Ordering::Acquire,
                ) {
                    debug!(bytes, "Inter-thread update on write region, trying again");
                    raw_bytes = bytes;
                    continue;
                };

                debug!(
                    writ_indx = writ_indx_new,
                    writ_size = writ_size - 1,
                    read_indx,
                    read_size = read_size + 1,
                    "Updated write region"
                );
                let cell = AckCell::new(elem);
                unsafe { self.ring.add(writ_indx as usize).write(cell) };
                break None;
            }
        }
    }

    #[cfg_attr(test, tracing::instrument(skip(self)))]
    fn grow_write(&self, raw_bytes: u64) -> Result<u64, &'static str> {
        // We are indexing the element right AFTER the end of the write region to see if we can
        // overwrite it (ie: it has been read and acknowledged)
        let writ_indx = get_writ_indx(raw_bytes);
        let writ_size = get_writ_size(raw_bytes);
        let read_indx = get_read_indx(raw_bytes);
        let read_size = get_read_size(raw_bytes);
        let stop = fast_mod(writ_indx + writ_size, self.cap);

        if stop == read_indx && read_size != 0 {
            debug!("Acknowledge region is empty");
            return Err("Failed to grow write region, acknowledge region is epmty");
        }

        debug!(writ_indx, writ_size, read_indx, read_size, stop, "Trying to grow write region");

        // There are a few invariants which guarantee that this will never index into uninitialized
        // memory:
        //
        // 1. We do not allow to create a empty write region.
        // 2. We only ever call grow if we have no more space left to write.
        // 3. A write region should initially cover the entirety of the array being written to.
        //
        // Inv. [1] and Inv. [3] guarantee that we are not writing into an empty array.
        // Consequentially, Inv. [2] guarantees that if there is no more space left to write, then
        // we must have filled up the array, hence we will wrap around to a value which was already
        // written to previously.
        //
        // Note that the `ack` state of that value/cell might have been updated by a `MqGuard` in
        // the meantime, which is what we are checking for: we cannot grow and mark a value as ready
        // to write to if it has not already been read and acknowledged.
        //
        // See the note in `MqGuard` to understand why we only read the `ack` state!
        let cell = unsafe { self.ring.add(stop as usize).as_ref() };
        let ack = cell.ack.load(sync::atomic::Ordering::Acquire);

        // Why would this fail? Consider the following buffer state:
        //
        //    ┌───┬───┬───┬───┬───┬───┬───┬───┬───┐
        // B: │!a │ a │ a │ r │ r │ w │ w │ w │ w │
        //    └───┴───┴───┴───┴───┴───┴───┴───┴───┘
        //      0   1   2   3   4   5   6   7   8
        //
        //    ┌───────────────────────────────────┐
        //    │ .B: buffer                        │
        //    │ .r: read region                   │
        //    │ .w: write region                  │
        //    │ .a: acknowledged                  │
        //    │ !a: NOT acknowledged              │
        //    └───────────────────────────────────┘
        //
        // Notice how the element at index 0 has been read but not acknowledge yet: this means we
        // cannot overwrite it as another thread might read it in the future! In contrary, the
        // elements at index 1 and 2 have been read and acknowledged, meaning they are safe to
        // overwrite. However, since element 0 precedes them, we cannot grow the write region to
        // encompass them.
        //
        // This is done to avoid fragmenting the buffer and keep read and write operations simple
        // and efficient.
        tracing::debug!(ack, self.cap, "Checking for cell acknowledgment");
        if ack && writ_size != self.cap {
            debug!("Write region is ready to grow");

            let raw_bytes_new = get_raw_bytes(writ_indx, writ_size + 1, read_indx, read_size);
            match self.read_write.compare_exchange(
                raw_bytes,
                raw_bytes_new,
                sync::atomic::Ordering::Release,
                sync::atomic::Ordering::Acquire,
            ) {
                Err(bytes) => {
                    debug!(bytes, "Failed to grow write region, cross-thread update");
                    Ok(bytes)
                }
                Ok(_) => {
                    debug!(writ_indx, writ_size = writ_size + 1, read_indx, read_size, "Managed to grow write region");
                    Ok(raw_bytes_new)
                }
            }
        } else {
            debug!("Cannot grow write region");

            Err("Failed to grow write region, next element has not been acknowledged yet")
        }
    }
}

impl<T: TBound> AckCell<T> {
    fn new(elem: T) -> Self {
        Self { elem: std::mem::MaybeUninit::new(elem), ack: sync::atomic::AtomicBool::new(false) }
    }
}

fn fast_mod(n: impl Into<u64>, pow_of_2: impl Into<u64>) -> u16 {
    (n.into() & (pow_of_2.into() - 1)) as u16
}

fn get_writ_indx(raw_bytes: u64) -> u16 {
    ((raw_bytes & WRIT_INDX_MASK) >> 48) as u16
}

fn get_writ_size(raw_bytes: u64) -> u16 {
    ((raw_bytes & WRIT_SIZE_MASK) >> 32) as u16
}

fn get_read_indx(raw_bytes: u64) -> u16 {
    ((raw_bytes & READ_INDX_MASK) >> 16) as u16
}

fn get_read_size(raw_bytes: u64) -> u16 {
    (raw_bytes & READ_SIZE_MASK) as u16
}

#[cfg_attr(test, tracing::instrument(skip_all))]
fn get_raw_bytes(writ_indx: u16, writ_size: u16, read_indx: u16, read_size: u16) -> u64 {
    ((writ_indx as u64) << 48) | ((writ_size as u64) << 32) | ((read_indx as u64) << 16) | read_size as u64
}

#[cfg(test)]
mod common {
    pub(crate) type LogConfig = tracing_subscriber::fmt::SubscriberBuilder<
        tracing_subscriber::fmt::format::DefaultFields,
        tracing_subscriber::fmt::format::Format<tracing_subscriber::fmt::format::Full, ()>,
        tracing_subscriber::EnvFilter,
    >;

    #[rstest::fixture]
    pub(crate) fn log_conf() -> LogConfig {
        let env = tracing_subscriber::EnvFilter::from_default_env();
        tracing_subscriber::fmt::Subscriber::builder().with_env_filter(env).without_time()
    }

    #[rstest::fixture]
    pub(crate) fn log_stdout(log_conf: LogConfig) {
        let _ = log_conf.with_test_writer().try_init();
    }

    #[cfg(feature = "loom")]
    #[rstest::fixture]
    pub(crate) fn model(#[default("loomtest")] path: &str, #[allow(unused)] log_stdout: ()) -> loom::model::Builder {
        let mut model = loom::model::Builder::new();
        model.checkpoint_interval = 1;
        model.checkpoint_file = Some(std::path::PathBuf::from(format!("{path}.json")));
        model.location = true;
        model
    }

    #[cfg(feature = "loom")]
    #[rstest::fixture]
    pub(crate) fn model_bounded(
        #[allow(unused)]
        #[default("loomtest")]
        path: &str,
        #[with(path)] mut model: loom::model::Builder,
    ) -> loom::model::Builder {
        model.preemption_bound = Some(3);
        model
    }

    #[derive(Clone)]
    pub(crate) struct DropCounter<T: Send + Clone> {
        elem: T,
        counter: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl<T: Send + Clone + std::fmt::Debug> std::fmt::Debug for DropCounter<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("DropCounter").field("elem", &self.elem).finish()
        }
    }

    impl<T: Send + Clone> Drop for DropCounter<T> {
        #[tracing::instrument(skip(self))]
        fn drop(&mut self) {
            tracing::trace!("Incrementing drop counter");
            self.counter.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        }
    }

    impl<T: Send + Clone> DropCounter<T> {
        pub(crate) fn new(elem: T, counter: std::sync::Arc<std::sync::atomic::AtomicUsize>) -> Self {
            Self { elem, counter }
        }

        pub(crate) fn get(self) -> T {
            self.elem.clone()
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use common::*;

    /// This function makes sure that when the message queue reads from its buffer, it returns a
    /// _reference_ to an [AckCell]. This is done so that updates to its [ack] state propagate to
    /// the underlying memory and don't just happen on a copy.
    ///
    /// > This bug was found through property testing (one could think this should have been easier
    /// > to spot :P).
    ///
    /// [ack]: AckCell::ack
    #[tokio::test]
    #[rstest::rstest]
    async fn ref_acknowledge(#[allow(unused)] log_stdout: ()) {
        let (sx, rx) = channel(1);

        assert_eq!(sx.send(0), None);
        assert_eq!(rx.recv().await.unwrap().read_acknowledge(), 0);
        assert_eq!(sx.send(1), None);
    }
}

/// [loom] is a deterministic concurrent permutation simulator. From the loom docs:
///
/// > _"At a high level, it runs tests many times, permuting the possible concurrent executions of
/// > each test according to what constitutes valid executions under the C11 memory model. It then
/// > uses state reduction techniques to avoid combinatorial explosion of the number of possible
/// > executions."_
///
/// # Running Loom
///
/// To run the tests below, first enter:
///
/// ```bash
/// LOOM_LOCATION=1 \
///     LOOM_CHECKPOINT_INTERVAL=1 \
///     LOOM_CHECKPOINT_FILE=test_name.json \
///     cargo test test_name --release --features loom
/// ```
///
/// This will begin by running loom with no logs, checking all possible permutations of
/// multithreaded operations for our program (actually this tests _most_ permutations, with
/// limitations in regard to [SeqCst] and [Relaxed] ordering, but since we do not use those loom
/// will be exploring the full concurrent permutations). If an invariant is violated, this will
/// cause the test to fail and the fail state will be saved under `LOOM_CHECKPOINT_FILE`.
///
/// > We do not enable logs for this first run as loom might simulate many thousand permutations
/// > before finding a single failing case, and this would polute `stdout`. Also, we run in
/// > `release` mode to make this process faster.
///
/// Once a failing case has been identified, resume the tests with:
///
/// ```bash
/// LOOM_LOG=debug \
///     LOOM_LOCATION=1 \
///     LOOM_CHECKPOINT_INTERVAL=1 \
///     LOOM_CHECKPOINT_FILE=test_name.json \
///     cargo test test_name --release --features loom
/// ```
///
/// This will resume testing with the previously failing case. We enable logging this time as only a
/// single iteration of the test will be run before the failure is caught.
///
/// > Note that if ever you update the code of a test, you will then need to delete
/// > `LOOM_CHECKPOINT_FILE` before running the tests again. Otherwise loom will complain about
// > having reached an unexpected execution path.
///
/// # Complexity explosion
///
/// Due to the way in which loom checks for concurrent access permutations, execution time will grow
/// exponentially with the size of the model. For this reason, it might be necessary to limit the
/// breath of checks done by loom.
///
/// ```bash
/// LOOM_MAX_PREEMPTIONS=3 \
///     LOOM_LOCATION=1 \
///     LOOM_CHECKPOINT_INTERVAL=1 \
///     LOOM_CHECKPOINT_FILE=test_name.json \
///     cargo test test_name --release --features loom
/// ```
///
/// From the loom docs:
///
/// > _"you may need to not run an exhaustive check, and instead tell loom to prune out
/// > interleavings that are unlikely to reveal additional bugs. You do this by providing loom with
/// > a thread pre-emption bound. If you set such a bound, loom will check all possible executions
/// > that include at most n thread pre-emptions (where one thread is forcibly stopped and another
/// > one runs in its place. In practice, setting the thread pre-emption bound to 2 or 3 is enough
/// > to catch most bugs while significantly reducing the number of possible executions."_
///
/// [SeqCst]: std::sync::atomic::Ordering::SeqCst
/// [Relaxed]: std::sync::atomic::Ordering::Relaxed
#[cfg(all(test, feature = "loom"))]
mod threadtesting {
    use super::*;
    use common::*;

    /// Single Producer Single Consumer, one message
    #[rstest::rstest]
    fn spsc_1(#[with("spsc_1")] model: loom::model::Builder) {
        model.check(|| {
            let (sx, rx) = channel(1);
            let elem = 42;

            let counter1 = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let counter2 = std::sync::Arc::clone(&counter1);

            assert_eq!(sx.queue.writ_indx(), 0);
            assert_eq!(sx.queue.writ_size(), 1); // closest power of 2
            assert_eq!(sx.queue.read_indx(), 0);
            assert_eq!(sx.queue.read_size(), 0);

            let handle = loom::thread::spawn(move || {
                tracing::info!(elem, "Sending element");
                assert_matches::assert_matches!(
                    sx.send(DropCounter::new(vec![elem], counter1)),
                    None,
                    "Failed to send value, message queue is {:#?}",
                    sx.queue
                );
            });

            loom::future::block_on(async move {
                tracing::info!(elem, "Waiting for element");
                let guard = rx.recv().await;
                assert_matches::assert_matches!(
                    guard,
                    Some(guard) => { assert_eq!(guard.read_acknowledge().get(), vec![elem]) },
                    "Failed to acquire acknowledge guard, message queue is {:#?}",
                    rx.queue
                );

                tracing::info!("Checking close correctness");
                let guard = rx.recv().await;
                assert!(guard.is_none(), "Guard acquired on supposedly empty message queue: {:?}", guard.unwrap());

                tracing::info!("Checking drop correctness");
                assert_eq!(counter2.load(std::sync::atomic::Ordering::Acquire), 1)
            });

            handle.join().unwrap();
        })
    }

    /// Single Produce Single Consumer, multiple messages
    #[rstest::rstest]
    fn spsc_2(#[with("spsc_2")] model_bounded: loom::model::Builder) {
        model_bounded.check(|| {
            let (sx, rx) = channel(3);

            let counter1 = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let counter2 = std::sync::Arc::clone(&counter1);

            assert_eq!(sx.queue.writ_indx(), 0);
            assert_eq!(sx.queue.writ_size(), 4); // closest power of 2
            assert_eq!(sx.queue.read_indx(), 0);
            assert_eq!(sx.queue.read_size(), 0);

            let handle = loom::thread::spawn(move || {
                for i in 0..2 {
                    tracing::info!(i, "Sending element");
                    assert_matches::assert_matches!(
                        sx.send(DropCounter::new(vec![i], std::sync::Arc::clone(&counter1))),
                        None,
                        "Failed to send {i}, message queue is {:#?}",
                        sx.queue
                    )
                }
            });

            loom::future::block_on(async move {
                for i in 0..2 {
                    tracing::info!(i, "Waiting for element");
                    let guard = rx.recv().await;
                    tracing::info!(i, "Received element");
                    assert_matches::assert_matches!(
                        guard,
                        Some(guard) => { assert_eq!(guard.read_acknowledge().get(), vec![i]) },
                        "Failed to acquire acknowledge guard {i}, message queue is {:#?}",
                        rx.queue
                    );
                }

                tracing::info!("Checking close correctness");
                let guard = rx.recv().await;
                assert!(guard.is_none(), "Guard acquired on supposedly empty message queue: {:?}", guard.unwrap());

                tracing::info!("Checking drop correctness");
                assert_eq!(counter2.load(std::sync::atomic::Ordering::Acquire), 2)
            });

            handle.join().unwrap();
        })
    }

    /// Single Producer Multiple Consumer, multiple messages
    #[rstest::rstest]
    fn spmc(#[with("spmc")] model_bounded: loom::model::Builder) {
        model_bounded.check(|| {
            let (sx, rx1) = channel(3);
            let rx2 = rx1.resubscribe();
            let rx3 = rx1.resubscribe();

            let witness1 = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::default()));
            let witness2 = std::sync::Arc::clone(&witness1);
            let witness3 = std::sync::Arc::clone(&witness1);

            let counter1 = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let counter2 = std::sync::Arc::clone(&counter1);

            assert_eq!(sx.queue.writ_indx(), 0);
            assert_eq!(sx.queue.writ_size(), 4); // closest power of 2
            assert_eq!(sx.queue.read_indx(), 0);
            assert_eq!(sx.queue.read_size(), 0);

            let handle1 = loom::thread::spawn(move || {
                for i in 0..2 {
                    assert_matches::assert_matches!(
                        sx.send(DropCounter::new(vec![i], std::sync::Arc::clone(&counter1))),
                        None,
                        "Failed to send {i}, message queue is {:#?}",
                        sx.queue
                    )
                }
            });

            let handle2 = loom::thread::spawn(move || {
                loom::future::block_on(async move {
                    tracing::info!("Waiting for element");
                    let guard = rx1.recv().await;
                    assert_matches::assert_matches!(
                        guard,
                        Some(guard) => { witness1.lock().await.push(guard.read_acknowledge().get()) },
                        "Failed to acquire acknowledge guard, message queue is {:#?}",
                        rx1.queue
                    );
                })
            });

            let handle3 = loom::thread::spawn(move || {
                loom::future::block_on(async move {
                    tracing::info!("Waiting for element");
                    let guard = rx2.recv().await;
                    assert_matches::assert_matches!(
                        guard,
                        Some(guard) => { witness2.lock().await.push(guard.read_acknowledge().get()) },
                        "Failed to acquire acknowledge guard, message queue is {:#?}",
                        rx2.queue
                    );
                })
            });

            handle1.join().unwrap();
            handle2.join().unwrap();
            handle3.join().unwrap();

            loom::future::block_on(async move {
                tracing::info!(?rx3, "Checking close correctness");
                let guard = rx3.recv().await;
                assert!(guard.is_none(), "Guard acquired on supposedly empty message queue: {:?}", guard.unwrap());

                let mut witness = witness3.lock().await;
                witness.sort();
                tracing::info!(witness = ?*witness, "Checking receive correctness");

                for (expected, actual) in witness.iter().enumerate() {
                    assert_eq!(*actual, vec![expected]);
                }

                tracing::info!("Checking drop correctness");
                assert_eq!(counter2.load(std::sync::atomic::Ordering::Acquire), 2);
            });
        })
    }

    /// Multiple Producer Multiple Consumer, multiple messages
    #[rstest::rstest]
    fn mpsc(#[with("mpsc")] model_bounded: loom::model::Builder) {
        model_bounded.check(|| {
            let (sx1, rx) = channel(3);
            let sx2 = sx1.resubscribe();

            let counter1 = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let counter2 = std::sync::Arc::clone(&counter1);
            let counter3 = std::sync::Arc::clone(&counter1);

            assert_eq!(sx1.queue.writ_indx(), 0);
            assert_eq!(sx1.queue.writ_size(), 4); // closest power of 2
            assert_eq!(sx1.queue.read_indx(), 0);
            assert_eq!(sx1.queue.read_size(), 0);

            let handle1 = loom::thread::spawn(move || {
                assert_matches::assert_matches!(
                    sx1.send(DropCounter::new(vec![42], counter1)),
                    None,
                    "Failed to send 42, message queue is {:#?}",
                    sx1.queue
                )
            });

            let handle2 = loom::thread::spawn(move || {
                assert_matches::assert_matches!(
                    sx2.send(DropCounter::new(vec![69], counter2)),
                    None,
                    "Failed to send 69, message queue is {:#?}",
                    sx2.queue
                )
            });

            loom::future::block_on(async move {
                let mut res = vec![];

                for i in 0..2 {
                    tracing::info!("Waiting for element");
                    let guard = rx.recv().await;
                    assert_matches::assert_matches!(
                        guard,
                        Some(guard) => { res.push(guard.read_acknowledge().get()) },
                        "Failed to acquire acknowledge guard {i}, message queue is {:#?}",
                        rx.queue
                    );
                }

                tracing::info!("Checking close correctness");
                let guard = rx.recv().await;
                assert!(guard.is_none(), "Guard acquired on supposedly empty message queue: {:?}", guard.unwrap());

                res.sort();
                tracing::info!(?res, "Checking receive correctness");

                assert_eq!(res.len(), 2);
                assert_eq!(res[0], vec![42]);
                assert_eq!(res[1], vec![69]);

                tracing::info!("Checking drop correctness");
                assert_eq!(counter3.load(std::sync::atomic::Ordering::Acquire), 2);
            });

            handle1.join().unwrap();
            handle2.join().unwrap();
        })
    }

    #[rstest::rstest]
    fn resend(#[with("resend")] model: loom::model::Builder) {
        model.check(|| {
            let (sx1, rx) = channel(2);

            let counter1 = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let counter2 = std::sync::Arc::clone(&counter1);

            assert_eq!(sx1.queue.writ_indx(), 0);
            assert_eq!(sx1.queue.writ_size(), 2); // closest power of 2
            assert_eq!(sx1.queue.read_indx(), 0);
            assert_eq!(sx1.queue.read_size(), 0);

            let handle = loom::thread::spawn(move || {
                // Notice that we only send ONE element!
                tracing::info!("Sending element");
                assert_matches::assert_matches!(
                    sx1.send(DropCounter::new(vec![69], counter1)),
                    None,
                    "Failed to send 69, message queue is {:#?}",
                    sx1.queue
                )
            });

            loom::future::block_on(async move {
                // We receive the element once but we do not acknowledge it, so it is added back to
                // the queue.
                tracing::info!("Waiting for element");
                let guard = rx.recv().await;
                assert_matches::assert_matches!(
                    guard,
                    Some(guard) => { assert_eq!(guard.read().get(), vec![69]); },
                    "Failed to acquire acknowledge guard, message queue is {:#?}",
                    rx.queue
                );

                // We receive the element a second time and we acknowledge it...
                tracing::info!("Waiting for element");
                let guard = rx.recv().await;
                assert_matches::assert_matches!(
                    guard,
                    Some(guard) => { assert_eq!(guard.read_acknowledge().get(), vec![69]); },
                    "Failed to acquire acknowledge guard, message queue is {:#?}",
                    rx.queue
                );

                // ...so that the queue is now empty.
                tracing::info!("Checking close correctness");
                let guard = rx.recv().await;
                assert!(guard.is_none(), "Guard acquired on supposedly empty message queue: {:?}", guard.unwrap());

                // We count 2 drops because calling `MqGuard.read()` creates a clone of the
                // underlying data (`MqGuard.read_acknowledge` does not).
                tracing::info!("Checking drop correctness");
                assert_eq!(counter2.load(std::sync::atomic::Ordering::Acquire), 2);
            });

            handle.join().unwrap();
        })
    }

    #[rstest::rstest]
    fn wrap_around(#[with("wrap_around")] model_bounded: loom::model::Builder) {
        model_bounded.check(|| {
            let (sx1, rx) = channel(2);
            let sx2 = sx1.resubscribe();

            let counter1 = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let counter2 = std::sync::Arc::clone(&counter1);
            let counter3 = std::sync::Arc::clone(&counter1);

            assert_eq!(sx1.queue.writ_indx(), 0);
            assert_eq!(sx1.queue.writ_size(), 2); // closest power of 2
            assert_eq!(sx1.queue.read_indx(), 0);
            assert_eq!(sx1.queue.read_size(), 0);

            let handle = loom::thread::spawn(move || {
                for i in 0..2 {
                    tracing::info!(i, "Sending element");
                    assert_matches::assert_matches!(
                        sx1.send(DropCounter::new(vec![i], std::sync::Arc::clone(&counter1))),
                        None,
                        "Failed to send {i}, message queue is {:#?}",
                        sx1.queue
                    )
                }
            });

            loom::future::block_on(async {
                for i in 0..2 {
                    tracing::info!("Waiting for element");
                    let guard = rx.recv().await;
                    tracing::info!("Received element");
                    assert_matches::assert_matches!(
                        guard,
                        Some(guard) => { assert_eq!(guard.read_acknowledge().get(), vec![i]); },
                        "Failed to acquire acknowledge guard {i}, message queue is {:#?}",
                        rx.queue
                    );
                }
            });
            handle.join().unwrap();

            let handle = loom::thread::spawn(move || {
                for i in 2..4 {
                    tracing::info!(i, "Sending element");
                    assert_matches::assert_matches!(
                        sx2.send(DropCounter::new(vec![i], std::sync::Arc::clone(&counter2))),
                        None,
                        "Failed to send {i}, message queue is {:#?}",
                        sx2.queue
                    )
                }
            });

            loom::future::block_on(async {
                for i in 2..4 {
                    tracing::info!("Waiting for element");
                    let guard = rx.recv().await;
                    tracing::info!("Received element");
                    assert_matches::assert_matches!(
                        guard,
                        Some(guard) => { assert_eq!(guard.read_acknowledge().get(), vec![i]); },
                        "Failed to acquire acknowledge guard {i}, message queue is {:#?}",
                        rx.queue
                    );
                }

                tracing::info!(?rx, "Checking close correctness");
                let guard = rx.recv().await;
                assert!(guard.is_none(), "Guard acquired on supposedly empty message queue: {:?}", guard.unwrap());

                tracing::info!("Checking drop correctness");
                assert_eq!(counter3.load(std::sync::atomic::Ordering::Acquire), 4)
            });
            handle.join().unwrap();
        })
    }

    #[rstest::rstest]
    fn close(#[with("close")] model_bounded: loom::model::Builder) {
        model_bounded.check(|| {
            let (sx1, rx) = channel(3);
            let sx2 = sx1.resubscribe();

            let counter1 = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let counter2 = std::sync::Arc::clone(&counter1);
            let counter3 = std::sync::Arc::clone(&counter1);

            assert_eq!(sx1.queue.writ_indx(), 0);
            assert_eq!(sx1.queue.writ_size(), 4); // closest power of 2
            assert_eq!(sx1.queue.read_indx(), 0);
            assert_eq!(sx1.queue.read_size(), 0);

            let handle1 = loom::thread::spawn(move || {
                for i in 0..2 {
                    tracing::info!(i, "Sending element");
                    sx1.send(DropCounter::new(vec![i], std::sync::Arc::clone(&counter1)));
                }
                sx1.close();
            });

            let handle2 = loom::thread::spawn(move || {
                for i in 2..4 {
                    tracing::info!(i, "Sending element");
                    sx2.send(DropCounter::new(vec![i], std::sync::Arc::clone(&counter2)));
                }
                sx2.close();
            });

            handle1.join().unwrap();
            handle2.join().unwrap();

            loom::future::block_on(async move {
                tracing::info!(?rx, "Checking close correctness");
                let guard = rx.recv().await;
                assert!(guard.is_none(), "Guard acquired on supposedly empty message queue: {:?}", guard.unwrap());

                tracing::info!("Checking drop correctness");
                assert_eq!(counter3.load(std::sync::atomic::Ordering::Acquire), 4)
            });
        })
    }

    #[rstest::rstest]
    fn zst(#[with("zst")] model_bounded: loom::model::Builder) {
        model_bounded.check(|| {
            let (sx, rx) = channel(3);

            assert_eq!(sx.queue.writ_indx(), 0);
            assert_eq!(sx.queue.writ_size(), 4); // closest power of 2
            assert_eq!(sx.queue.read_indx(), 0);
            assert_eq!(sx.queue.read_size(), 0);

            let handle = loom::thread::spawn(move || {
                for _ in 0..2 {
                    tracing::info!("Sending element");
                    assert_matches::assert_matches!(
                        sx.send(()),
                        None,
                        "Failed to send, message queue is {:#?}",
                        sx.queue
                    )
                }
            });

            loom::future::block_on(async move {
                for _ in 0..2 {
                    tracing::info!("Waiting for element");
                    let guard = rx.recv().await;
                    assert_matches::assert_matches!(
                        guard,
                        Some(guard) => { guard.acknowledge() },
                        "Failed to acquire acknowledge guard, message queue is {:#?}",
                        rx.queue
                    );
                }

                tracing::info!(?rx, "Checking close correctness");
                let guard = rx.recv().await;
                assert!(guard.is_none(), "Guard acquired on supposedly empty message queue: {:?}", guard.unwrap());
            });

            handle.join().unwrap();
        })
    }

    #[rstest::rstest]
    fn fail_send(#[with("fail_send")] model: loom::model::Builder) {
        model.check(|| {
            let (sx1, rx) = channel(2);
            let sx2 = sx1.resubscribe();

            let counter1 = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let counter2 = std::sync::Arc::clone(&counter1);
            let counter3 = std::sync::Arc::clone(&counter1);

            assert_eq!(sx1.queue.writ_indx(), 0);
            assert_eq!(sx1.queue.writ_size(), 2); // closest power of 2
            assert_eq!(sx1.queue.read_indx(), 0);
            assert_eq!(sx1.queue.read_size(), 0);

            loom::thread::spawn(move || {
                for i in 0..2 {
                    tracing::info!(i, "Sending element");
                    assert_matches::assert_matches!(
                        sx1.send(DropCounter::new(vec![i], std::sync::Arc::clone(&counter1))),
                        None,
                        "Failed to send {i}, message queue is {:#?}",
                        sx1.queue
                    )
                }
            })
            .join()
            .unwrap();

            loom::thread::spawn(move || {
                tracing::info!("Sending element");
                assert_matches::assert_matches!(
                    sx2.send(DropCounter::new(vec![2], counter2)),
                    Some(counter) => { assert_eq!(counter.get(), vec![2]) },
                    "Queue should be full! {:#?}",
                    sx2.queue
                )
            })
            .join()
            .unwrap();

            loom::future::block_on(async move {
                for i in 0..2 {
                    tracing::info!(i, "Waiting for element");
                    let guard = rx.recv().await;
                    assert_matches::assert_matches!(
                        guard,
                        Some(guard) => { assert_eq!(guard.read_acknowledge().get(), vec![i]); },
                        "Failed to acquire acknowledge guard, message queue is {:#?}",
                        rx.queue
                    );
                }

                tracing::info!(?rx, "Checking close correctness");
                let guard = rx.recv().await;
                assert!(guard.is_none(), "Guard acquired on supposedly empty message queue: {:?}", guard.unwrap());

                // We drop the sent value 3 times:
                // - in the first two (successful) sends
                // - in the last (failed) send
                tracing::info!("Checking drop correctness");
                assert_eq!(counter3.load(std::sync::atomic::Ordering::Acquire), 3)
            });
        })
    }

    // TEST: proptest
}

#[cfg(all(test, feature = "proptest"))]
mod proptesting {
    use super::*;
    use common::*;
    use proptest::prelude::*;
    use proptest_state_machine::*;

    prop_state_machine! {
        #![proptest_config(ProptestConfig {
            // Enable verbose mode to make the state machine test print the
            // transitions for each case.
            verbose: 1,
            // The number of tests which need to be valid for this to pass.
            cases: 256,
            // Max duration (in milliseconds) for each generated case.
            timeout: 1_000,
            ..Default::default()
        })]

        #[test]
        fn mq_proptest(sequential 1..512 => SystemUnderTest);
    }

    #[derive(thiserror::Error, Clone, Debug)]
    enum PropError {
        #[error("Failed to shrink region, no space left")]
        Shrink,
        #[error("Failed to grow region, no space left")]
        Grow,
    }

    struct SystemUnderTest {
        sxs: std::collections::VecDeque<MqSender<u32>>,
        rxs: std::collections::VecDeque<MqReceiver<u32>>,
    }

    #[derive(Clone, Debug)]
    struct Reference {
        // FIFO, push-front pop-back
        last_read: std::collections::VecDeque<u32>,
        last_writ: std::collections::VecDeque<u32>,
        status: Result<(), PropError>,
        read: ReferenceRegion,
        writ: ReferenceRegion,
        next: u32,
        count_sx: usize,
        count_rx: usize,
        cap: u16,
    }

    #[derive(Clone, Debug)]
    struct ReferenceRegion {
        start: u16,
        size: u16,
    }

    #[derive(Clone, Debug)]
    enum Transition {
        Send(u32),
        Recv,
        ResubscribeSender,
        ResubscribeReceiver,
        DropSender,
        DropReceiver,
    }

    impl ReferenceStateMachine for Reference {
        type State = Self;
        type Transition = Transition;

        fn init_state() -> BoxedStrategy<Self::State> {
            (1..512u16).prop_map(|cap| Self::new(cap)).boxed()
        }

        fn transitions(state: &Self::State) -> BoxedStrategy<Self::Transition> {
            prop_oneof![
                1 => Just(Transition::Send(state.next)),
                1 => Just(Transition::Recv),
            ]
            .boxed()
        }

        fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
            match transition {
                Transition::Send(elem) => {
                    if state.count_sx != 0 {
                        match state.clone().write(*elem) {
                            Ok(state_new) => state_new,
                            Err(e) => {
                                state.status = Err(e);
                                state
                            }
                        }
                    } else {
                        state
                    }
                }
                Transition::Recv => {
                    if state.count_rx != 0 {
                        match state.clone().read() {
                            Ok(state_new) => state_new,
                            Err(e) => {
                                state.status = Err(e);
                                state
                            }
                        }
                    } else {
                        state
                    }
                }
                Transition::ResubscribeSender => {
                    state.count_sx += 1;
                    state
                }
                Transition::ResubscribeReceiver => {
                    state.count_rx += 1;
                    state
                }
                Transition::DropSender => {
                    state.count_sx = state.count_sx.saturating_sub(1);
                    state
                }
                Transition::DropReceiver => {
                    state.count_rx = state.count_rx.saturating_sub(1);
                    state
                }
            }
        }
    }

    impl StateMachineTest for SystemUnderTest {
        type SystemUnderTest = Self;
        type Reference = Reference;

        fn init_test(ref_state: &<Self::Reference as ReferenceStateMachine>::State) -> Self::SystemUnderTest {
            let (sx, rx) = channel(ref_state.cap);
            Self { sxs: std::collections::VecDeque::from([sx]), rxs: std::collections::VecDeque::from([rx]) }
        }

        #[tracing::instrument(skip(state))]
        fn apply(
            mut state: Self::SystemUnderTest,
            ref_state: &<Self::Reference as ReferenceStateMachine>::State,
            transition: <Self::Reference as ReferenceStateMachine>::Transition,
        ) -> Self::SystemUnderTest {
            let file = std::fs::OpenOptions::new()
                .write(true)
                .append(true)
                .create(true)
                .open("./log")
                .expect("Failed to open file");
            let (appender, _guard) = tracing_appender::non_blocking(file);
            let logger = log_conf().with_writer(appender).finish();

            tracing::subscriber::with_default(logger, || {
                tracing::warn!(?transition, "Testing...");
                match transition {
                    Transition::Send(elem) => {
                        tracing::info!(elem, "Processing a SEND request");

                        if let Some(sx) = state.sxs.pop_back() {
                            tracing::debug!("Found a sender to process the request");
                            let res = sx.send(elem);

                            tracing::trace!(?ref_state, "Comparing to reference state");
                            match ref_state.status {
                                Ok(_) => {
                                    tracing::debug!("Write is legal");
                                    assert_eq!(res, None);
                                    tracing::info!("Write successful");
                                }
                                Err(_) => {
                                    tracing::debug!("Write is illegal");
                                    assert_matches::assert_matches!(res, Some(e) => { assert_eq!(e, elem) });
                                    tracing::info!("Write failed successfully ;)");
                                }
                            }

                            tracing::debug!(?ref_state.writ, ?ref_state.read, "Checking read/write regions");

                            assert_eq!(sx.queue.writ_indx(), ref_state.writ.start);
                            assert_eq!(sx.queue.writ_size(), ref_state.writ.size);
                            assert_eq!(sx.queue.read_indx(), ref_state.read.start);
                            assert_eq!(sx.queue.read_size(), ref_state.read.size);

                            tracing::trace!("Restoring sender state");
                            state.sxs.push_front(sx);
                        } else {
                            tracing::debug!("No sender available");
                        }
                    }
                    Transition::Recv => {
                        tracing::info!("Processing a RECV request");

                        if let Some(rx) = state.rxs.pop_back() {
                            tracing::debug!("Found a receiver to process the request");
                            {
                                let res = tokio_test::task::spawn(rx.recv()).poll();

                                tracing::trace!(?ref_state, "Comparing to reference state");
                                match ref_state.status {
                                    Ok(_) => {
                                        tracing::debug!("Read is legal");
                                        let elem = ref_state.last_read.front().copied().unwrap();
                                        assert_matches::assert_matches!(res, std::task::Poll::Ready(Some(e)) => {
                                            assert_eq!(e.read_acknowledge(), elem)
                                        });
                                        tracing::info!("Read successful");
                                    }
                                    Err(_) => {
                                        tracing::debug!("Read is illegal");
                                        assert_matches::assert_matches!(res, std::task::Poll::Pending);
                                        tracing::info!("Read failed successfully ;)");
                                    }
                                }
                            }

                            tracing::debug!(?ref_state.writ, ?ref_state.read, "Checking read/write regions");

                            assert_eq!(rx.queue.writ_indx(), ref_state.writ.start);
                            assert_eq!(rx.queue.writ_size(), ref_state.writ.size);
                            assert_eq!(rx.queue.read_indx(), ref_state.read.start);
                            assert_eq!(rx.queue.read_size(), ref_state.read.size);

                            tracing::trace!("Restoring sender state");
                            state.rxs.push_front(rx);
                        } else {
                            tracing::debug!("No receiver available");
                        }
                    }
                    Transition::ResubscribeSender => todo!(),
                    Transition::ResubscribeReceiver => todo!(),
                    Transition::DropSender => todo!(),
                    Transition::DropReceiver => todo!(),
                }
            });
            state
        }
    }

    impl Reference {
        fn new(cap_base: u16) -> Self {
            let cap = cap_base.checked_next_power_of_two().expect("Failed to get next power of 2");
            Self {
                last_read: Default::default(),
                last_writ: Default::default(),
                status: Ok(()),
                read: ReferenceRegion::new_read(),
                writ: ReferenceRegion::new_write(cap),
                next: 0,
                count_sx: 1,
                count_rx: 1,
                cap,
            }
        }

        fn read(mut self) -> Result<Self, PropError> {
            self.read.shrink(self.cap).map(|_| {
                self.last_read.push_front(self.last_writ.pop_back().expect("Invalid state"));
                self.status = Ok(());
                self
            })
        }

        fn write(mut self, elem: u32) -> Result<Self, PropError> {
            self.writ
                .shrink(self.cap)
                .or_else(|_| self.writ.grow(self.cap).and_then(|_| self.writ.shrink(self.cap)))
                .and_then(|_| self.read.grow(self.cap))
                .map(|_| {
                    self.last_writ.push_front(elem);
                    self.next += 1;
                    self.status = Ok(());
                    self
                })
        }
    }

    impl ReferenceRegion {
        fn new_read() -> Self {
            Self { start: 0, size: 0 }
        }

        fn new_write(cap: u16) -> Self {
            Self { start: 0, size: cap }
        }

        fn len(&self) -> u16 {
            self.size
        }

        fn is_empty(&self) -> bool {
            self.len() == 0
        }

        fn is_full(&self, cap: u16) -> bool {
            self.size == cap
        }

        fn can_shrink(&self) -> Result<(), PropError> {
            if self.is_empty() { Err(PropError::Shrink) } else { Ok(()) }
        }

        fn shrink(&mut self, cap: u16) -> Result<(), PropError> {
            self.can_shrink().map(|_| {
                self.start = (self.start + 1) % cap;
                self.size -= 1;
            })
        }

        fn can_grow(&self, cap: u16) -> Result<(), PropError> {
            if self.is_full(cap) { Err(PropError::Grow) } else { Ok(()) }
        }

        fn grow(&mut self, cap: u16) -> Result<(), PropError> {
            self.can_grow(cap).map(|_| self.size += 1)
        }
    }
}
