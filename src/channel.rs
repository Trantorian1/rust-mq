use crate::macros::*;
use crate::sync::*;

const WRIT_INDX_MASK: u64 = 0xffff000000000000;
const WRIT_SIZE_MASK: u64 = 0x0000ffff00000000;
const READ_INDX_MASK: u64 = 0x00000000ffff0000;
const READ_SIZE_MASK: u64 = 0x000000000000ffff;

#[cfg(test)]
pub trait TBound: Send + Clone + std::fmt::Debug {}
#[cfg(test)]
impl<T: Send + Clone + std::fmt::Debug> TBound for T {}

#[cfg(not(test))]
pub trait TBound: Send + Clone {}
#[cfg(not(test))]
impl<T: Send + Clone> TBound for T {}

/// A lock free [`MessageQueue`] sender. Read and writes happen concurrently, with reads having to
/// be [acknowledged] for a value to be fully removed from the queue.
///
/// [acknowledged]: MqGuard::acknowledge
pub struct MqSender<T: TBound> {
    queue: sync::Arc<MessageQueue<T>>,
    close: sync::Arc<sync::atomic::AtomicBool>,
    waker: sync::Arc<Waker>,
}

/// A lock free [`MessageQueue`] receiver. Read and writes happen concurrently, with reads having to
/// be [acknowledged] for a value to be fully removed from the queue.
///
/// [acknowledged]: MqGuard::acknowledge
pub struct MqReceiver<T: TBound> {
    queue: sync::Arc<MessageQueue<T>>,
    waker: sync::Arc<Waker>,
}

/// A guard over a value which has been read from a [`MessageQueue`]. Needs to be [acknowledged]
/// for the value to be fully removed from the queue.
///
/// [acknowledged]: Self::acknowledge
#[must_use]
pub struct MqGuard<'a, T: TBound> {
    ack: bool,
    cell: &'a mut AckCell<T>,
    queue: sync::Arc<MessageQueue<T>>,
    waker: sync::Arc<Waker>,
    _phantom: std::marker::PhantomData<&'a ()>,
}

/// A lock-free atomic message queue.
///
/// # Implementation details
///
/// This queue is backed by a ring buffer which is bounded into three atomic regions:
///
/// - The write region represents a contiguous part of the ring which is reserved for future writes:
///   initially, this is the whole of the queue.
///
/// - The read region represents a contiguous part of the queue which is reserved for reads:
///   initially this is empty but grows as new values are written.
///
/// - The acknowledge region represents a contiguous part of the queue where values have been read
///   but not yet acknowledged. Such values can no longer be read but cannot be overwritten yet.
///
/// ## Inner ring
///
/// The ring buffer backing this message queue might look something like this:
///
/// ```text
///
///    ┌───┬───┬───┬───┬───┬───┬───┬───┬───┐
/// B: │ a │ a │!a │ r │ r │ w │ w │ w │ w │
///    └───┴───┴───┴───┴───┴───┴───┴───┴───┘
///      0   1   2   3   4   5   6   7   8
///
///    ┌───────────────────────────────────┐
///    │ .B: buffer                        │
///    │ .r: read region                   │
///    │ .w: write region                  │
///    │ .a: acknowledged                  │
///    │ !a: NOT acknowledged              │
///    └───────────────────────────────────┘
/// ```
///
/// The write region will grow as needed during attempted writes. In the above example, cells 0 and
/// 1 can be overwritten as they have been read _and_ acknowledged, but cell 2 cannot as it has not
/// been acknowledged yet! Cells 3 till 8 cannot be overwritten as they are part of the current read
/// or write regions.
///
/// ## Atomicity
///
/// Because of the way the underlying ring buffer is structured, each region has to be updated
/// atomically as a whole. This means that it should not be possible for a thread to see an update
/// to the read region that does not coincide with an update to the write region (as this would mean
/// that we are indexing into potentially uninitialized memory -yikes!). To avoid this, information
/// about the read and write regions are encoded as [`u16`] values inside a single [`AtomicU64`] as
/// follows:
///
/// ```text
/// 0x aaaa bbbb cccc dddd
///    └┬─┘ └┬─┘ └┬─┘ └┬─┘
///     │    │    │    │
///     │    │    │   read size
///     │    │    │
///     │    │   read index
///     │    │
///     │   write size
///     │
///    write index
/// ```
///
/// The acknowledge region is a bit more tricky as it can get fragmented since elements can be
/// acknowledged individually and irrespective of the order in which they are received. This extra
/// information is stored as an [`AtomicBool`] inside of each cell. At any point, the ring buffer
/// might then look something like this:
///
/// ```text
///    ┌───┬───┬───┬───┬───┬───┬───┬───┬───┐
/// B: │!a │ a │!a │ r │ r │ w │ w │ w │ w │
///    └───┴───┴───┴───┴───┴───┴───┴───┴───┘
/// ```
///
/// Where several non-consecutive cells have not yet been acknowledged. This is checked when we try
/// and grow the write region to make sure we only overwrite cell which have been acknowledged.
///
/// ## Resending
///
/// To make sure values can always be re-sent into the queue if they are not acknowledged, we
/// allocate for twice the required capacity when calling [`channel`]. Half of this space is
/// reserved for re-sends to ensure that if ever a value is not acknowledged, there will _always_ be
/// enough space left in the queue to resend it.
///
/// [`AtomicU64`]: std::sync::atomic::AtomicU64
/// [`AtomicBool`]: std::sync::atomic::AtomicBool
pub struct MessageQueue<T: TBound> {
    ring: std::ptr::NonNull<AckCell<T>>,
    senders: sync::atomic::AtomicUsize,
    read_write: sync::atomic::AtomicU64,
    cap: u16,
    size: u16,
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

unsafe impl<T: TBound> Send for MessageQueue<T> {}
unsafe impl<T: TBound> Sync for MessageQueue<T> {}

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
        let raw_bytes = self.read_write.load(sync::atomic::Ordering::Acquire);
        let writ_indx = get_writ_indx(raw_bytes);
        let writ_size = get_writ_size(raw_bytes);
        let read_indx = get_read_indx(raw_bytes);
        let read_size = get_read_size(raw_bytes);
        f.debug_struct("MessageQueue")
            .field("senders", &senders)
            .field("writ_indx", &writ_indx)
            .field("writ_size", &writ_size)
            .field("read_indx", &read_indx)
            .field("read_size", &read_size)
            .field("cap", &self.cap)
            .field("size", &self.size)
            .finish()
    }
}

impl<T: TBound> Drop for MqSender<T> {
    fn drop(&mut self) {
        if !self.close.load(sync::atomic::Ordering::Acquire) {
            self.queue.sender_unregister();
            self.waker.notify_waiters();
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
            // It is safe not to clone here since:
            //
            // 1. Simply reading the element returns a clone to the user,
            // 2. Read-acknowledging the element does not create a clone but then this would not
            //    execute.
            let elem = self.elem_take();
            self.cell.ack.store(true, sync::atomic::Ordering::Release);

            // We can be sure this send will succeed since we reserve half the capacity of the
            // message queue for resends.
            let res = self.queue.write(elem, 0);
            self.waker.notify_one();
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

        for i in (read_indx..stop).map(|i| fast_mod(i, self.size)) {
            unsafe { self.ring.add(i as usize).read() };
        }

        let layout = alloc::Layout::array::<AckCell<T>>(self.size as usize).unwrap();
        unsafe { alloc::dealloc(self.ring.as_ptr() as *mut u8, layout) }
    }
}

/// Initializes a new [`MqSender`] and [`MqReceiver`] which both index over the same bounded
/// [`MessageQueue`]. No more than `cap` messages can be held at once in the queue.
#[cfg_attr(test, tracing::instrument)]
pub fn channel<T: TBound>(cap: u16) -> (MqSender<T>, MqReceiver<T>) {
    debug!("Creating new channel");

    let queue_s = sync::Arc::new(MessageQueue::new(cap));
    let queue_r = sync::Arc::clone(&queue_s);

    let wake_s = sync::Arc::new(Waker::new());
    let wake_r = sync::Arc::clone(&wake_s);
    wake_s.resubscribe();

    let sx = MqSender::new(queue_s, wake_s);
    let rx = MqReceiver::new(queue_r, wake_r);

    (sx, rx)
}

impl<T: TBound> MqSender<T> {
    fn new(queue: sync::Arc<MessageQueue<T>>, wake: sync::Arc<Waker>) -> Self {
        queue.sender_register();
        Self { queue, close: sync::Arc::new(sync::atomic::AtomicBool::new(false)), waker: wake }
    }

    /// Creates a new instance of [`MqSender`] which indexes over the same [`MessageQueue`].
    pub fn resubscribe(&self) -> Self {
        self.queue.sender_register();
        Self {
            queue: sync::Arc::clone(&self.queue),
            close: sync::Arc::clone(&self.close),
            waker: sync::Arc::clone(&self.waker),
        }
    }

    /// Sends a value to the underlying [`MessageQueue`] to be received by all its [`MqReceiver`]s
    #[cfg_attr(test, tracing::instrument(skip(self)))]
    pub fn send(&self, elem: T) -> Option<T> {
        debug!("Trying to send value");
        match self.queue.write(elem, self.queue.cap) {
            // Failed to write to the queue. This can happen if the next element right after the
            // write region has been read but not acknowledge yet.
            Some(elem) => {
                error!("Failed to send value");
                Some(elem)
            }
            None => {
                debug!("Value sent successfully");
                self.waker.notify_one();
                None
            }
        }
    }

    // TODO: make is to this returns all the messages which have not been read yet!
    /// Closes the [`MessageQueue`], waking up any [`MqReceiver`] in the process.
    #[cfg_attr(test, tracing::instrument(skip(self)))]
    pub fn close(self) {
        warn!("Closing channel");

        self.queue.sender_unregister_all();
        self.close.store(true, sync::atomic::Ordering::Release);

        let mut raw_bytes = self.queue.read_write.load(sync::atomic::Ordering::Acquire);
        let mut read_indx = get_read_indx(raw_bytes);
        let mut read_size = get_read_size(raw_bytes);

        //                               THIS IS VERY IMPORTANT!
        //
        // It is still possible for a value to be sent while we are closing the channel! This can
        // happen if a sender has already begun to send that value before we could set the write
        // region to 0. IN THAT CASE (and because of how the write method works to guarantee
        // atomicity), it is possible that the read region will be updated AFTER the channel has
        // been closed. While no new values can be sent, this means that some values could still be
        // written to the queue while we are closing it! This need to be taken into account.
        //
        // # An incorrect approach
        //
        // Consider what would happen if we just set the read/write region to 0:
        //
        //    ┌───┬───┬───┬───┬───┬───┬───┐
        // B: │ r │ r │ r │ w │ w │ w │ w │
        //    └───┴───┴───┴───┴───┴───┴───┘
        //      0   1   2   3   4   5   6
        //
        //    ┌───────────────────────────────────┐
        //    │ .B: buffer                        │
        //    │ .r: read region                   │
        //    │ .w: write region                  │
        //    └───────────────────────────────────┘
        //
        // 1. Another thread starts writing to the queue and reads the state of the current
        //    read/write region. (Thread B)
        //
        // - The read region is currently { indx: 0, size: 3 }
        // - The write region is currently { indx: 3, size: 4 }
        //
        // 2. Another thread updates the write region as part of the `write` method. (Thread B)
        //
        // - The read region is currently { indx: 0, size: 3 }
        // - The write region is currently { indx: 4, size: 3 }
        //
        // 3. We set the read and write region to 0. (Thread A)
        //
        // - The read region is currently { indx: 0, size: 0 }
        // - The write region is currently { indx: 0, size: 0 }
        //
        // 4. Another thread increments the read region as part of the `write` method. (Thread B)
        //
        // - The read region is currently { indx: 0, size: 1 }
        // - The write region is currently { indx: 0, size: 0 }
        //
        // 5. We free elements from index 0 to 2, the state of the queue is now:
        //
        //    ┌───┬───┬───┬───┬───┬───┬───┐
        // B: │r/f│ f │ f │ e │ x │ x │ x │
        //    └───┴───┴───┴───┴───┴───┴───┘
        //      0   1   2   3   4   5   6
        //
        //    ┌───────────────────────────────────┐
        //    │ .B: buffer                        │
        //    │ .r: read region                   │
        //    │ .f: element freed by thread A     │
        //    │ .e: element added by thread B     │
        //    │ .x: empty region                  │
        //    └───────────────────────────────────┘
        //
        // This is an invalid state! When this function is called again or the queue is dropped,
        // the element at index 0 will be dropped again which will result in a double free!
        //
        // ---
        //
        // # A correct approach
        //
        // Now consider instead what we are ACTUALLY doing:
        //
        //    ┌───┬───┬───┬───┬───┬───┬───┐
        // B: │ r │ r │ r │ w │ w │ w │ w │
        //    └───┴───┴───┴───┴───┴───┴───┘
        //      0   1   2   3   4   5   6
        //
        //    ┌───────────────────────────────────┐
        //    │ .B: buffer                        │
        //    │ .r: read region                   │
        //    │ .w: write region                  │
        //    └───────────────────────────────────┘
        //
        // 1. Another thread starts writing to the queue and reads the state of the current
        //    read/write region. (Thread B)
        //
        // - The read region is currently { indx: 0, size: 3 }
        // - The write region is currently { indx: 3, size: 4 }
        //
        // 2. Another thread updates the write region as part of the `write` method. (Thread B)
        //
        // - The read region is currently { indx: 0, size: 3 }
        // - The write region is currently { indx: 4, size: 3 }
        //
        // 3. We update the read/write region. Notice the change in `indx`! (Thread A)
        //
        // - The read region is currently { indx: 3, size: 0 }
        // - The write region is currently { indx: 0, size: 0 }
        //
        // 4. Another thread updates the read region as part of the `write` method. (Thread B)
        //
        // - The read region is currently { indx: 3, size: 1 }
        // - The write region is currently { indx: 0, size: 0 }
        //
        // 5. We free elements from index 0 to 2, the state of the queue is now:
        //
        //    ┌───┬───┬───┬───┬───┬───┬───┐
        // B: │ f │ f │ f │r/e│ x │ x │ x │
        //    └───┴───┴───┴───┴───┴───┴───┘
        //      0   1   2   3   4   5   6
        //
        //    ┌───────────────────────────────────┐
        //    │ .B: buffer                        │
        //    │ .r: read region                   │
        //    │ .f: element freed by thread A     │
        //    │ .e: element added by thread B     │
        //    │ .x: empty region                  │
        //    └───────────────────────────────────┘
        //
        // This is a valid state! When this functions is called again or the queue is dropped, the
        // element at index 3 will be freed appropriately.
        loop {
            let read_indx_new = fast_mod(read_indx + read_size, self.queue.size);
            let raw_bytes_new = get_raw_bytes(0, 0, read_indx_new, 0);

            if let Err(bytes) = self.queue.read_write.compare_exchange(
                raw_bytes,
                raw_bytes_new,
                sync::atomic::Ordering::Release,
                sync::atomic::Ordering::Acquire,
            ) {
                read_indx = get_read_indx(bytes);
                read_size = get_read_size(bytes);
                raw_bytes = bytes;
                continue;
            } else {
                break;
            }
        }

        // Cast to u64 to avoid overflow
        let read_indx = read_indx as u64;
        let read_size = read_size as u64;
        let stop = read_indx + read_size;

        debug!(read_indx, read_size, stop, "Dropping elements");

        for i in (read_indx..stop).map(|i| fast_mod(i, self.queue.size)) {
            unsafe { self.queue.ring.add(i as usize).read().elem.assume_init() };
        }

        self.waker.notify_waiters();
    }
}

impl<T: TBound> MqReceiver<T> {
    fn new(queue: sync::Arc<MessageQueue<T>>, wake: sync::Arc<Waker>) -> Self {
        Self { queue, waker: wake }
    }

    /// Creates a new instance of [`MqReceiver`] which indexes over the same [`MessageQueue`].
    pub fn resubscribe(&self) -> Self {
        self.waker.resubscribe();
        Self { queue: sync::Arc::clone(&self.queue), waker: sync::Arc::clone(&self.waker) }
    }

    /// Receives the next value sent to the underlying [`MessageQueue`] or waits for one to be
    /// available. Values have to be [acknowledged] or else they will be sent back into the queue.
    ///
    /// [acknowledged]: MqGuard::acknowledge
    #[cfg_attr(test, tracing::instrument(skip(self)))]
    pub async fn recv(&self) -> Option<MqGuard<T>> {
        debug!("Trying to receive value");
        loop {
            match self.queue.read() {
                Some(cell_ptr) => {
                    // TODO: could replace this with a reference?A
                    let guard = MqGuard::new(cell_ptr, sync::Arc::clone(&self.queue), sync::Arc::clone(&self.waker));
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
                        self.waker.notified().await;
                        debug!("A send was detected");
                    }
                }
            }
        }
    }
}

impl<'a, T: TBound> MqGuard<'a, T> {
    fn new(cell: &'a mut AckCell<T>, queue: sync::Arc<MessageQueue<T>>, waker: sync::Arc<Waker>) -> Self {
        MqGuard { ack: false, queue, waker, cell, _phantom: std::marker::PhantomData }
    }

    /// Creates a [`Clone`] of the guarded value but does not [acknowledge] it.
    ///
    /// [acknowledge]: Self::acknowledge
    pub fn read(&self) -> T {
        unsafe { self.cell.elem.assume_init_ref().clone() }
    }

    /// Retrieves the guarded value and [acknowledges] it. This method does not result in a
    /// [`Clone`].
    ///
    /// [acknowledges]: Self::acknowledge
    #[cfg_attr(test, tracing::instrument(skip(self)))]
    pub fn read_acknowledge(mut self) -> T {
        let elem = self.elem_take();
        debug!(?elem, "Acknowledging");
        self.acknowledge();
        elem
    }

    /// Acknowledges a value. Values which are not explicitly acknowledged will be sent back to
    /// their [`MessageQueue`] to be processed again.
    pub fn acknowledge(mut self) {
        self.ack = true;
        self.cell.ack.store(true, sync::atomic::Ordering::Release);
    }

    fn elem_take(&mut self) -> T {
        let mut elem = std::mem::MaybeUninit::uninit();
        std::mem::swap(&mut self.cell.elem, &mut elem);
        unsafe { elem.assume_init() }
    }
}

impl<T: TBound> MessageQueue<T> {
    #[cfg_attr(test, tracing::instrument)]
    fn new(cap: u16) -> Self {
        assert!(cap > 0, "Tried to create a message queue with a capacity < 1");
        assert!(cap <= u16::MAX >> 1);

        let cap = cap.checked_next_power_of_two().expect("failed to retrieve the next power of 2 for cap");
        let size = cap << 1;
        debug!(size, "Determining array layout");
        let layout = alloc::Layout::array::<AckCell<T>>(size as usize).unwrap();

        // From the `Layout` docs: "All layouts have an associated size and a power-of-two alignment.
        // The size, when rounded up to the nearest multiple of align, does not overflow isize (i.e.
        // the rounded value will always be less than or equal to isize::MAX)."
        //
        // I could not find anything in the source code of this method that checks this, is this
        // really necessary?
        assert!(layout.size() <= isize::MAX as usize);

        debug!(?layout, "Allocating layout");
        let ptr = unsafe { alloc::alloc(layout) };
        let ring = match std::ptr::NonNull::new(ptr as *mut AckCell<T>) {
            Some(p) => p,
            None => std::alloc::handle_alloc_error(layout),
        };

        let senders = sync::atomic::AtomicUsize::new(0);
        let read_write = sync::atomic::AtomicU64::new(get_raw_bytes(0, size, 0, 0));

        Self { ring, senders, read_write, cap, size }
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

    fn sender_count(&self) -> usize {
        self.senders.load(sync::atomic::Ordering::Acquire)
    }

    #[cfg_attr(test, tracing::instrument(skip(self)))]
    fn sender_available(&self) -> bool {
        let senders = self.sender_count();
        debug!(senders, "Senders available");
        senders > 0
    }

    // TODO: refactor this into a single method
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
        let mut raw_bytes = self.read_write.load(sync::atomic::Ordering::Acquire);
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

                let read_indx_new = fast_mod(read_indx + 1, self.size);
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
                // it to update `read_write` only if `read_write` has not been changed by another
                // thread in the meantime. If this is not the case, we re-try the whole operations
                // (checking `size`, computing `strt_new` and `size_new`) with this updated
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
                // cycles are very simple (in the order of single instructions), which satisfies
                // assumption [2], therefore it is reasonable to expect we will NOT keep missing the
                // store.
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

    #[cfg_attr(test, tracing::instrument(skip(self)))]
    fn write(&self, elem: T, cap: u16) -> Option<T> {
        let mut raw_bytes = self.read_write.load(sync::atomic::Ordering::Acquire);
        let mut writ_indx = get_writ_indx(raw_bytes);
        let mut writ_size = get_writ_size(raw_bytes);
        let mut read_indx = get_read_indx(raw_bytes);
        let mut read_size = get_read_size(raw_bytes);

        debug!(writ_indx, writ_size, read_indx, read_size, "Trying to write to buffer");

        // Writing to the buffer takes place in two steps to guarantee atomicity:
        //
        // # Step 1
        //
        // We update the write region, moving on to the next element and decreasing its size by 1.
        // This acts as a locking mechanism where the sender races against other threads to reserve
        // a writing index. If the sender looses the race (ie: `read_write` is updated by another
        // thread) then it tries again, exiting if the write region now has a size of 0.
        //
        // # Step 2
        //
        // We write to the writing index which was reserved in step 1. Once that is done, we increase
        // the read region by 1, signaling that a new element is ready to be read. Crucially, the
        // update to the read region is only done AFTER we have written the element, so there is no
        // possibility for a data race.
        //
        // # Caveats
        //
        // One important consideration to keep in mind is that the read region and the write region
        // are stored in the SAME atomic integer. This, and the use of `Release` as store ordering
        // guarantees that step 2 will ALWAYS be seen AFTER step 1 across threads. Take a moment to
        // think about this: if we were using two separate atomic integers for the read and write
        // regions, then the updates to each region would be atomic in of themselves, but there
        // would be no way to make sure that other threads wouldn't read the state of step 2 BEFORE
        // step 1.
        //
        // Consider the following example:
        //
        // - We update the write region (Thread A).
        // - Another thread reads the state of the read region (Thread B).
        // - We update the read region (Thread A). Thread B does not see this!
        // - Another thread reads the state of the write region (Thread B).
        //
        // Notice that in this case, thread B is seeing a partial update to the global state of the
        // read/write region! This is not possible in our case since we are operating on the same
        // atomic integer and so updates can be globally ordered.
        loop {
            // Step 1
            if writ_size <= cap {
                if let Ok(bytes) = self.grow_write(raw_bytes) {
                    writ_indx = get_writ_indx(bytes);
                    writ_size = get_writ_size(bytes);
                    read_indx = get_read_indx(bytes);
                    read_size = get_read_size(bytes);
                    raw_bytes = bytes;
                } else {
                    debug!("Failed to grow write region");
                    return Some(elem);
                }
            }

            // size - 1 is checked above and `grow` will increment size by 1 if it succeeds, so
            // whatever happens size > 0
            let writ_indx_new = fast_mod(writ_indx + 1, self.size);
            let raw_bytes_new = get_raw_bytes(writ_indx_new, writ_size - 1, read_indx, read_size);

            debug!(writ_indx = writ_indx_new, "Trying to update write region");

            if let Err(bytes) = self.read_write.compare_exchange(
                raw_bytes,
                raw_bytes_new,
                sync::atomic::Ordering::Release,
                sync::atomic::Ordering::Acquire,
            ) {
                writ_indx = get_writ_indx(bytes);
                writ_size = get_writ_size(bytes);
                read_indx = get_read_indx(bytes);
                read_size = get_read_size(bytes);
                raw_bytes = bytes;
                continue;
            }

            debug!(writ_indx = writ_indx_new, writ_size = writ_size - 1, "Updated write region");

            break;
        }

        // Step 2
        debug!(writ_indx, "Writing to buffer");

        let cell = AckCell::new(elem);
        unsafe { self.ring.add(writ_indx as usize).write(cell) };
        self.read_write.fetch_add(1, sync::atomic::Ordering::AcqRel); // Increments read size

        debug!(read_indx, read_size = read_size + 1, "Updated read region");

        None
    }

    #[cfg_attr(test, tracing::instrument(skip(self)))]
    fn grow_write(&self, raw_bytes: u64) -> Result<u64, &'static str> {
        // We are indexing the element right AFTER the end of the write region to see if we can
        // overwrite it (ie: it has been read and acknowledged)
        let writ_indx = get_writ_indx(raw_bytes);
        let writ_size = get_writ_size(raw_bytes);
        let read_indx = get_read_indx(raw_bytes);
        let read_size = get_read_size(raw_bytes);
        let stop = fast_mod(writ_indx + writ_size, self.size);

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
        // 3. A write region should initially cover the entirety of the queue.
        //
        // Inv. [1] and Inv. [3] guarantee that we are not writing into an empty array.
        // Consequentially, Inv. [2] guarantees that if there is no more space left to write, then
        // we must have filled up the array, hence we will wrap around to a value which was already
        // written to previously.
        //
        // Note that the `ack` state of that value/cell might have been updated by a `MqGuard` in
        // the meantime, which is what we are checking for: we cannot overwrite a value until it
        // has been acknowledged.
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
        // encompass them. This is because we want to maintain the invariant that the read/write
        // regions are _contiguous_.
        //
        // This is done to avoid fragmenting the buffer and keep read and write operations simple
        // and efficient.
        tracing::debug!(ack, self.size, "Checking for cell acknowledgment");
        if ack && writ_size != self.size {
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
pub mod common {
    use super::*;

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
        model.max_duration = Some(std::time::Duration::from_secs(300)); // 5 minutes
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
/// cargo test test_name --release --features loom
/// ```
///
/// This will begin by running loom with no logs, checking all possible permutations of
/// multithreaded operations for our program (actually this tests _most_ permutations, with
/// limitations in regard to [SeqCst] and [Relaxed] ordering, but since we do not use those loom
/// will be exploring the full concurrent permutations, given enough time -and this can be a long
/// time). If an invariant is violated, this will cause the test to fail and the fail state to be
/// saved under `LOOM_CHECKPOINT_FILE`.
///
/// > We do not enable logs for this first run as loom might simulate many thousand permutations
/// > before finding a single failing case, and this would pollute `stdout`. Also, we run in
/// > `release` mode to make this process faster.
///
/// Once a failing case has been identified, resume the tests with:
///
/// ```bash
/// RUST_LOG=debug cargo test test_name --release --features loom
/// ```
///
/// This will resume testing with the previously failing case. We enable logging this time as only a
/// single iteration of the test will be run before the failure is caught.
///
/// > Note that if ever you update the code of a test, you will then need to delete
/// > `LOOM_CHECKPOINT_FILE` before running the tests again, otherwise loom will complain about
/// > having reached an unexpected execution path.
///
/// # Complexity explosion
///
/// Due to the way in which loom checks for concurrent access permutations, execution time will grow
/// exponentially with the size of the model. For this reason, some particularly demanding tests
/// are configured to prune branches which are less likely to reveal bugs. It does not take much
/// for a test to become demanding so most tests here use pruning in combination with a time limit.
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
/// # Test duplication
///
/// We are not _only_ writing multithreaded code, we are also writing _asynchronous_ code. For this
/// reason, we want to be able to run the same test under [`loom`] _and_ [`tokio`]. To avoid having
/// to write the same test multiple times, we use a series of macros which unify the API calls
/// between `loom` and `tokio`, so we can write a test once and have the compiler generate both
/// versions for us.
///
/// To run only the tokio version of each test, simply run:
///
/// ```bash
/// cargo test --release
/// ```
///
/// # Miri
///
/// We also run [Miri] on each test to ensure we do not rely on any undefinded behavior. To run the
/// Miri version of each test, you can use:
///
/// ```bash
/// MIRIFLAGS=-Zmiri-disable-isolation cargo +nightly miri test
/// ```
///
/// Note that this requires you to have Miri installed on your system.
///
/// [SeqCst]: std::sync::atomic::Ordering::SeqCst
/// [Relaxed]: std::sync::atomic::Ordering::Relaxed
/// [Miri]: https://github.com/rust-lang/miri
#[cfg(all(test, not(feature = "proptest")))]
pub(crate) mod test {
    use super::*;
    use crate::macros::test::*;
    use common::*;

    // Some helper functions to generate a loom model and an async test from the same code. We use
    // the loom model to fuzz concurrent branches and the async test to run against miri.

    /// Generates a test with no upper bound to the complexity to the loom simulation model,
    /// meaning all concurrent execution branches will be tested. Due to combinatory explosions, we
    /// can only use this in very simple tests.
    macro_rules! model {
        (async fn $func:ident() $($body:tt)+) => {
            loom!(async fn $func() $($body)+);
            miri!(async fn $func() $($body)+);
        };
    }

    /// Generates a test with an upper bound to the time spent and the number of branches which the
    /// loom model will explore. In practice, we use this the most since even relatively simple
    /// tests can result in many possible concurrent branches.
    macro_rules! model_bounded {
        (async fn $func:ident() $($body:tt)+) => {
            loom_bounded!(async fn $func() $($body)+);
            miri!(async fn $func() $($body)+);
        };
    }

    macro_rules! loom {
        (async fn $func:ident() $($body:tt)+) => {
            #[cfg(feature = "loom")]
            #[rstest::rstest]
            fn $func(#[with(stringify!($func))] model: loom::model::Builder) {
                model.check(|| {
                    $($body)+
                })
            }
        };
    }

    macro_rules! loom_bounded {
        (async fn $func:ident() $($body:tt)+) => {
            #[cfg(feature = "loom")]
            #[rstest::rstest]
            fn $func(#[with(stringify!($func))] model_bounded: loom::model::Builder) {
                model_bounded.check(|| {
                    $($body)+
                })
            }
        };
    }

    macro_rules! miri {
        (async fn $func:ident() $($body:tt)+) => {
            #[cfg(not(feature = "loom"))]
            #[tokio::test]
            #[rstest::rstest]
            async fn $func(#[allow(unused)] log_stdout: ()) {
                $($body)+
            }
        };
    }

    // Sanity check! We make sure we can send messages from another thread and receive them
    // correctly.
    model_bounded! {
        async fn simple_send() {
            let (sx, rx) = channel(4);
            let rx = &rx;

            let handle = spawn! {
                for i in 0..2 {
                    tracing::info!(i, "Sending element");
                    // Sends and receives happen concurrently and lock-free!
                    sx.send(i);
                }
            };

            // Notice how we are waiting for elements BEFORE we perform the join. This means we
            // might be waiting on some sends from the other thread.
            for i in 0..2 {
                // Messages have to be acknowledged explicitly by the receiver, else
                // they are added back to the queue to avoid message loss.
                block_on! {
                    assert_eq!(rx.recv().await.unwrap().read_acknowledge(), i)
                };
            };

            join!(handle);
        }
    }

    // Sending elements should work in single-producer, single-consumer mode.
    model! {
        async fn spsc_1() {
            let (sx, rx) = channel(1);

            let counter1 = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let counter2 = std::sync::Arc::clone(&counter1);

            assert_eq!(sx.queue.writ_indx(), 0);
            assert_eq!(sx.queue.writ_size(), 2);
            assert_eq!(sx.queue.read_indx(), 0);
            assert_eq!(sx.queue.read_size(), 0);

            // Producing thread
            let handle = spawn! {
                tracing::info!("Sending 42");
                assert_matches::assert_matches!(
                    sx.send(DropCounter::new(vec![42], counter1)),
                    None,
                    "Failed to send value, message queue is {:#?}",
                    sx.queue
                );
            };

            // Receiving thread
            block_on! {
                tracing::info!("Waiting for 42");
                let guard = rx.recv().await;
                assert_matches::assert_matches!(
                    guard,
                    Some(guard) => { assert_eq!(guard.read_acknowledge().get(), vec![42]) },
                    "Failed to acquire acknowledge guard, message queue is {:#?}",
                    rx.queue
                );

                tracing::info!("Checking close correctness");
                let guard = rx.recv().await;
                assert!(guard.is_none(), "Guard acquired on supposedly empty message queue: {:?}", guard.unwrap());

                tracing::info!("Checking drop correctness");
                assert_eq!(counter2.load(std::sync::atomic::Ordering::Acquire), 1)
            };

            join!(handle);
        }
    }

    // Same as above but now we try and send multiple elements
    model_bounded! {
        async fn spsc_2() {
            let (sx, rx) = channel(3);

            let counter1 = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let counter2 = std::sync::Arc::clone(&counter1);

            assert_eq!(sx.queue.writ_indx(), 0);
            assert_eq!(sx.queue.writ_size(), 8);
            assert_eq!(sx.queue.read_indx(), 0);
            assert_eq!(sx.queue.read_size(), 0);

            let handle = spawn! {
                for i in 0..2 {
                    tracing::info!(i, "Sending element");
                    assert_matches::assert_matches!(
                        sx.send(DropCounter::new(vec![i], std::sync::Arc::clone(&counter1))),
                        None,
                        "Failed to send {i}, message queue is {:#?}",
                        sx.queue
                    )
                }
            };

            block_on! {
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
            };

            join!(handle);
        }
    }

    // Sending elements should work in single-producer, multiple-consumer mode.
    model_bounded! {
        async fn spmc() {
            let (sx, rx1) = channel(3);
            let rx2 = rx1.resubscribe();
            let rx3 = rx1.resubscribe();

            let witness1 = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::default()));
            let witness2 = std::sync::Arc::clone(&witness1);
            let witness3 = std::sync::Arc::clone(&witness1);

            let counter1 = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let counter2 = std::sync::Arc::clone(&counter1);

            assert_eq!(sx.queue.writ_indx(), 0);
            assert_eq!(sx.queue.writ_size(), 8);
            assert_eq!(sx.queue.read_indx(), 0);
            assert_eq!(sx.queue.read_size(), 0);

            // Sending thread
            let handle1 = spawn! {
                for i in 0..2 {
                    tracing::info!(i, "Sending element");
                    assert_matches::assert_matches!(
                        sx.send(DropCounter::new(vec![i], std::sync::Arc::clone(&counter1))),
                        None,
                        "Failed to send {i}, message queue is {:#?}",
                        sx.queue
                    )
                }
            };

            // Receiving thread - 1
            let handle2 = spawn! {
                block_on! {
                    tracing::info!("Waiting for element");
                    let guard = rx1.recv().await;
                    assert_matches::assert_matches!(
                        guard,
                        Some(guard) => { witness1.lock().await.push(guard.read_acknowledge().get()) },
                        "Failed to acquire acknowledge guard, message queue is {:#?}",
                        rx1.queue
                    );
                }
            };

            // Receiving thread - 2
            let handle3 = spawn! {
                block_on! {
                    tracing::info!("Waiting for element");
                    let guard = rx2.recv().await;
                    assert_matches::assert_matches!(
                        guard,
                        Some(guard) => { witness2.lock().await.push(guard.read_acknowledge().get()) },
                        "Failed to acquire acknowledge guard, message queue is {:#?}",
                        rx2.queue
                    );
                }
            };

            join!(handle1);
            join!(handle2);
            join!(handle3);

            block_on! {
                tracing::info!(?rx3, "Checking close correctness");
                let guard = rx3.recv().await;
                assert!(guard.is_none(), "Guard acquired on supposedly empty message queue: {:?}", guard.unwrap());

                let mut witness = witness3.lock().await;
                witness.sort();
                tracing::info!(witness = ?*witness, "Checking receive correctness");

                assert_eq!(witness.len(), 2);
                for (expected, actual) in witness.iter().enumerate() {
                    assert_eq!(*actual, vec![expected]);
                }

                tracing::info!("Checking drop correctness");
                assert_eq!(counter2.load(std::sync::atomic::Ordering::Acquire), 2);
            };
        }
    }

    // Sending elements should work in multiple-producer, single-consumer mode.
    model_bounded! {
        async fn mpsc() {
            let (sx1, rx) = channel(3);
            let sx2 = sx1.resubscribe();

            let counter1 = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let counter2 = std::sync::Arc::clone(&counter1);
            let counter3 = std::sync::Arc::clone(&counter1);

            assert_eq!(sx1.queue.writ_indx(), 0);
            assert_eq!(sx1.queue.writ_size(), 8);
            assert_eq!(sx1.queue.read_indx(), 0);
            assert_eq!(sx1.queue.read_size(), 0);

            // Sending thread - 1
            let handle1 = spawn! {
                tracing::info!("Sending 42");
                assert_matches::assert_matches!(
                    sx1.send(DropCounter::new(vec![42], counter1)),
                    None,
                    "Failed to send 42, message queue is {:#?}",
                    sx1.queue
                )
            };

            // Sending thread - 2
            let handle2 = spawn! {
                tracing::info!("Sending 69");
                assert_matches::assert_matches!(
                    sx2.send(DropCounter::new(vec![69], counter2)),
                    None,
                    "Failed to send 69, message queue is {:#?}",
                    sx2.queue
                )
            };

            // Receiving thread
            block_on! {
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
            };

            join!(handle1);
            join!(handle2);
        }
    }

    // Sending elements should work in multiple-producer, mutiple-consumer mode.
    model_bounded! {
        async fn mpmc() {
            let (sx1, rx1) = channel(4);
            let sx2 = sx1.resubscribe();
            let rx2 = rx1.resubscribe();
            let rx3 = rx1.resubscribe();

            let counter1 = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let counter2 = std::sync::Arc::clone(&counter1);
            let counter3 = std::sync::Arc::clone(&counter1);

            let witness1 = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::default()));
            let witness2 = std::sync::Arc::clone(&witness1);
            let witness3 = std::sync::Arc::clone(&witness1);

            // Sending thread - 1
            let handle1 = spawn! {
                for i in 0..2 {
                    tracing::info!(i, "Sending element");
                    assert_matches::assert_matches!(
                        sx1.send(DropCounter::new(vec![i], std::sync::Arc::clone(&counter1))),
                        None,
                        "Failed to send {i}, message queue is {:#?}",
                        sx1.queue
                    )
                }
            };

            // Sending thread - 2
            let handle2 = spawn! {
                for i in 2..4 {
                    tracing::info!(i, "Sending element");
                    assert_matches::assert_matches!(
                        sx2.send(DropCounter::new(vec![i], std::sync::Arc::clone(&counter2))),
                        None,
                        "Failed to send {i}, message queue is {:#?}",
                        sx2.queue
                    )
                }
            };

            // Receiving thread - 1
            let handle3 = spawn! {
                block_on! {
                    for _ in 0..2 {
                        tracing::info!("Waiting for element");
                        let guard = rx1.recv().await;
                        assert_matches::assert_matches!(
                            guard,
                            Some(guard) => { witness1.lock().await.push(guard.read_acknowledge().get()) },
                            "Failed to acquire acknowledge guard, message queue is {:#?}",
                            rx1.queue
                        );
                    }
                }
            };

            // Receiving thread - 2
            let handle4 = spawn! {
                block_on! {
                    for _ in 0..2 {
                        tracing::info!("Waiting for element");
                        let guard = rx2.recv().await;
                        assert_matches::assert_matches!(
                            guard,
                            Some(guard) => { witness2.lock().await.push(guard.read_acknowledge().get()) },
                            "Failed to acquire acknowledge guard, message queue is {:#?}",
                            rx2.queue
                        );
                    }
                }
            };

            join!(handle1);
            join!(handle2);
            join!(handle3);
            join!(handle4);

            block_on! {
                tracing::info!(?rx3, "Checking close correctness");
                let guard = rx3.recv().await;
                assert!(guard.is_none(), "Guard acquired on supposedly empty message queue: {:?}", guard.unwrap());

                let mut witness = witness3.lock().await;
                witness.sort();
                tracing::info!(witness = ?*witness, "Checking receive correctness");

                assert_eq!(witness.len(), 4);
                for (expected, actual) in witness.iter().enumerate() {
                    assert_eq!(*actual, vec![expected]);
                }

                tracing::info!("Checking drop correctness");
                assert_eq!(counter3.load(std::sync::atomic::Ordering::Acquire), 4);
            };
        }
    }

    // Messages which are not acknowledged should be sent back into the queue.
    model_bounded! {
        async fn resend_1() {
            let (sx1, rx) = channel(2);

            let counter1 = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let counter2 = std::sync::Arc::clone(&counter1);

            assert_eq!(sx1.queue.writ_indx(), 0);
            assert_eq!(sx1.queue.writ_size(), 4);
            assert_eq!(sx1.queue.read_indx(), 0);
            assert_eq!(sx1.queue.read_size(), 0);

            let handle = spawn! {
                // Notice that we only send ONE element!
                tracing::info!("Sending 69");
                assert_matches::assert_matches!(
                    sx1.send(DropCounter::new(vec![69], counter1)),
                    None,
                    "Failed to send 69, message queue is {:#?}",
                    sx1.queue
                )
            };

            block_on! {
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
            };

            join!(handle);
        }
    }

    // Messages which are not acknowledged should be sent back into the queue, EVEN IF THERE IS NO
    // MORE SPACE LEFT TO SEND MESSSAGES. This is possible as we allocate twice the required
    // capacity upon creating the queue, and only half can be filled up by a normal send. Notice
    // that the `Drop` implementation of `MqGuard` can forcefully send messages even the send
    // cannot any more.
    model_bounded! {
        async fn resend_2() {
            let (sx1, rx1) = channel(1);
            let sx2 = sx1.resubscribe();
            let rx3 = rx1.resubscribe();

            let counter1 = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let counter2 = std::sync::Arc::clone(&counter1);
            let counter3= std::sync::Arc::clone(&counter1);

            let witness1 = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
            let witness2 = std::sync::Arc::clone(&witness1);
            let witness3 = std::sync::Arc::clone(&witness1);

            assert_eq!(sx1.queue.writ_indx(), 0);
            assert_eq!(sx1.queue.writ_size(), 2);
            assert_eq!(sx1.queue.read_indx(), 0);
            assert_eq!(sx1.queue.read_size(), 0);

            // We are looking to trigger a very specific situation where:
            //
            // 1. We send an element into the queue (we don't care which one)
            // 2. That element is received, but not acknowledged.
            // 3. WHILE that element's `MqGuard` is being dropped, but AFTER it has been marked as
            //    acknowledged (this is so it can be overwritten), ANOTHER element is sent into the
            //    queue, filling it up.
            // 4. The element which was not acknowledged is forcefully inserted into the queue.
            let handle1 = spawn! {
                block_on! {
                    tracing::info!("Sending 69");
                    if sx1.send(DropCounter::new(vec![69], counter1)).is_none() {
                        witness1.lock().await.push(69);
                    }

                    tracing::info!("Receiving");
                    let _guard = rx1.recv().await;

                    // <= HERE: we drop the guard and count on loom to fuzz a simultaneous insert
                }
            };

            let handle2 = spawn! {
                block_on! {
                    tracing::info!("Sending 42");
                    if sx2.send(DropCounter::new(vec![42], counter2)).is_none() {
                        witness2.lock().await.push(42);
                    }
                }
            };

            join!(handle1);
            join!(handle2);

            block_on! {
                let lock = witness3.lock().await;

                if lock.len() == 1 {
                    // Case 1: only 1 element was sent.
                    //
                    // This is the case that should happen most of the time, where we:
                    //
                    // - send a first element into the queue (success)
                    // - send another one (failure, the queue can only hold 1 element at a time)
                    // - drop the guard to the first element without acknowledging it, causing is to be resent into the
                    //   queue.

                    tracing::info!("Waiting for element");
                    let guard = rx3.recv().await;
                    assert_matches::assert_matches!(
                        guard,
                        Some(guard) => { assert_eq!(guard.read_acknowledge().get(), vec![lock[0]]); },
                        "Failed to acquire acknowledge guard, message queue is {:#?}",
                        rx3.queue
                    );

                    tracing::info!("Checking drop correctness");
                    assert_eq!(counter3.load(std::sync::atomic::Ordering::Acquire), 2);
                } else {
                    // Case 2: both elements are sent into a queue despite it having a size of 1.
                    //
                    // This is the lest trivial case where we:
                    //
                    // - send a first element (success)
                    // - drop its guard without acknowledging it
                    // - mark it as acknowledged (ready to be overwritten)
                    // - send a second element (success, since the queue is temporarily empty)
                    // - resend the first element (success, since we allocated twice the initial
                    //   capacity to handle this case).

                    let mut received = Vec::new();

                    tracing::info!("Waiting for element");
                    let guard = rx3.recv().await;
                    assert_matches::assert_matches!(
                        guard,
                        Some(guard) => { received.push(guard.read_acknowledge().get()); },
                        "Failed to acquire acknowledge guard, message queue is {:#?}",
                        rx3.queue
                    );

                    tracing::info!("Waiting for element");
                    let guard = rx3.recv().await;
                    assert_matches::assert_matches!(
                        guard,
                        Some(guard) => { received.push(guard.read_acknowledge().get()); },
                        "Failed to acquire acknowledge guard, message queue is {:#?}",
                        rx3.queue
                    );

                    received.sort();
                    assert_eq!(received[0], vec![42]);
                    assert_eq!(received[1], vec![69]);

                    tracing::info!("Checking drop correctness");
                    assert_eq!(counter3.load(std::sync::atomic::Ordering::Acquire), 2);
                }
            }
        }
    }

    // The ring buffer underlying the message queue should be able to wrap around in the case where
    // `capacity` messages have been sent and acknowledged.
    model! {
        async fn wrap_around() {
            let (sx1, rx1) = channel(2);
            let sx2 = sx1.resubscribe();
            let rx2 = rx1.resubscribe();
            let rx3 = rx1.resubscribe();

            let counter1 = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let counter2 = std::sync::Arc::clone(&counter1);
            let counter3 = std::sync::Arc::clone(&counter1);

            assert_eq!(sx1.queue.writ_indx(), 0);
            assert_eq!(sx1.queue.writ_size(), 4);
            assert_eq!(sx1.queue.read_indx(), 0);
            assert_eq!(sx1.queue.read_size(), 0);

            // Fill in the queue
            let handle = spawn! {
                block_on! {
                    for i in 0..2 {
                        tracing::info!(i, "Sending element");
                        assert_matches::assert_matches!(
                            sx1.send(DropCounter::new(vec![i], std::sync::Arc::clone(&counter1))),
                            None,
                            "Failed to send {i}, message queue is {:#?}",
                            sx1.queue
                        );

                        tracing::info!("Waiting for element");
                        let guard = rx1.recv().await;
                        tracing::info!("Received element");
                        assert_matches::assert_matches!(
                            guard,
                            Some(guard) => { assert_eq!(guard.read_acknowledge().get(), vec![i]); },
                            "Failed to acquire acknowledge guard {i}, message queue is {:#?}",
                            rx1.queue
                        );
                    }
                }
            };
            join!(handle);


            // Fill in the queue (again, ring buffer has wrapped around)
            let handle = spawn! {
                block_on! {
                    for i in 2..4 {
                        tracing::info!(i, "Sending element");
                        assert_matches::assert_matches!(
                            sx2.send(DropCounter::new(vec![i], std::sync::Arc::clone(&counter2))),
                            None,
                            "Failed to send {i}, message queue is {:#?}",
                            sx2.queue
                        );

                        tracing::info!("Waiting for element");
                        let guard = rx2.recv().await;
                        tracing::info!("Received element");
                        assert_matches::assert_matches!(
                            guard,
                            Some(guard) => { assert_eq!(guard.read_acknowledge().get(), vec![i]); },
                            "Failed to acquire acknowledge guard {i}, message queue is {:#?}",
                            rx2.queue
                        );
                    }
                }
            };
            join!(handle);

            // Empty the queue (again)
            block_on! {
                tracing::info!(?rx3, "Checking close correctness");
                let guard = rx3.recv().await;
                assert!(guard.is_none(), "Guard acquired on supposedly empty message queue: {:?}", guard.unwrap());

                tracing::info!("Checking drop correctness");
                assert_eq!(counter3.load(std::sync::atomic::Ordering::Acquire), 4)
            };
        }
    }

    // Closing the message queue should work correctly and not leak any memory, even with multiple
    // threads writing to the queue concurrently and DURING the close.
    model_bounded! {
        async fn close() {
            let (sx1, rx) = channel(3);
            let sx2 = sx1.resubscribe();

            let counter1 = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let counter2 = std::sync::Arc::clone(&counter1);
            let counter3 = std::sync::Arc::clone(&counter1);

            assert_eq!(sx1.queue.writ_indx(), 0);
            assert_eq!(sx1.queue.writ_size(), 8);
            assert_eq!(sx1.queue.read_indx(), 0);
            assert_eq!(sx1.queue.read_size(), 0);

            let handle1 = spawn! {
                for i in 0..2 {
                    tracing::info!(i, "Sending element");
                    sx1.send(DropCounter::new(vec![i], std::sync::Arc::clone(&counter1)));
                }

                // Both threads race to close, meaning any send might fail, or succeed but happen
                // during the close. This must not result int any leaked memory or invalid state.
                sx1.close();
            };

            let handle2 = spawn! {
                for i in 2..4 {
                    tracing::info!(i, "Sending element");
                    sx2.send(DropCounter::new(vec![i], std::sync::Arc::clone(&counter2)));
                }
                sx2.close();
            };

            join!(handle1);
            join!(handle2);

            block_on! {
                tracing::info!(?rx, "Checking close correctness");
                let guard = rx.recv().await;
                assert!(guard.is_none(), "Guard acquired on supposedly empty message queue: {:?}", guard.unwrap());

                // At this point all elements should have been dropped (because we called close
                // twice).
                tracing::info!("Checking drop correctness");
                assert_eq!(counter3.load(std::sync::atomic::Ordering::Acquire), 4)
            };
        }
    }

    // TODO: add close test where we do not call close twice

    // Sending ZSTs over the message queue should work.
    model_bounded! {
        async fn zst() {
            let (sx, rx) = channel(3);

            assert_eq!(sx.queue.writ_indx(), 0);
            assert_eq!(sx.queue.writ_size(), 8);
            assert_eq!(sx.queue.read_indx(), 0);
            assert_eq!(sx.queue.read_size(), 0);

            let handle = spawn! {
                for _ in 0..2 {
                    tracing::info!("Sending element");
                    assert_matches::assert_matches!(
                        sx.send(()),
                        None,
                        "Failed to send, message queue is {:#?}",
                        sx.queue
                    )
                }
            };

            block_on! {
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
            };

            join!(handle);
        }
    }

    // The message queue should refuse elements if it is full.
    model! {
        async fn fail_send() {
            let (sx1, rx) = channel(2);
            let sx2 = sx1.resubscribe();

            let counter1 = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let counter2 = std::sync::Arc::clone(&counter1);
            let counter3 = std::sync::Arc::clone(&counter1);

            assert_eq!(sx1.queue.writ_indx(), 0);
            assert_eq!(sx1.queue.writ_size(), 4);
            assert_eq!(sx1.queue.read_indx(), 0);
            assert_eq!(sx1.queue.read_size(), 0);

            // Filling up the message queue.
            let handle = spawn! {
                for i in 0..2 {
                    tracing::info!(i, "Sending element");
                    assert_matches::assert_matches!(
                        sx1.send(DropCounter::new(vec![i], std::sync::Arc::clone(&counter1))),
                        None,
                        "Failed to send {i}, message queue is {:#?}",
                        sx1.queue
                    )
                }
            };
            join!(handle);

            // New elements should be rejected.
            let handle = spawn! {
                tracing::info!("Sending element");
                assert_matches::assert_matches!(
                    sx2.send(DropCounter::new(vec![2], counter2)),
                    Some(counter) => { assert_eq!(counter.get(), vec![2]) },
                    "Queue should be full! {:#?}",
                    sx2.queue
                )
            };
            join!(handle);

            // Elements which have already been sent should still be available.
            block_on! {
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
            };
        }
    }
}

/// [proptest] is a hypothesis-like framework which allows us to test the invariants of a System
/// Under Test (or SUT) by having it run against many different simulated scenarios. We model this
/// here using [proptest_state_machine] to test our SUT under a series of legal transitions.
///
/// If during the run, any invariant is violated, then proptest will automatically try and regress
/// this issue to a minimum failing case by shrinking the inputs (the transitions) which lead to
/// the failed invariants. This has the advantage that generally, a problem which might have
/// occurred as the result of 100 transitions would be narrowed down to 4 transitions (for example).
///
/// At the same time, we can test for many, many more scenarios that we could ever write by hand
/// (the test below for example will simulate an average 262,144 different transitions and ensure
/// they are all valid).
///
/// Check out the [proptest book] if you are curious and want to learn more.
///
/// [proptest book]: https://proptest-rs.github.io/proptest/proptest/index.html
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
            cases: 1024,
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
        Shrink(String),
        #[error("Failed to grow region, no space left")]
        Grow(String),
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
        read: ReferenceRegion<Read>,
        writ: ReferenceRegion<Write>,
        next: u32,
        count_sx: usize,
        count_rx: usize,
        cap: u16,
        size: u16,
    }

    #[derive(Clone, Debug)]
    struct Read;
    #[derive(Clone, Debug)]
    struct Write;

    #[derive(Clone, Debug)]
    struct ReferenceRegion<Region: Clone + std::fmt::Debug> {
        start: u16,
        size: u16,
        _phantom: std::marker::PhantomData<Region>,
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
            (1..512u16).prop_map(Self::new).boxed()
        }

        fn transitions(state: &Self::State) -> BoxedStrategy<Self::Transition> {
            prop_oneof![
                // 75% of the time we are either sending or receiving
                6 => Just(Transition::Send(state.next)),
                6 => Just(Transition::Recv),
                // 25% of the time we are resubscribing or dropping
                1 => Just(Transition::ResubscribeSender),
                1 => Just(Transition::ResubscribeReceiver),
                1 => Just(Transition::DropSender),
                1 => Just(Transition::DropReceiver),
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
                    if state.count_sx != 0 {
                        state.count_sx += 1;
                    }
                    state
                }
                Transition::ResubscribeReceiver => {
                    if state.count_rx != 0 {
                        state.count_rx += 1;
                    }
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

        #[tracing::instrument(skip_all)]
        fn apply(
            mut state: Self::SystemUnderTest,
            ref_state: &<Self::Reference as ReferenceStateMachine>::State,
            transition: <Self::Reference as ReferenceStateMachine>::Transition,
        ) -> Self::SystemUnderTest {
            let file =
                std::fs::OpenOptions::new().append(true).create(true).open("./log").expect("Failed to open file");
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
                                Err(ref err) => {
                                    tracing::debug!(?err, "Write is illegal");
                                    assert_matches::assert_matches!(
                                        res,
                                        Some(e) => { assert_eq!(e, elem) },
                                        "{err:?}"
                                    );
                                    tracing::info!("Write failed successfully ;)");
                                }
                            }

                            tracing::debug!(?ref_state.writ, ?ref_state.read, "Checking read/write regions");

                            assert_eq!(sx.queue.writ_indx(), ref_state.writ.start);
                            assert_eq!(
                                sx.queue.writ_size(),
                                ref_state.writ.size,
                                "{}: {}",
                                sx.queue.cap,
                                ref_state.cap
                            );
                            assert_eq!(sx.queue.read_indx(), ref_state.read.start);
                            assert_eq!(sx.queue.read_size(), ref_state.read.size);

                            tracing::trace!("Restoring sender state");
                            state.sxs.push_front(sx);
                        } else {
                            tracing::debug!("No sender available");
                            assert_eq!(state.sxs.len(), 0);
                            assert_eq!(ref_state.count_sx, 0);
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
                                    Err(_) if ref_state.count_sx != 0 => {
                                        tracing::debug!("Read is illegal");
                                        assert_matches::assert_matches!(res, std::task::Poll::Pending);
                                        tracing::info!("Read failed successfully ;)");
                                    }
                                    _ => {
                                        tracing::debug!("Read with no senders or values");
                                        assert_eq!(state.sxs.len(), 0);
                                        assert_eq!(rx.queue.sender_count(), 0);
                                        assert_eq!(ref_state.count_sx, 0);
                                        assert_matches::assert_matches!(res, std::task::Poll::Ready(None));
                                        tracing::info!("Read failed successfully ;)");
                                    }
                                }
                            }

                            tracing::debug!(?ref_state.writ, ?ref_state.read, "Checking read/write regions");

                            assert_eq!(rx.queue.writ_indx(), ref_state.writ.start);
                            assert_eq!(rx.queue.writ_size(), ref_state.writ.size);
                            assert_eq!(rx.queue.read_indx(), ref_state.read.start);
                            assert_eq!(rx.queue.read_size(), ref_state.read.size);

                            tracing::trace!("Restoring receiver state");
                            state.rxs.push_front(rx);
                        } else {
                            tracing::debug!("No receiver available");
                            assert_eq!(state.rxs.len(), 0);
                            assert_eq!(ref_state.count_rx, 0);
                        }
                    }
                    Transition::ResubscribeSender => {
                        tracing::info!("Processing a RESUB SEND request");

                        if let Some(sx) = state.sxs.pop_back() {
                            tracing::debug!("Found a sender for resub");

                            state.sxs.push_front(sx.resubscribe());

                            tracing::trace!(?ref_state, "Comparing to reference state");
                            assert_eq!(sx.queue.sender_count(), ref_state.count_sx);
                            assert_eq!(state.sxs.len() + 1, ref_state.count_sx);

                            tracing::trace!("Restoring sender state");
                            state.sxs.push_back(sx);
                        } else {
                            tracing::debug!("No sender available");
                            assert_eq!(state.sxs.len(), 0);
                            assert_eq!(ref_state.count_sx, 0);
                        }
                    }
                    Transition::ResubscribeReceiver => {
                        tracing::info!("Processing a RESUB RECV request");

                        if let Some(rx) = state.rxs.pop_back() {
                            tracing::debug!("Found a receiver for resub");

                            state.rxs.push_front(rx.resubscribe());

                            tracing::trace!(?ref_state, "Comparing to reference state");
                            assert_eq!(state.rxs.len() + 1, ref_state.count_rx);

                            tracing::trace!("Restoring receiver state");
                            state.rxs.push_back(rx);
                        } else {
                            tracing::debug!("No receiver available");
                            assert_eq!(state.rxs.len(), 0);
                            assert_eq!(ref_state.count_rx, 0);
                        }
                    }
                    Transition::DropSender => {
                        tracing::info!("Processing a DROP SEND request");

                        if let Some(sx) = state.sxs.pop_back() {
                            tracing::debug!("Found a sender to drop");
                            let count = sx.queue.sender_count();
                            drop(sx);

                            tracing::trace!(?ref_state, "Comparing to reference state");
                            assert_eq!(count - 1, ref_state.count_sx);
                            assert_eq!(state.sxs.len(), ref_state.count_sx);
                        } else {
                            tracing::debug!("No sender available");
                            assert_eq!(state.sxs.len(), 0);
                            assert_eq!(ref_state.count_sx, 0);
                        }
                    }
                    Transition::DropReceiver => {
                        tracing::info!("Processing a DROP RECV request");

                        if let Some(rx) = state.rxs.pop_back() {
                            tracing::debug!("Found a receiver to drop");
                            drop(rx);

                            tracing::trace!(?ref_state, "Comparing to reference state");
                            assert_eq!(state.rxs.len(), ref_state.count_rx);
                        } else {
                            tracing::debug!("No receiver available");
                            assert_eq!(state.rxs.len(), 0);
                            assert_eq!(ref_state.count_rx, 0);
                        }
                    }
                }
            });
            state
        }
    }

    impl Reference {
        fn new(cap_base: u16) -> Self {
            let cap = cap_base.next_power_of_two();
            let size = cap << 1;
            Self {
                last_read: Default::default(),
                last_writ: Default::default(),
                status: Ok(()),
                read: ReferenceRegion::<Read>::new(),
                writ: ReferenceRegion::<Write>::new(size),
                next: 0,
                count_sx: 1,
                count_rx: 1,
                cap,
                size,
            }
        }

        fn read(mut self) -> Result<Self, PropError> {
            self.read.shrink(self.size).map(|_| {
                self.last_read.push_front(self.last_writ.pop_back().expect("Invalid state"));
                self.status = Ok(());
                self
            })
        }

        fn write(mut self, elem: u32) -> Result<Self, PropError> {
            self.writ
                .shrink(self.cap, self.size)
                .or_else(|_| self.writ.grow(self.size).and_then(|_| self.writ.shrink(self.cap, self.size)))
                .and_then(|_| self.read.grow(self.cap))
                .map(|_| {
                    self.last_writ.push_front(elem);
                    self.next += 1;
                    self.status = Ok(());
                    self
                })
        }
    }

    impl<Region: Clone + std::fmt::Debug> ReferenceRegion<Region> {
        fn len(&self) -> u16 {
            self.size
        }

        fn is_empty(&self) -> bool {
            self.len() == 0
        }

        fn is_full(&self, cap: u16) -> bool {
            self.size == cap
        }

        fn can_grow(&self, capsize: u16) -> Result<(), PropError> {
            if self.is_full(capsize) { Err(PropError::Grow(format!("{self:?}"))) } else { Ok(()) }
        }

        fn grow(&mut self, capsize: u16) -> Result<(), PropError> {
            self.can_grow(capsize).map(|_| self.size += 1)
        }
    }

    impl ReferenceRegion<Read> {
        fn new() -> Self {
            Self { start: 0, size: 0, _phantom: std::marker::PhantomData }
        }

        fn can_shrink(&self) -> Result<(), PropError> {
            if self.is_empty() { Err(PropError::Shrink(format!("{self:?}"))) } else { Ok(()) }
        }

        fn shrink(&mut self, size: u16) -> Result<(), PropError> {
            self.can_shrink().map(|_| {
                self.start = (self.start + 1) % size;
                self.size -= 1;
            })
        }
    }

    impl ReferenceRegion<Write> {
        fn new(size: u16) -> Self {
            ReferenceRegion::<Write> { start: 0, size, _phantom: std::marker::PhantomData }
        }

        fn can_shrink(&self, cap: u16) -> Result<(), PropError> {
            if self.size <= cap { Err(PropError::Shrink(format!("{self:?}"))) } else { Ok(()) }
        }

        fn shrink(&mut self, cap: u16, size: u16) -> Result<(), PropError> {
            self.can_shrink(cap).map(|_| {
                self.start = (self.start + 1) % size;
                self.size -= 1;
            })
        }
    }
}
