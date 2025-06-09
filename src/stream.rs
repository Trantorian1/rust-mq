use crate::macros::*;
use crate::sync::*;

const WRIT_INDX_MASK: u64 = 0xffff000000000000;
const WRIT_SIZE_MASK: u64 = 0x0000ffff00000000;
const READ_INDX_MASK: u64 = 0x00000000ffff0000;
const READ_SIZE_MASK: u64 = 0x000000000000ffff;

#[cfg(test)]
pub trait TBound: Send + Sync + Clone + std::fmt::Debug {}
#[cfg(test)]
impl<T: Send + Sync + Clone + std::fmt::Debug> TBound for T {}

#[cfg(not(test))]
pub trait TBound: Send + Sync + Clone {}
#[cfg(not(test))]
impl<T: Send + Sync + Clone> TBound for T {}

#[derive(std::fmt::Debug, thiserror::Error)]
enum SendError {}

struct MqSender<T: TBound> {
    queue: std::sync::Arc<Mq<T>>,
    wakers: std::sync::Arc<WakerChain>,
}

struct MqReceiver<T: TBound> {
    queue: std::sync::Arc<Mq<T>>,
    wakers: std::sync::Arc<WakerChain>,
}

#[must_use]
pub struct MqGuard<'a, T: TBound> {
    ack: bool,
    cell: &'a mut AckCell<T>,
    queue: sync::Arc<Mq<T>>,
    waker: sync::Arc<Waker>,
    _phantom: std::marker::PhantomData<&'a ()>,
}

struct WakerChain {
    head: std::sync::atomic::AtomicPtr<WakerNode>,
}

struct WakerNode {
    waker: std::task::Waker,
    next: Box<WakerNode>,
}

struct Mq<T: TBound> {
    ring: std::ptr::NonNull<AckCell<T>>,
    senders: sync::atomic::AtomicUsize,
    read_write: sync::atomic::AtomicU64,
    cap: u16,
    size: u16,
}

unsafe impl<T: TBound> Send for MqSender<T> {}
unsafe impl<T: TBound> Sync for MqSender<T> {}

unsafe impl<T: TBound> Send for MqReceiver<T> {}
unsafe impl<T: TBound> Sync for MqReceiver<T> {}

unsafe impl<T: TBound> Send for MqGuard<'_, T> {}
unsafe impl<T: TBound> Sync for MqGuard<'_, T> {}

unsafe impl<T: TBound> Send for Mq<T> {}
unsafe impl<T: TBound> Sync for Mq<T> {}

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

impl<T: TBound> Drop for Mq<T> {
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

impl<'a, T: TBound> MqGuard<'a, T> {
    fn new(cell: &'a mut AckCell<T>, queue: sync::Arc<Mq<T>>, waker: sync::Arc<Waker>) -> Self {
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

/// An atomic acknowledge cell, use to ensure an element has been read.
struct AckCell<T: TBound> {
    elem: std::mem::MaybeUninit<T>,
    ack: sync::atomic::AtomicBool,
}

impl<T: TBound> futures::Sink<T> for MqSender<T> {
    type Error = SendError;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        todo!()
    }

    fn start_send(self: std::pin::Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        todo!()
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        todo!()
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        todo!()
    }
}

impl<T: TBound> futures::Stream for MqReceiver<T> {
    type Item = T;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        todo!()
    }
}

impl<T: TBound> AckCell<T> {
    fn new(elem: T) -> Self {
        Self { elem: std::mem::MaybeUninit::new(elem), ack: sync::atomic::AtomicBool::new(false) }
    }
}

impl<T: TBound> Mq<T> {
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

fn get_raw_bytes(writ_indx: u16, writ_size: u16, read_indx: u16, read_size: u16) -> u64 {
    ((writ_indx as u64) << 48) | ((writ_size as u64) << 32) | ((read_indx as u64) << 16) | read_size as u64
}
