use std::ops::Add;

use num::{
    Integer, One, Zero,
    traits::{WrappingAdd, WrappingSub},
};

use crate::sync::*;

type IncReserveU16<GW, GR> = IncReserve<sync::atomic::AtomicU64, GW, GR>;
type IncReserveU8<GW, GR> = IncReserve<sync::atomic::AtomicU32, GW, GR>;

#[derive(Eq, PartialEq)]
pub struct RawBytes<T: QuarterSize + Sized> {
    raw: T::Raw,
    read_indx: T::Quarter,
    read_size: T::Quarter,
    writ_indx: T::Quarter,
    writ_size: T::Quarter,
}

impl<T: QuarterSize + Sized> Clone for RawBytes<T> {
    fn clone(&self) -> Self {
        Self {
            raw: self.raw,
            read_indx: self.read_indx,
            read_size: self.read_size,
            writ_indx: self.writ_indx,
            writ_size: self.writ_size,
        }
    }
}
impl<T: QuarterSize + Sized> Copy for RawBytes<T> {}

impl<T: QuarterSize + Sized> std::fmt::Debug for RawBytes<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawBytes")
            .field("raw", &self.raw)
            .field("read_indx", &self.read_indx)
            .field("read_size", &self.read_size)
            .field("writ_indx", &self.writ_indx)
            .field("writ_size", &self.writ_size)
            .finish()
    }
}

pub trait QuarterSize: Sized {
    type Raw: Copy + std::fmt::Debug;
    type Quarter: num::Zero
        + num::One
        + num::Integer
        + num::Unsigned
        + num::traits::WrappingSub
        + num::traits::WrappingAdd
        + std::cmp::PartialEq
        + std::fmt::Debug
        + Into<usize>
        + Copy;

    fn new(n: Self::Quarter) -> Self;
    fn read(&self) -> RawBytes<Self>;
    fn write(&self, pre: Self::Raw, new: &RawBytes<Self>) -> Result<Self::Raw, RawBytes<Self>>;
}
impl QuarterSize for sync::atomic::AtomicU64 {
    type Raw = u64;
    type Quarter = u16;

    fn new(n: Self::Quarter) -> Self {
        Self::new(n as u64)
    }

    fn read(&self) -> RawBytes<Self> {
        let raw_bytes = self.load(sync::atomic::Ordering::Acquire);
        RawBytes {
            raw: raw_bytes,
            read_indx: ((raw_bytes & 0xffff000000000000) >> 48) as u16,
            read_size: ((raw_bytes & 0x0000ffff00000000) >> 32) as u16,
            writ_indx: ((raw_bytes & 0x00000000ffff0000) >> 16) as u16,
            writ_size: ((raw_bytes & 0x000000000000ffff) >> 0) as u16,
        }
    }

    fn write(&self, pre: Self::Raw, new: &RawBytes<Self>) -> Result<Self::Raw, RawBytes<Self>> {
        let new = ((new.read_indx as u64) << 48)
            | ((new.read_size as u64) << 32)
            | ((new.writ_indx as u64) << 16)
            | new.writ_size as u64;
        match self.compare_exchange(pre, new, sync::atomic::Ordering::Release, sync::atomic::Ordering::Acquire) {
            Ok(raw) => Ok(raw),
            Err(raw_bytes) => Err(RawBytes {
                raw: raw_bytes,
                read_indx: ((raw_bytes & 0xffff000000000000) >> 48) as u16,
                read_size: ((raw_bytes & 0x0000ffff00000000) >> 32) as u16,
                writ_indx: ((raw_bytes & 0x00000000ffff0000) >> 16) as u16,
                writ_size: ((raw_bytes & 0x000000000000ffff) >> 0) as u16,
            }),
        }
    }
}
impl QuarterSize for sync::atomic::AtomicU32 {
    type Raw = u32;
    type Quarter = u8;

    fn new(n: Self::Quarter) -> Self {
        Self::new((n as u32) << 3)
    }

    fn read(&self) -> RawBytes<Self> {
        let raw_bytes = self.load(sync::atomic::Ordering::Acquire);
        RawBytes {
            raw: raw_bytes,
            read_indx: ((raw_bytes & 0xff000000) >> 24) as u8,
            read_size: ((raw_bytes & 0x00ff0000) >> 16) as u8,
            writ_indx: ((raw_bytes & 0x0000ff00) >> 8) as u8,
            writ_size: ((raw_bytes & 0x000000ff) >> 0) as u8,
        }
    }

    fn write(&self, pre: Self::Raw, new: &RawBytes<Self>) -> Result<Self::Raw, RawBytes<Self>> {
        let new = ((new.read_indx as u32) << 24)
            | ((new.read_size as u32) << 16)
            | ((new.writ_indx as u32) << 8)
            | (new.writ_size as u32);
        match self.compare_exchange(pre, new, sync::atomic::Ordering::Release, sync::atomic::Ordering::Acquire) {
            Ok(raw) => Ok(raw),
            Err(raw_bytes) => Err(RawBytes {
                raw: raw_bytes,
                read_indx: ((raw_bytes & 0xff000000) >> 24) as u8,
                read_size: ((raw_bytes & 0x00ff0000) >> 16) as u8,
                writ_indx: ((raw_bytes & 0x0000ff00) >> 8) as u8,
                writ_size: ((raw_bytes & 0x000000ff) >> 0) as u8,
            }),
        }
    }
}

pub trait AtomicRead {
    type Item;

    fn grow<S: QuarterSize + Sized>(&self, bytes: &RawBytes<S>) -> bool;
    fn read<S: QuarterSize + Sized>(&self, read_indx: S::Quarter) -> Self::Item;
}
pub trait AtomicWrit {
    type Item;

    fn grow<S: QuarterSize + Sized>(&self, bytes: &RawBytes<S>) -> bool;
    fn writ<S: QuarterSize + Sized>(&self, writ_indx: S::Quarter, item: Self::Item);
}

pub struct IncReserve<S, R, W>
where
    S: QuarterSize,
    R: AtomicRead,
    W: AtomicWrit,
{
    raw_bytes: S,
    size: S::Quarter,
    _phantom: std::marker::PhantomData<(R, W)>,
}

impl<S, R, W> IncReserve<S, R, W>
where
    S: QuarterSize,
    R: AtomicRead,
    W: AtomicWrit,
{
    pub fn new(size: S::Quarter) -> Self {
        Self { raw_bytes: S::new(size), size, _phantom: std::marker::PhantomData }
    }

    pub fn read_release(&self, grow_read: &R) -> Option<R::Item> {
        let mut bytes = self.raw_bytes.read();
        loop {
            // FIXME: is it really pertinent to allow the read region to grow here?
            if bytes.read_size.is_zero() && !grow_read.grow(&bytes) {
                break None;
            } else {
                bytes.read_size = bytes.writ_indx.wrapping_sub(&S::Quarter::one());
            }

            let read_indx = bytes.read_indx;
            bytes.read_indx = bytes.read_indx.wrapping_add(&S::Quarter::one()).mod_floor(&self.size);
            let data = Some(grow_read.read::<S>(read_indx)); // WARNING: don't read this yet!

            match self.raw_bytes.write(bytes.raw, &bytes) {
                Ok(_raw) => break data,
                Err(store) => bytes = store,
            }
        }
    }

    pub fn writ_reserve(&self, grow_writ: &W, item: W::Item) -> bool {
        let mut bytes = self.raw_bytes.read();
        let writ_indx = loop {
            if bytes.writ_size.is_zero() && !grow_writ.grow(&bytes) {
                return false;
            } else {
                bytes.writ_size = bytes.writ_size.wrapping_sub(&S::Quarter::one());
            }

            let writ_indx = bytes.writ_indx;
            bytes.writ_indx = bytes.writ_indx.wrapping_add(&S::Quarter::one()).mod_floor(&self.size);

            match self.raw_bytes.write(bytes.raw, &bytes) {
                Ok(_raw) => break writ_indx,
                Err(store) => bytes = store,
            }
        };

        grow_writ.writ::<S>(writ_indx, item);

        loop {
            bytes.read_size = bytes.read_size.wrapping_add(&S::Quarter::one());

            match self.raw_bytes.write(bytes.raw, &bytes) {
                Ok(_) => break true,
                Err(store) => bytes = store,
            }
        }
    }
}

struct AtomicStack<T: Send + Sync> {
    inc_reserve: IncReserveU16<Self, Self>,
    size: u16,
    ring: std::ptr::NonNull<T>,
}

impl<T: Send + Sync> AtomicRead for AtomicStack<T> {
    type Item = T;

    fn grow<S: QuarterSize + Sized>(&self, _bytes: &RawBytes<S>) -> bool {
        false
    }

    fn read<S: QuarterSize + Sized>(&self, read_indx: S::Quarter) -> Self::Item {
        unsafe { self.ring.add(Into::<usize>::into(read_indx)).read() }
    }
}

impl<T: Send + Sync> AtomicWrit for AtomicStack<T> {
    type Item = T;

    fn grow<S: QuarterSize + Sized>(&self, bytes: &RawBytes<S>) -> bool {
        if Into::<usize>::into(bytes.read_size.add(bytes.writ_size)) < self.size as usize {
            unsafe { self.ring.add(Into::<usize>::into(bytes.writ_indx)).drop_in_place() };
            true
        } else {
            false
        }
    }

    fn writ<S: QuarterSize + Sized>(&self, writ_indx: S::Quarter, item: Self::Item) {
        unsafe { self.ring.add(Into::<usize>::into(writ_indx)).write(item) }
    }
}

impl<T: Send + Sync> AtomicStack<T> {
    pub fn new(size: u16) -> Self {
        let layout = alloc::Layout::array::<T>(size as usize).unwrap();
        assert!(layout.size() <= isize::MAX as usize);

        let ptr = unsafe { alloc::alloc(layout) };
        let ring = match std::ptr::NonNull::new(ptr as *mut T) {
            Some(p) => p,
            None => std::alloc::handle_alloc_error(layout),
        };

        Self { inc_reserve: IncReserve::new(size), size, ring }
    }

    pub fn push(&self, item: T) -> bool {
        self.inc_reserve.writ_reserve(self, item)
    }

    pub fn pop(&self) -> Option<T> {
        self.inc_reserve.read_release(&self)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn push() {
        let stack = AtomicStack::new(4);
        assert!(stack.push(0));
        assert!(stack.push(1));
        assert!(stack.push(2));
        assert!(stack.push(3));
        assert!(!stack.push(4));
    }

    #[test]
    fn pop() {
        let stack = AtomicStack::new(1);
        assert!(stack.pop().is_none());
        assert!(stack.push(42));
        assert_eq!(stack.pop(), Some(42));
    }
}
