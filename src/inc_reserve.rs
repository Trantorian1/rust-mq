use num::{
    Integer, One, Zero,
    traits::{WrappingAdd, WrappingSub},
};

use crate::sync::*;

type IncReserveU16 = IncReserve<sync::atomic::AtomicU64>;
type IncReserveU8 = IncReserve<sync::atomic::AtomicU32>;

#[derive(Clone, Copy, Eq, PartialEq)]
struct RawBytes<T: QuarterSize + Sized> {
    raw: T::Raw,
    read_indx: T::Quarter,
    read_size: T::Quarter,
    writ_indx: T::Quarter,
    writ_size: T::Quarter,
}

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

pub struct IncReserve<S: QuarterSize> {
    raw_bytes: S,
    size: S::Quarter,
}

impl<S: QuarterSize> IncReserve<S> {
    pub fn new(size: S::Quarter) -> Self {
        Self { raw_bytes: S::new(size), size }
    }

    pub fn reserve(&self) -> bool {
        let mut load = self.raw_bytes.read();
        loop {
            if load.writ_size.is_zero() {
                return false;
            }

            load.writ_size = load.writ_size.wrapping_sub(&S::Quarter::one());
            load.writ_indx = load.writ_indx.wrapping_add(&S::Quarter::one()).mod_floor(&self.size);

            match self.raw_bytes.write(load.raw, &load) {
                Ok(raw) => {
                    load.raw = raw;
                    break;
                }
                Err(store) => load = store,
            }
        }

        // TODO: generic write logic goes here

        loop {
            load.read_size = load.read_size.wrapping_add(&S::Quarter::one());

            match self.raw_bytes.write(load.raw, &load) {
                Ok(_) => break true,
                Err(store) => load = store,
            }
        }
    }

    pub fn release(&self) -> bool {
        let mut load = self.raw_bytes.read();
        loop {
            if load.read_size.is_zero() {
                break false;
            }

            load.read_size = load.writ_indx.wrapping_sub(&S::Quarter::one());
            load.read_indx = load.read_indx.wrapping_add(&S::Quarter::one()).mod_floor(&self.size);

            // TODO: replace this with generic grow_write logic
            load.writ_size = load.writ_size.wrapping_add(&S::Quarter::one());

            match self.raw_bytes.write(load.raw, &load) {
                Ok(_) => break true,
                Err(store) => load = store,
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::inc_reserve::QuarterSize;

    use super::IncReserveU16;

    #[test]
    fn reserve() {
        let incres = IncReserveU16::new(4);
        assert!(incres.reserve());
        assert!(incres.reserve());
        assert!(incres.reserve());
        assert!(incres.reserve());
        assert!(!incres.reserve());
    }

    #[test]
    fn release() {
        let incres = IncReserveU16::new(1);
        assert!(incres.reserve());
        assert!(!incres.reserve());
        assert!(incres.release());
        assert!(incres.reserve());
        assert!(!incres.reserve());
    }
}
