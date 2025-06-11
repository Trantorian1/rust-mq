use crate::sync::*;

struct RawBytes<T: QuarterSize + Sized> {
    raw: T::Raw,
    a: T::Quarter,
    b: T::Quarter,
    c: T::Quarter,
    d: T::Quarter,
}

trait QuarterSize: Sized {
    type Raw;
    type Quarter;

    fn new() -> Self;
    fn read(&self) -> RawBytes<Self>;
    fn write(&self, pre: Self::Raw, new: RawBytes<Self>) -> Result<(), RawBytes<Self>>;
}
impl QuarterSize for sync::atomic::AtomicU64 {
    type Raw = u64;
    type Quarter = u16;

    fn new() -> Self {
        Self::new(0)
    }

    fn read(&self) -> RawBytes<Self> {
        let raw_bytes = self.load(sync::atomic::Ordering::Acquire);
        RawBytes {
            raw: raw_bytes,
            a: ((raw_bytes & 0xffff000000000000) >> 6) as u16,
            b: ((raw_bytes & 0x0000ffff00000000) >> 4) as u16,
            c: ((raw_bytes & 0x00000000ffff0000) >> 2) as u16,
            d: ((raw_bytes & 0x000000000000ffff) >> 0) as u16,
        }
    }

    fn write(&self, pre: Self::Raw, new: RawBytes<Self>) -> Result<(), RawBytes<Self>> {
        let new = ((new.a as u64) << 6) & ((new.b as u64) << 4) & ((new.c as u64) << 2) & new.d as u64;
        match self.compare_exchange(pre, new, sync::atomic::Ordering::Release, sync::atomic::Ordering::Acquire) {
            Ok(_) => Ok(()),
            Err(raw_bytes) => Err(RawBytes {
                raw: raw_bytes,
                a: ((raw_bytes & 0xffff000000000000) >> 6) as u16,
                b: ((raw_bytes & 0x0000ffff00000000) >> 4) as u16,
                c: ((raw_bytes & 0x00000000ffff0000) >> 2) as u16,
                d: ((raw_bytes & 0x000000000000ffff) >> 0) as u16,
            }),
        }
    }
}
impl QuarterSize for sync::atomic::AtomicU32 {
    type Raw = u32;
    type Quarter = u8;

    fn new() -> Self {
        Self::new(0)
    }

    fn read(&self) -> RawBytes<Self> {
        let raw_bytes = self.load(sync::atomic::Ordering::Acquire);
        RawBytes {
            raw: raw_bytes,
            a: ((raw_bytes & 0xff000000) >> 3) as u8,
            b: ((raw_bytes & 0x00ff0000) >> 2) as u8,
            c: ((raw_bytes & 0x0000ff00) >> 1) as u8,
            d: ((raw_bytes & 0x000000ff) >> 0) as u8,
        }
    }

    fn write(&self, pre: Self::Raw, new: RawBytes<Self>) -> Result<(), RawBytes<Self>> {
        let new = ((new.a as u32) << 3) & ((new.b as u32) << 2) & ((new.c as u32) << 1) & (new.d as u32);
        match self.compare_exchange(pre, new, sync::atomic::Ordering::Release, sync::atomic::Ordering::Acquire) {
            Ok(_) => Ok(()),
            Err(raw_bytes) => Err(RawBytes {
                raw: raw_bytes,
                a: ((raw_bytes & 0xff000000) >> 3) as u8,
                b: ((raw_bytes & 0x00ff0000) >> 2) as u8,
                c: ((raw_bytes & 0x0000ff00) >> 1) as u8,
                d: ((raw_bytes & 0x000000ff) >> 0) as u8,
            }),
        }
    }
}

pub struct IncReserve<T: QuarterSize>(T);
