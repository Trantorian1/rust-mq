use crate::macros::*;
use crate::sync::*;

#[cfg(test)]
pub trait TBound: Send + Clone + std::fmt::Debug {}
#[cfg(test)]
impl<T: Send + Clone + std::fmt::Debug> TBound for T {}

#[cfg(not(test))]
pub trait TBound: Send + Clone {}
#[cfg(not(test))]
impl<T: Send + Clone> TBound for T {}

/// An atomic linked list which acts as a fifo deque. We incurr a cache invalidation the first
/// time we access the head or the tail but commonly accesses segments on each extremity of the
/// queue should be kept in cache.
#[repr(C)]
struct AtomicWakerList {
    // We can probably assume that both the first few nodes at the head and the last few nodes at
    // the tail will be kept around in (ideally L1) cache as long as we keep pushing at popping at
    // a constant rate (ie: we often read a few head nodes and a few tail nodes).
    // TODO: do not deallocate once a cell is made empty!!!
    head: Option<std::sync::atomic::AtomicPtr<CacheLine>>,
    tail: Option<std::sync::atomic::AtomicPtr<CacheLine>>,
}

/// Its fine for this to be singularly-linked since we keep track of the head and the tail of the
/// queue in [`AtomicWakerList`] and can leverage that when performing insert/pop operations.     
#[repr(C)]
struct CacheLine {
    //                                                   64 bytes (budget)
    strt: u32,                              //  4 * 1 =>  4 bytes
    size: u32,                              //  4 * 1 =>  4 bytes
    next: std::sync::atomic::AtomicPtr<()>, //  8 * 1 =>  8 bytes
    waker: [std::task::Waker; 15],          // 16 * 3 => 48 bytes
}

#[cfg(test)]
mod test {
    #[test]
    fn foo() {
        println!("sizeof(Waker): {}", size_of::<std::task::Waker>());
        println!("sizeof(AtomicPtr): {}", size_of::<std::sync::atomic::AtomicPtr<()>>());
        println!("sizeof(CacheLine): {}", size_of::<super::CacheLine>());
        println!("sizeof(Option<CacheLine>): {}", size_of::<Option<super::CacheLine>>());
        println!("sizeof(AtomicWakerList): {}", size_of::<super::AtomicWakerList>());
    }
}
