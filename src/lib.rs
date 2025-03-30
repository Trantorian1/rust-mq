//! `rust-mq` is a asynchronous, lock-free, multiple-producer, multiple-receiver message queue
//! implementation in unsafe Rust, designed to prevent message loss while maintaining high
//! performance and minimizing copies.
//!
//! This implementation has been thoroughly simulation tested to ensure proper functioning under
//! many different scenarios and concurrent execution pathways. See the [testing] section for more
//! information on this.
//!
//! # Usage
//!
//! You can create a new message queue with [`channel`]. This will allocate a fixed-size ringbuffer
//! up-front for the queue to use. `rust-mq` differs from other channels though in ways that make
//! it especially suited to handling application-critical information:
//!
//! 1. When [resubscribing], a new [receiver] will still be able to receive messages which were sent
//!    before it was created, as long as those messages have not already been received.
//!
//! 2. A message can only ever be read by a _single_ receiver.
//!
//! 3. Messages are not popped from the [`MessageQueue`] until they are [acknowledged].
//!
//! 4. It is not enough to [`read`] the value of a message, it has to be acknowledged manually.
//!
//! 5. If a message has not been acknowledged by the time it is dropped, it is added back to the
//!    queue. This avoids situations where a message has been received but some unexpected error
//!    causes it to be dropped before it can be fully processed (such as a panic). This can also be
//!    very useful in the context of cancellation safety or when running multiple futures against
//!    each other.
//!
//! The following is a simple example of using `rust-mq` as a bounded spsc channel:
//!
//! ```rust
//! #[tokio::main]
//! async fn main() {
//!     let (sx, rx) = rust_mq::channel(100);
//!
//!     let handle = tokio::spawn(async move {
//!         for i in 0..100 {
//!             // Sends and receives happen concurrently and lock-free!
//!             sx.send(i);
//!         }
//!     });
//!
//!     for i in 0..100 {
//!         // Messages have to be acknowledged explicitly by the receiver, else
//!         // they are added back to the queue to avoid message loss.
//!         assert_eq!(rx.recv().await.read_acknowledge(), i);
//!     };
//!
//!     handle.join().await;
//! }
//! ```
//!
//! # Testing
//!
//! To ensure correctness, `rust-mq` is thoroughly tested under many different scenarios. [`loom`]
//! is used to fuzz hundreds of thousands of concurrent execution pathways and drop calls and
//! [`proptest`] is used to test the system under hundreds of thousands of different send, recv and
//! drop transitions. On top of this, known edge cases are tested manually to avoid regressions.
//!
//! While this helps ensure a reasonable level of confidence in the system, this is by no means an
//! exaustive search as some concurrent execution tests had to be bounded in their exploration of
//! the problem space due to an exponential explosion in complexity. Similarly, proptest performs
//! a _reasonable_ exploration but does not check every single possible permutation of operations.
//!
//! Still, this gives us strong guarantees that `rust-mq` works as intended.
//!
//! # Known limitations
//!
//! `rust-mq` is still in early development, with some features missing or needing improvements.
//! This includes:
//!
//! ## [closing] a channel
//!
//! Currently, calling `close` on a channel will result in all elements sent to the message queue
//! which have not yet been read to be lost. This can be fixed by having `close` return these
//! elements, but it is still possible for some  elements to be lost if they were received at the
//! time of the close but not acknowledged. Adding a `close_wait` which waits for all receivers to
//! process their messages should fix this.
//!
//! ## Re-sending a message
//!
//! As the current implementation of [`MessageQueue`] is bounded, this can pause a problem when
//! trying to re-send a message which has not been acknowledged, as there might be no more space
//! left. This is made complicated because [`MqGuard`] works by holding an exclusive mutable
//! reference which points to the value it guards in the message queue. It seems very complicated to
//! allow the message queue to grow in this special case while keeping these references valid and
//! avoiding potential DOS vectors. This should still be possible by using a combination of atomic
//! pointers and some new atomic primitives to replace the references in `MqGuard`, but this will
//! take quite a bit of effort and significant testing to get right.
//!
//! ## Sending a message with no receivers
//!
//! It is currently possible to send a message with no receivers. Because of the way in which
//! [`resubscribe`] works, this means there would be no way to create new receivers and so the
//! value would be lost. Implementing `close_wait` as mentioned above should alleviate this issue,
//! but it would also be good for there to be a way to create a new [`MqReceiver`] from a
//! [`MqSender`].
//!
//! ## Async fuzz testing
//!
//! While the concurrent aspect of the system is already quite thoroughly tested, it remains
//! uncertain whether or not some bugs are possible in cases of strange ordering of async
//! operations. Async simulation testing in Rust is not as mature however so a custom
//! implementation of a simulator might be needed.
//!
//! # Benchmarks
//!
//! Considering the above limitations, I have to assume the API and inner workings of `rust-mq` will
//! still be changing quite a bit in the near future. I do not see a reason to start benchmarking
//! this code until its design is a bit more stable.
//!
//! [testing]: self#testing
//! [resubscribing]: MqSender::resubscribe
//! [sender]: MqSender
//! [receiver]: MqReceiver
//! [acknowledged]: MqGuard::acknowledge
//! [`read`]: MqGuard::read
//! [closing]: MqSender::close
//! [`resubscribe`]: MqSender::resubscribe

mod channel;
mod macros;
mod sync;

pub use channel::*;
