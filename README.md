# 🦀 `rust-mq`

`rust-mq` is a **asynchronous**, **lock-free**, multiple-producer, multiple-receiver message queue 
implementation in unsafe Rust, designed to prevent message loss while maintaining high
performance and minimizing copies.

You can read more about the implementation details and how it works by by reading the docs:

```bash
cargo doc --no-deps --all-features && \
    open target/doc/rust_mq/index.html
```

## ⚙️ Usage

You can create a new message queue with `channel`. This will allocate a fixed-size ring buffer
up-front for the queue to use. `rust-mq` differs from other channels though in ways that make
it especially suited to handling application-critical information:

1. When [resubscribing], a new [receiver] will still be able to receive messages which were sent
   before it was created, as long as those messages have not already been received.

2. A message can only ever be read by a _single_ receiver.

3. Messages are not popped from the message queue until they are acknowledged.

4. It is not enough to `read` the value of a message, it has to be acknowledged manually.

5. If a message has not been acknowledged by the time it is dropped, it is added back to the
   queue. This avoids situations where a message has been received but some unexpected error
   causes it to be dropped before it can be fully processed (such as a panic). This can also be
   very useful in the context of cancellation safety or when running multiple futures against
   each other.

The following is a simple example of using `rust-mq` as a bounded spsc channel:

```rust
#[tokio::main]
async fn main() {
    let (sx, rx) = rust_mq::channel(100);

    let handle = tokio::spawn(async move {
        for i in 0..100 {
            // Sends and receives happen concurrently and lock-free!
            sx.send(i);
        }
    });

    for i in 0..100 {
        // Messages have to be acknowledged explicitly by the receiver, else
        // they are added back to the queue to avoid message loss.
        assert_eq!(rx.recv().await.read_acknowledge(), i);
    };

    handle.join().await;
}
```

## 📝 Testing
 
To ensure correctness, `rust-mq` is thoroughly tested under many different scenarios. [`loom`]
is used to fuzz hundreds of thousands of concurrent execution pathways and drop calls and
[`proptest`] is used to test the system under hundreds of thousands of different send, recv and
drop transitions. On top of this, known edge cases are tested manually to avoid regressions.
 
While this helps ensure a reasonable level of confidence in the system, this is by no means an
exaustive search as some concurrent execution tests had to be bounded in their exploration of
the problem space due to an exponential explosion in complexity. Similarly, proptest performs
a _reasonable_ exploration but does not check every single possible permutation of operations.
 
Still, this gives us strong guarantees that `rust-mq` works as intended.

[`loom`]: https://github.com/tokio-rs/loom 
[`proptest`]: https://github.com/proptest-rs/proptest
