use rust_mq::channel;

#[tokio::main]
async fn main() {
    let (sx, rx) = channel(100);

    tokio::spawn(async move {
        for i in 0..100 {
            // Sends and receives happen concurrently and lock-free!
            sx.send(i).await;
        }
    });

    for i in 0..100 {
        // Messages have to be acknowledged explicitly by the receiver, else
        // they are added back to the queue to avoid message loss.
        assert_eq!(rx.recv().await.unwrap().read_acknowledge(), i);
    }
}
