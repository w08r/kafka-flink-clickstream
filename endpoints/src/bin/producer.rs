use rand::prelude::*;
use std::time::Duration;

use tokio::time;

use rdkafka::config::{FromClientConfig,ClientConfig};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};

#[tokio::main]
async fn main() {
    let brokers = "kafka:9092";

    let topic_name = "clicks";

    let mut binding = ClientConfig::new();
    let cfg = binding
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000");

    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let rng = rand::thread_rng();

    loop {
        // This loop is non blocking: all messages will be sent one after the other, without waiting
        // for the results.
        let futures = (0..50)
            .map(|i| {
                let mut rngc = rng.clone();
                let u = rngc.gen_range(0..8);
                async move {
                    // The send operation on the topic returns a future, which will be
                    // completed once the result or failure from Kafka is received.
                    let kv = format!("/url/{}", u);
                    let delivery_status = producer
                        .send(
                            FutureRecord::to(topic_name)
                                .payload(&kv)
                                .key(&kv)
                                .headers(OwnedHeaders::new().insert(Header {
                                    key: "header_key",
                                    value: Some("header_value"),
                                })),
                            Duration::from_secs(0),
                        )
                        .await;

                    // This will be executed when the result is received.
                    delivery_status
                }
            })
            .collect::<Vec<_>>();

        // This loop will wait until all delivery statuses have been received.
        for future in futures {
            future.await;
            // println!("Future completed. Result: {:?}", future.await);
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
