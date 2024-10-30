use rand::prelude::*;
use rdkafka::config::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::Serialize;
use std::env;
use std::time::{Duration, SystemTime};

#[derive(Serialize)]
struct Click<'a> {
    url: &'a String,
    time: u64,
}

#[tokio::main]
async fn main() {
    let brokers = match env::var("KAFKA_BROKERS") {
        Ok(v) => v,
        Err(_) => String::from("kafka:9092"),
    };

    let topic_name = "clicks";

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
            .map(|_i| {
                let mut rngc = rng.clone();
                let u = rngc.gen_range(0..8);
                let k = format!("/url/{}", u);
                async move {
                    // The send operation on the topic returns a future, which will be
                    // completed once the result or failure from Kafka is received.
                    let kv = serde_json::to_string(&Click {
                        url: &k,
                        time: SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .expect("failed to get time")
                            .as_secs(),
                    })
                    .expect("failed to create json payload");
                    

                    // This will be executed when the result is received.
                    producer
                        .send(
                            FutureRecord::to(topic_name)
                                .payload(&kv)
                                .key(&serde_json::to_string(&k).expect("failed to json key"))
                                .headers(OwnedHeaders::new().insert(Header {
                                    key: "header_key",
                                    value: Some("header_value"),
                                })),
                            Duration::from_secs(0),
                        )
                        .await
                }
            })
            .collect::<Vec<_>>();

        // This loop will wait until all delivery statuses have been received.
        for future in futures {
            let _ = future.await;
            // println!("Future completed. Result: {:?}", future.await);
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
