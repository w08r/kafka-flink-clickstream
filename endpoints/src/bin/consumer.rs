use rdkafka::config::{ClientConfig};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::message::{Message};

#[tokio::main]
async fn main() {
    let brokers = "kafka:9092";

    let consumer: &StreamConsumer = &ClientConfig::new()
        .set("group.id", "1")
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&vec!["counts", "total"])
        .expect("Can't subscribe to specified topics");

    loop {
        match consumer.recv().await {
            Err(e) => println!("Kafka error: {}", e),
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        println!("Error while deserializing message payload: {:?}", e);
                        ""
                    }
                };
                println!("{}", payload);
                consumer.commit_message(&m, CommitMode::Async).expect("Commit failed");
            }
        };
    }
}
