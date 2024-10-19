use std::time::Duration;

use tokio::time;

use rdkafka::config::{FromClientConfig,ClientConfig};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::{Headers, Message};
use rdkafka::topic_partition_list::TopicPartitionList;

#[tokio::main]
async fn main() {
    let brokers = "kafka:9092";

    let topic_name = "counts";

    let consumer: &StreamConsumer = &ClientConfig::new()
        .set("group.id", "1")
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        //.set("statistics.interval.ms", "30000")
        //.set("auto.offset.reset", "smallest")
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&vec![topic_name])
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
                // println!("key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                      // m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp());
                println!("{}", payload);
                // if let Some(headers) = m.headers() {
                //     for header in headers.iter() {
                //         println!("  Header {:#?}: {:?}", header.key, header.value);
                //     }
                // }
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        };
    }
}
