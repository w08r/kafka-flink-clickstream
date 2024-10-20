package com.github.w08r;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

class RS implements KafkaRecordSerializationSchema<String> {
    private String outTopic;

    public RS(String outTopic) {
        this.outTopic = outTopic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(String element,
            KafkaRecordSerializationSchema.KafkaSinkContext context,
            Long timestamp) {
        return new ProducerRecord<byte[], byte[]>(
                this.outTopic,
                java.util.UUID.randomUUID().toString().getBytes(),
                element.getBytes());
    }
}

class Click {
    private String url;

    public Click(String url) {
        this.url = url;
    }

    public String getUrl() {
        return this.url;
    }
}

class UrlCounter
        extends ProcessWindowFunction<Click, String, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<Click> input, Collector<String> out) {
        long count = 0;
        for (Click in : input) {
            count++;
        }
        out.collect(key + ":" + Long.toString(count));
    }
}

class TotalCounter
        extends ProcessAllWindowFunction<Click, String, TimeWindow> {

    @Override
    public void process(Context context, Iterable<Click> input, Collector<String> out) {
        long count = 0;
        for (Click in : input) {
            count++;
        }
        out.collect("total:" + Long.toString(count));
    }
}

public class DataStreamJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(1000);

        KafkaSource<String> ks = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("clicks")
                .setGroupId("flink")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSink<String> counts = KafkaSink.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setRecordSerializer(new RS("counts"))
                .build();
        KafkaSink<String> total = KafkaSink.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setRecordSerializer(new RS("total"))
                .build();

        DataStream<Click> in = env.fromSource(ks,
                // recommended watermark from reading from partitioned kafka.
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)),
                "clicks")
                .map(s -> new Click(s));

        // A tumbling window (no overlap) of counts grouped by url
        in.keyBy(c -> c.getUrl())
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
                .process(new UrlCounter())
                .sinkTo(counts);

        // sliding window (10 seconds size, 5 seconds slide) of total clicks
        in.windowAll(SlidingEventTimeWindows.of(Duration.ofSeconds(10), Duration.ofSeconds(5)))
                .process(new TotalCounter())
                .sinkTo(total);

        env.execute("Kafka streamer");
    }
}
