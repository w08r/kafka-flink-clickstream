package com.github.w08r;

import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;
import java.io.IOException;
import java.time.Duration;

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.Table;

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

    public Click(ObjectNode url) {
        this.url = url.get("value").get("url").asText();
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

class MyJSONKeyValueDeserializationSchema
        implements KafkaRecordDeserializationSchema<ObjectNode> {

    private JSONKeyValueDeserializationSchema kvd = new JSONKeyValueDeserializationSchema(false);

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        kvd.open(context);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<ObjectNode> out) throws IOException {
        try {
            this.kvd.deserialize(record, out);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public TypeInformation<ObjectNode> getProducedType() {
        return getForClass(ObjectNode.class);
    }
}

public class DataStreamJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(20000);

        KafkaSource<ObjectNode> ks = KafkaSource.<ObjectNode>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("clicks")
                .setGroupId("flink")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new MyJSONKeyValueDeserializationSchema())
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
                .map(o -> new Click(o));

        // A tumbling window (no overlap) of counts grouped by url
        in.keyBy(c -> c.getUrl())
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
                .process(new UrlCounter())
                .sinkTo(counts);

        // sliding window (10 seconds size, 5 seconds slide) of total clicks
        in.windowAll(SlidingEventTimeWindows.of(Duration.ofSeconds(10), Duration.ofSeconds(5)))
                .process(new TotalCounter())
                .sinkTo(total);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table inputTable = tableEnv.fromDataStream(in);

        env.execute("Kafka streamer");
    }
}
