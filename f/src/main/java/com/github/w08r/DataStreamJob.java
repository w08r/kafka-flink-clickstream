package com.github.w08r;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

class RS implements KafkaRecordSerializationSchema<String> {
    @Override
    public ProducerRecord<byte[], byte[]> serialize(String element,
            KafkaRecordSerializationSchema.KafkaSinkContext context,
            Long timestamp) {
        return new ProducerRecord<byte[], byte[]>(
                "counts",
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

class MyProcessWindowFunction
    extends ProcessWindowFunction<Click, String, String, TimeWindow> {

  @Override
  public void process(String key, Context context, Iterable<Click> input, Collector<String> out) {
    long count = 0;
    for (Click in: input) {
      count++;
    }
    out.collect(key + ":" + Long.toString(count));
  }
}

public class DataStreamJob {

     public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> ks = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("clicks")
                .setGroupId("flink")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSink<String> kn = KafkaSink.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setRecordSerializer(new RS())
                .build();

        DataStream<String> in = env.fromSource(ks, WatermarkStrategy.noWatermarks(), "flink");
        in.map(s -> new Click(s))
            .keyBy(c -> c.getUrl())
            .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
            .process(new MyProcessWindowFunction())
            .sinkTo(kn);

        env.execute("Kafka streamer");
    }
}
