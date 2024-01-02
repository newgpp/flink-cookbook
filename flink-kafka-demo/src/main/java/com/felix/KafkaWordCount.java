package com.felix;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author felix
 * https://stackoverflow.com/questions/64226597/apache-flink-to-use-s3-for-backend-state-and-checkpoints
 */
public class KafkaWordCount {

    private static final Logger log = LoggerFactory.getLogger(KafkaWordCount.class);

    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);
        // kafka连接参数
        String brokers = "192.168.159.111:9092";
        String topic = "wc-topic";
        String groupId = "wc-topic-flink-group-1";

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> inputDataStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
        //对输入数据进行转换和处理
        DataStream<String> dataStream = inputDataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> collector) throws Exception {
                // 处理数据的逻辑
                collector.collect(value);
            }
        }).name("word-input");

        //设置检查点
        env.enableCheckpointing(35000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        //s3a flink-s3-fs-hadoop
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("s3a://s3-bucket/checkpoints/"));

        dataStream.addSink(new SinkFunction<String>() {
            @Override
            public void invoke(String value) throws Exception {
                log.info(value);
            }
        }).name("word-count");

        //执行程序
        env.execute("FlinkKafkaWorldCount");
    }
}
