package com.felix;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author felix
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
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);
        DataStreamSource<String> inputDataStream = env.addSource(flinkKafkaConsumer);
        //对输入数据进行转换和处理
        DataStream<String> dataStream = inputDataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> collector) throws Exception {
                // 处理数据的逻辑
                String[] words = value.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        });

        //设置检查点
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);

        dataStream.addSink(new SinkFunction<String>() {
            @Override
            public void invoke(String value) throws Exception {
                log.info(value);
            }
        });

        //执行程序
        env.execute("FlinkKafkaWorldCount");
    }
}
