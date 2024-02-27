package com.felix.job;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class MyKafkaJob {

    public static void main(String[] args) throws Exception {
//        myKafkaProducerDemo();
        myKafkaConsumerDemo();
    }

    private static void myKafkaConsumerDemo() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.enableCheckpointing(5000L);

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.159.111:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group_test");
        //partition分区发现
        props.put(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, 5000);

        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>("my-topic", new SimpleStringSchema(), props);
        flinkKafkaConsumer.setStartFromGroupOffsets();

        DataStreamSource<String> dataSource = env.addSource(flinkKafkaConsumer);
        dataSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                System.out.println(value);
            }
        });

        env.execute("myKafkaConsumerJob");
    }


    private static void myKafkaProducerDemo() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.enableCheckpointing(5000L);

        DataStreamSource<String> dataStream = env.addSource(new MyNoParallelSource()).setParallelism(1);
        //默认使用 FlinkFixedPartitioner
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>("192.168.159.111:9092", "my-topic", new SimpleStringSchema());
        //写入kafka时附加记录的事件时间戳
        producer.setWriteTimestampToKafka(true);
        dataStream.addSink(producer);
        env.execute("myKafkaProducerJob");
    }

    public static class MyNoParallelSource implements SourceFunction<String> {

        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (isRunning) {
                List<String> books = new ArrayList<>();
                books.add("西游记");
                books.add("水浒传");
                books.add("三国演义");
                books.add("红楼梦");
                int i = new Random().nextInt(4);
                ctx.collect(books.get(i));
                TimeUnit.SECONDS.sleep(2L);
            }

        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
