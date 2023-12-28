package com.felix;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * @author felix
 */
public class KafkaProduceTest {

    private static final Logger log = LoggerFactory.getLogger(KafkaProduceTest.class);

    private static final String brokers = "192.168.159.111:9092";
    private static final String topic = "wc-topic";

    private static KafkaProducer<String, String> producer;

    @Before
    public void init() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        producer = new KafkaProducer<>(props);
    }

    private void sendMsg(String topic, String key, String msg) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, msg);
        try {
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata recordMetadata = future.get();
            System.out.println(recordMetadata);
        } catch (Exception e) {
            log.error("MQ发送消息异常: ", e);
        }
    }

    @Test
    public void send_msg_should_success() {
        //given
        InputStream inputStream = KafkaProduceTest.class.getClassLoader().getResourceAsStream("Hamlet.txt");

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                sendMsg(topic, null, line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
