package com.felix.job;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import redis.clients.jedis.Jedis;

public class MyRedisSinkJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkJedisPoolConfig poolConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("192.168.159.111")
                .setPort(6379)
                .setPassword("redis123").build();

        env.fromElements("Spark", "Flink", "Storm")
                .map(new MapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(String value) throws Exception {
                        return new Tuple2<>(value, value);
                    }
                })
                .addSink(new RedisSink<>(poolConfig, new RedisSetMapper()));

        env.execute("redisSetJob");
    }

    public static class RedisSetMapper implements RedisMapper<Tuple2<String, String>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SET);
        }

        @Override
        public String getKeyFromData(Tuple2<String, String> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, String> data) {
            return data.f1;
        }
    }

    /**
     * 自定义RedisSink
     */
    public static class MyRedisSink extends RichSinkFunction<Tuple2<String, String>> {

        private transient Jedis jedis;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            jedis = new Jedis("192.168.159.111", 6379);
            jedis.auth("redis123");
        }

        @Override
        public void close() throws Exception {
            super.close();
            jedis.close();
        }

        @Override
        public void invoke(Tuple2<String, String> value, Context context) throws Exception {
            if (!jedis.isConnected()) {
                jedis.connect();
            }
            jedis.set(value.f0, value.f1);
        }
    }
}
