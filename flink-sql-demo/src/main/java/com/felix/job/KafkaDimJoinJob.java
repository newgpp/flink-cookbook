package com.felix.job;

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.time.Duration;

/**
 *
 */
public class KafkaDimJoinJob {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointInterval(15_000);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setIdleStateRetention(Duration.ofMinutes(15));

        tableEnv.executeSql("CREATE TABLE `t_order_kafka` (\n" +
                "  proctime as PROCTIME(),\n" +
                "  `order_id` BIGINT COMMENT '订单id',\n" +
                "  `order_no` BIGINT COMMENT '订单编号',\n" +
                "  `customer_id` BIGINT COMMENT '客户id',\n" +
                "  `order_time` TIMESTAMP COMMENT '下单时间',\n" +
                "  `order_status` TINYINT COMMENT '订单状态 0-下单 1-支付 2-取消 3-完成',\n" +
                "  `order_amount` DECIMAL(10,2) COMMENT '订单金额',\n" +
                "  `goods_id` BIGINT COMMENT '商品id',\n" +
                "  `created_time` TIMESTAMP COMMENT '创建时间',\n" +
                "  `updated_time` TIMESTAMP COMMENT '修改时间',\n" +
                "  `event_time` TIMESTAMP(3) METADATA FROM 'timestamp'\n" +
                ") WITH (\n" +
                "\t'connector' = 'kafka',\n" +
                "\t'topic' = 'my-topic',\n" +
                "\t'properties.bootstrap.servers' = '192.168.159.111:9092',\n" +
                "\t'properties.group.id' = 'order_join_group_01',\n" +
                "\t'properties.enable.auto.commit' = 'true',\n" +
                "\t'properties.auto.commit.interval.ms' = '1000',\n" +
                "\t'scan.startup.mode' = 'group-offsets',\n" +
                "\t'value.json.ignore-parse-errors' = 'true',\n" +
                "\t'value.format' = 'json'\n" +
                ")");

        tableEnv.executeSql("CREATE TABLE `dim_status` (\n" +
                "  `status_value` STRING COMMENT '状态',\n" +
                "  `status_code` TINYINT COMMENT '订单状态 0-下单 1-支付 2-取消 3-完成'\n" +
                ") WITH (\n" +
                "\t'connector' = 'jdbc',\n" +
                "\t'url' = 'jdbc:mysql://192.168.159.111:3306/oper_db',\n" +
                "\t'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "\t'lookup.cache.max-rows' = '10',\n" +
                "\t'lookup.cache.ttl' = '60s',\n" +
                "\t'username' = 'root',\n" +
                "\t'password' = '123456',\n" +
                "\t'table-name' = 'dim_order_status'\n" +
                ")");

        tableEnv.executeSql("CREATE TABLE `dim_customer` (\n" +
                "  `id` BIGINT,\n" +
                "  `name` STRING\n" +
                ") WITH (\n" +
                "\t'connector' = 'jdbc',\n" +
                "\t'url' = 'jdbc:mysql://192.168.159.111:3306/oper_db',\n" +
                "\t'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "\t'lookup.cache.max-rows' = '10',\n" +
                "\t'lookup.cache.ttl' = '60s',\n" +
                "\t'username' = 'root',\n" +
                "\t'password' = '123456',\n" +
                "\t'table-name' = 'dim_customer'\n" +
                ")");

        TableResult tableResult = tableEnv.executeSql("SELECT t1.order_id, t1.order_status, t2.status_value, t1.customer_id, t3.name FROM t_order_kafka t1 " +
                "LEFT JOIN dim_status FOR SYSTEM_TIME AS OF t1.proctime as t2 ON t1.order_status = t2.status_code " +
                "LEFT JOIN dim_customer FOR SYSTEM_TIME AS OF t1.proctime as t3 ON t1.customer_id = t3.id");

        try (CloseableIterator<Row> it = tableResult.collect()) {
            while (it.hasNext()) {
                Row row = it.next();
                System.out.println(row);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
