package com.felix.job;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Mysql2MysqlJob {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.getCheckpointConfig().setCheckpointInterval(10_000L);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE `t_order` (\n" +
                "  `order_id` BIGINT COMMENT '订单id',\n" +
                "  `order_no` BIGINT COMMENT '订单编号',\n" +
                "  `customer_id` BIGINT COMMENT '客户id',\n" +
                "  `order_time` TIMESTAMP COMMENT '下单时间',\n" +
                "  `order_status` TINYINT COMMENT '订单状态 0-下单 1-支付 2-取消 3-完成',\n" +
                "  `order_amount` DECIMAL(10,2) COMMENT '订单金额',\n" +
                "  `goods_id` BIGINT COMMENT '商品id',\n" +
                "  `created_time` TIMESTAMP COMMENT '创建时间',\n" +
                "  `updated_time` TIMESTAMP COMMENT '修改时间',\n" +
                "  PRIMARY KEY (`order_id`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "\t'connector' = 'mysql-cdc',\n" +
                "\t'hostname' = '192.168.159.111',\n" +
                "\t'port' = '3306',\n" +
                "\t'username' = 'root',\n" +
                "\t'password' = '123456',\n" +
                "\t'database-name' = 'oper_db',\n" +
                "\t'table-name' = 't_order'\n" +
                ")");

        tableEnv.executeSql("CREATE TABLE `t_order_cdc` (\n" +
                "  `order_id` BIGINT COMMENT '订单id',\n" +
                "  `order_no` BIGINT COMMENT '订单编号',\n" +
                "  `customer_id` BIGINT COMMENT '客户id',\n" +
                "  `order_time` TIMESTAMP COMMENT '下单时间',\n" +
                "  `order_status` TINYINT COMMENT '订单状态 0-下单 1-支付 2-取消 3-完成',\n" +
                "  `order_amount` DECIMAL(10,2) COMMENT '订单金额',\n" +
                "  `goods_id` BIGINT COMMENT '商品id',\n" +
                "  `created_time` TIMESTAMP COMMENT '创建时间',\n" +
                "  `updated_time` TIMESTAMP COMMENT '修改时间',\n" +
                "  PRIMARY KEY (`order_id`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "\t'connector' = 'jdbc',\n" +
                "\t'url' = 'jdbc:mysql://192.168.159.111:3306/oper_db',\n" +
                "\t'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "\t'username' = 'root',\n" +
                "\t'password' = '123456',\n" +
                "\t'table-name' = 't_order_cdc'\n" +
                ")");

        tableEnv.executeSql("INSERT INTO t_order_cdc SELECT * FROM t_order");

    }
}
