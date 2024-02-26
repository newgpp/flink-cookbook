package com.felix.job;

import com.sun.org.apache.xpath.internal.operations.String;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

public class Mysql2PrintJob {

    public static void main(String[] args) throws Exception {
        sqlCdcPrint();
    }

    private static void sqlCdcPrint() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointInterval(5_000L);
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

        TableResult tableResult = tableEnv.executeSql("SELECT * FROM t_order");
        CloseableIterator<Row> collect = tableResult.collect();
        collect.forEachRemaining(System.out::println);
    }
}
