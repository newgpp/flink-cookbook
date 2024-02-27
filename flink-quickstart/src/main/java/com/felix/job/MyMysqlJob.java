package com.felix.job;

import com.felix.entity.Order;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MyMysqlJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.readTextFile("F:\\Idea Projects\\flink-cookbook\\quickstart\\src\\main\\resources\\orders.txt");

        DataStream<Order> orders = source.map(new MapFunction<String, Order>() {
            private final ObjectMapper mapper = new ObjectMapper();

            @Override
            public Order map(String value) throws Exception {
                return mapper.readValue(value, Order.class);
            }
        });

        SinkFunction<Order> mysqlSink = getMysqlSink();
        orders.addSink(mysqlSink);
        env.execute("mySql job");
    }

    private static SinkFunction<Order> getMysqlSink() {
        String sql = "insert into `t_order` (`order_no`, `customer_id`, `order_time`, `order_status`, `order_amount`, `goods_id`, `created_time`, `updated_time`) values(?, ?, ?, ?, ?, ?, ?, ?);";
        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
                .withMaxRetries(3)
                .withBatchSize(1)
                .withBatchIntervalMs(1000)
                .build();
        JdbcStatementBuilder<Order> statementBuilder = new JdbcStatementBuilder<Order>() {
            @Override
            public void accept(PreparedStatement psmt, Order order) throws SQLException {
                psmt.setLong(1, order.getOrder_no());
                psmt.setLong(2, order.getCustomer_id());
                psmt.setString(3, order.getOrder_time());
                psmt.setInt(4, order.getOrder_status());
                psmt.setBigDecimal(5, order.getOrder_amount());
                psmt.setLong(6, order.getGoods_id());
                psmt.setString(7, order.getCreated_time());
                psmt.setString(8, order.getUpdated_time());
            }
        };
        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUrl("jdbc:mysql://192.168.159.111:3306/oper_db?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true")
                .withUsername("root")
                .withPassword("123456")
                .build();
        return JdbcSink.sink(sql, statementBuilder, executionOptions, connectionOptions);
    }
}
