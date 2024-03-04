package com.felix.job;

import org.junit.Test;

public class FlinkTest {

    @Test
    public void print(){

        System.out.println("CREATE TABLE `dim_status` (\n" +
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
    }
}
