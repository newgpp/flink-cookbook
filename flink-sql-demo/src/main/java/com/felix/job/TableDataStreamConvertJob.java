package com.felix.job;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * table -> data stream: org.apache.flink.table.api.bridge.java.StreamTableEnvironment#toDataStream
 * data stream -> table: org.apache.flink.table.api.bridge.java.StreamTableEnvironment#fromDataStream
 */
public class TableDataStreamConvertJob {

    private static final Logger log = LoggerFactory.getLogger(TableDataStreamConvertJob.class);

    public static void main(String[] args) throws Exception {
//        table2DataStream();
        dataStream2Table();
    }

    private static void dataStream2Table() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointInterval(15_000);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<Integer> stream = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        //data stream -> table
        Table table = tableEnv.fromDataStream(stream, Schema.newBuilder()
                .columnByExpression("c1", "f0 + 42")
                .columnByExpression("c2", "f0 - 1")
                .build());

        tableEnv.createTemporaryView("demo", table);
        tableEnv.executeSql("CREATE TABLE print_sink(\n" +
                "  c1 INT,\n" +
                "  c2 INT\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ");");
        tableEnv.executeSql("insert into print_sink select c1, c2 from demo");
        env.execute("dataStream2Table job");
    }


    private static void table2DataStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointInterval(15_000);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String createTableSql = "CREATE TABLE source_table (\n"
                + "    id BIGINT,\n"
                + "    money BIGINT,\n"
                + "    row_time AS cast(CURRENT_TIMESTAMP as timestamp_LTZ(3)),\n"
                + "    WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n"
                + ") WITH (\n"
                + "  'connector' = 'datagen',\n"
                + "  'rows-per-second' = '1',\n"
                + "  'fields.id.min' = '1',\n"
                + "  'fields.id.max' = '100000',\n"
                + "  'fields.money.min' = '1',\n"
                + "  'fields.money.max' = '100000'\n"
                + ")\n";

        tableEnv.executeSql(createTableSql);

        String querySql = "SELECT UNIX_TIMESTAMP(CAST(window_end AS STRING)) * 1000 as window_end, \n"
                + "      window_start, \n"
                + "      sum(money) as sum_money,\n"
                + "      count(distinct id) as count_distinct_id\n"
                + "FROM TABLE(CUMULATE(\n"
                + "         TABLE source_table\n"
                + "         , DESCRIPTOR(row_time)\n"
                + "         , INTERVAL '5' SECOND\n"
                + "         , INTERVAL '1' DAY))\n"
                + "GROUP BY window_start, \n"
                + "        window_end";

        Table table = tableEnv.sqlQuery(querySql);

        //table -> data stream
        tableEnv.toDataStream(table)
                .flatMap(new FlatMapFunction<Row, Object>() {
                    @Override
                    public void flatMap(Row row, Collector<Object> collector) throws Exception {
                        long l = Long.parseLong(String.valueOf(row.getField("sum_money")));

                        if (l > 10000L) {
                            log.info("==================>报警，超过 1w");
                        }
                    }
                });

        env.execute("convert Job");
    }
}
