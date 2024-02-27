package com.felix.job;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import com.felix.source.MyStreamingSource;

public class MySqlJob {

    private static final OutputTag<MyStreamingSource.Item> evenTag = new OutputTag<>("even", TypeInformation.of(MyStreamingSource.Item.class));
    private static final OutputTag<MyStreamingSource.Item> oddTag = new OutputTag<>("odd", TypeInformation.of(MyStreamingSource.Item.class));

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<MyStreamingSource.Item> dataStream = env.addSource(new MyStreamingSource());
        //分成两个流 even odd
        SingleOutputStreamOperator<MyStreamingSource.Item> splitStream = dataStream.process(new ProcessFunction<MyStreamingSource.Item, MyStreamingSource.Item>() {
            @Override
            public void processElement(MyStreamingSource.Item value, ProcessFunction<MyStreamingSource.Item, MyStreamingSource.Item>.Context ctx, Collector<MyStreamingSource.Item> out) throws Exception {
                OutputTag<MyStreamingSource.Item> tag = value.getItemId() % 2 == 0 ? evenTag : oddTag;
                ctx.output(tag, value);
            }
        });
        DataStream<MyStreamingSource.Item> evenStream = splitStream.getSideOutput(evenTag);
        DataStream<MyStreamingSource.Item> oddStream = splitStream.getSideOutput(oddTag);
        //创建表
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.createTemporaryView("evenTable", evenStream, "itemId,itemType,itemName");
        tEnv.createTemporaryView("oddTable", oddStream, "itemId,itemType,itemName");

        Table table = tEnv.sqlQuery("select a.itemId,a.itemType,a.itemName,b.itemId,b.itemName from evenTable as a join oddTable as b on a.itemType=b.itemType");

        table.printSchema();

        tEnv.toRetractStream(table, TypeInformation.of(new TypeHint<Tuple5<Integer, String, String, Integer, String>>() {
                }))
                .print();

        env.execute("job sqlJoin");


    }
}
