package com.felix.job;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;

/**
 * 旁路分流器
 */
public class MySideOutPutJob {

    public static void main(String[] args) throws Exception {
        sideOutPutDemo();
    }

    private static void sideOutPutDemo() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Tuple3<Integer, Integer, Integer>> dataList = new ArrayList<>();
        dataList.add(new Tuple3<>(1, 2, 3));
        dataList.add(new Tuple3<>(1, 3, 4));
        dataList.add(new Tuple3<>(1, 4, 5));
        dataList.add(new Tuple3<>(2, 1, 3));
        dataList.add(new Tuple3<>(2, 2, 3));
        dataList.add(new Tuple3<>(2, 3, 3));
        dataList.add(new Tuple3<>(3, 1, 3));
        dataList.add(new Tuple3<>(3, 2, 3));
        dataList.add(new Tuple3<>(4, 6, 3));
        dataList.add(new Tuple3<>(4, 2, 3));


        DataStream<Tuple3<Integer, Integer, Integer>> source = env.fromCollection(dataList);

        // https://www.jianshu.com/p/871db08f169d
        TypeInformation<Tuple3<Integer, Integer, Integer>> t3Int = new TypeHint<Tuple3<Integer, Integer, Integer>>() {
        }.getTypeInfo();

        OutputTag<Tuple3<Integer, Integer, Integer>> tag1 = new OutputTag<>("tag1", t3Int);
        OutputTag<Tuple3<Integer, Integer, Integer>> tag2 = new OutputTag<>("tag2", t3Int);
        OutputTag<Tuple3<Integer, Integer, Integer>> tag3 = new OutputTag<>("tag3", t3Int);
        OutputTag<Tuple3<Integer, Integer, Integer>> tag4 = new OutputTag<>("tag4", t3Int);

        SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> sideOutPut = source.process(new ProcessFunction<Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {

            @Override
            public void processElement(Tuple3<Integer, Integer, Integer> value, ProcessFunction<Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>>.Context ctx, Collector<Tuple3<Integer, Integer, Integer>> out) throws Exception {
                if (value.f0 == 1) {
                    ctx.output(tag1, value);
                } else if (value.f0 == 2) {
                    ctx.output(tag2, value);
                } else if (value.f0 == 3) {
                    ctx.output(tag3, value);
                } else if (value.f0 == 4) {
                    ctx.output(tag4, value);
                }
            }
        });

        sideOutPut.getSideOutput(tag1).printToErr();

        sideOutPut.getSideOutput(tag4).print();

        env.execute("myOutPutDemo job");

    }
}
