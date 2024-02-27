package com.felix.job;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * CEP
 */
public class MyPatternStreamJob {

    public static void main(String[] args) throws Exception {
        consecutiveDemo();
    }

    /**
     * 帽子-1 鞋子-1 鞋子-2 鞋子-3
     *
     * followBy 匹配到就不在继续
     *
     * followByAny 匹配到仍然继续
     *
     */
    private static void consecutiveDemo() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        List<Tuple3<String, String, Integer>> list = new ArrayList<>();
        list.add(Tuple3.of("Marry", "外套", 1));
        list.add(Tuple3.of("Marry", "帽子", 1));
//        list.add(Tuple3.of("Alex", "帽子", 2));
//        list.add(Tuple3.of("Marry", "帽子", 2));
//        list.add(Tuple3.of("Alex", "帽子", 3));
        list.add(Tuple3.of("Marry", "衣服", 1));
        list.add(Tuple3.of("Marry", "鞋子", 1));
//        list.add(Tuple3.of("Marry", "帽子", 3));
//        list.add(Tuple3.of("Alex", "帽子", 1));
        list.add(Tuple3.of("Marry", "鞋子", 2));
        list.add(Tuple3.of("Marry", "鞋子", 3));
//        list.add(Tuple3.of("Marry", "帽子", 4));

        Pattern<Tuple3<String, String, Integer>, Tuple3<String, String, Integer>> pattern = Pattern
                .<Tuple3<String, String, Integer>>begin("start")
                .where(new SimpleCondition<Tuple3<String, String, Integer>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, Integer> value) throws Exception {
                        return value.f1.equals("帽子");
                    }
                })
                .followedByAny("middle")
                .where(new SimpleCondition<Tuple3<String, String, Integer>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, Integer> value) throws Exception {
                        return value.f1.equals("鞋子");
                    }
                }).times(2).consecutive();
        DataStream<Tuple3<String, String, Integer>> source = env.fromCollection(list);


        KeyedStream<Tuple3<String, String, Integer>, Tuple> keyedStream = source.keyBy(0);

        PatternStream<Tuple3<String, String, Integer>> patternStream = CEP.pattern(keyedStream, pattern);

        SingleOutputStreamOperator<String> matchStream = patternStream.select(new PatternSelectFunction<Tuple3<String, String, Integer>, String>() {
            @Override
            public String select(Map<String, List<Tuple3<String, String, Integer>>> pattern) throws Exception {
                List<Tuple3<String, String, Integer>> middle = pattern.get("middle");

                return middle.get(0).f0 + ":" + middle.get(0).f1 + ":" + middle.get(0).f2 + ":" + "一次帽子 两次鞋子！";
            }
        });

        matchStream.printToErr();

        env.execute("consecutiveDemo Job");
    }



    /**
     * 一个用户连续搜索两次帽子的事件：严格的连续匹配
     */
    private static void myPatternNextDemo() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        List<Tuple3<String, String, Integer>> list = new ArrayList<>();
        list.add(Tuple3.of("Marry", "外套", 1));
        list.add(Tuple3.of("Marry", "帽子", 1));
        list.add(Tuple3.of("Marry", "帽子", 2));
        list.add(Tuple3.of("Marry", "帽子", 3));
        list.add(Tuple3.of("Marry", "衣服", 1));
        list.add(Tuple3.of("Marry", "帽子", 4));
        list.add(Tuple3.of("Marry", "鞋子", 1));
        list.add(Tuple3.of("Marry", "鞋子", 2));
        list.add(Tuple3.of("Alex", "帽子", 1));
        list.add(Tuple3.of("Alex", "帽子", 2));
        list.add(Tuple3.of("Alex", "帽子", 3));

        DataStream<Tuple3<String, String, Integer>> source = env.fromCollection(list);

        //寻找连续搜索帽子的用户
        Pattern<Tuple3<String, String, Integer>, Tuple3<String, String, Integer>> pattern = Pattern
                .<Tuple3<String, String, Integer>>begin("start")
                .where(new SimpleCondition<Tuple3<String, String, Integer>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, Integer> value) throws Exception {
                        return value.f1.equals("帽子");
                    }
                })
                .next("middle")
                .where(new SimpleCondition<Tuple3<String, String, Integer>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, Integer> value) throws Exception {
                        return value.f1.equals("帽子");
                    }
                });

        KeyedStream<Tuple3<String, String, Integer>, Tuple> keyedStream = source.keyBy(0);

        PatternStream<Tuple3<String, String, Integer>> patternStream = CEP.pattern(keyedStream, pattern);

        SingleOutputStreamOperator<String> matchStream = patternStream.select(new PatternSelectFunction<Tuple3<String, String, Integer>, String>() {
            @Override
            public String select(Map<String, List<Tuple3<String, String, Integer>>> pattern) throws Exception {
                List<Tuple3<String, String, Integer>> middle = pattern.get("middle");

                return middle.get(0).f0 + ":" + middle.get(0).f2 + ":" + "连续两次搜索帽子！";
            }
        });

        matchStream.printToErr();

        env.execute("myPatternStreamDemo Job");
    }
}
