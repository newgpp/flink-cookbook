package com.felix.job;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class MyWindowFunctionJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Long>> source = env.addSource(new MySourceFunction());

        source.keyBy(0)
                .timeWindow(Time.seconds(5))
                .aggregate(new AverageAggregate())
                .print();

        env.execute("windowFunctionJob");
    }

    public static class MySourceFunction implements SourceFunction<Tuple2<String, Long>> {

        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
            List<Tuple2<String, Long>> input = new ArrayList<>();
            input.add(new Tuple2<>("张三", 90L));
            input.add(new Tuple2<>("李四", 90L));
            input.add(new Tuple2<>("张三", 80L));
            input.add(new Tuple2<>("李四", 85L));
            input.add(new Tuple2<>("张三", 100L));
            input.add(new Tuple2<>("王五", 100L));
            input.add(new Tuple2<>("王五", 100L));
            input.add(new Tuple2<>("王五", 100L));
            while (isRunning) {
                int i = new Random().nextInt(8);
                ctx.collect(input.get(i));
                TimeUnit.SECONDS.sleep(1L);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    /**
     * 计算 [学生姓名, 学生成绩] 总成绩
     */
    public static class SumReduce implements ReduceFunction<Tuple2<String, Long>> {

        @Override
        public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
            return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
        }
    }

    /**
     * 计算 [学生姓名, 学生成绩] 平均成绩
     */
    public static class AverageAggregate implements AggregateFunction<Tuple2<String, Long>, Tuple3<String, Long, Long>, Tuple4<String, Long, Long, Double>> {


        @Override
        public Tuple3<String, Long, Long> createAccumulator() {
            System.out.println("createAccumulator");
            return new Tuple3<>("", 0L, 0L);
        }

        @Override
        public Tuple3<String, Long, Long> add(Tuple2<String, Long> value, Tuple3<String, Long, Long> accumulator) {
            System.out.println("add value: + " + value + ", accumulator: " + accumulator);
            return new Tuple3<>(value.f0, accumulator.f1 + value.f1, accumulator.f2 + 1);
        }

        @Override
        public Tuple4<String, Long, Long, Double> getResult(Tuple3<String, Long, Long> accumulator) {
            System.out.println("getResult accumulator: + " + accumulator);
            return new Tuple4<>(accumulator.f0, accumulator.f1, accumulator.f2, (double) accumulator.f1 / accumulator.f2);
        }

        //合并不同分组的聚合结果。当数据在多个分区或任务上进行并行计算时，merge方法会被调用，以合并来自不同分区或任务的聚合结果
        @Override
        public Tuple3<String, Long, Long> merge(Tuple3<String, Long, Long> a, Tuple3<String, Long, Long> b) {
            System.out.println("merge a: + " + a + ", b: " + b);
            return new Tuple3<>(a.f0, a.f1 + b.f1, a.f2 + b.f2);
        }
    }

}
