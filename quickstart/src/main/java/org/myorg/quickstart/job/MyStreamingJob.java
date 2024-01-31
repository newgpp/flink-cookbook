package org.myorg.quickstart.job;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.myorg.quickstart.source.MyStreamingSource;

import java.util.Arrays;
import java.util.List;

public class MyStreamingJob {

    public static void main(String[] args) throws Exception {
        reduceDemo();
    }

    private static void keyByDemo() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<MyStreamingSource.Item> source = env.addSource(new MyStreamingSource());
        source.print();
        DataStream<MyStreamingSource.Item> maxStream = source.keyBy("itemType")
                .timeWindow(Time.seconds(6), Time.seconds(3))
                .maxBy("itemId");

        maxStream.printToErr();

        env.execute("job keyByDemo");
    }

    private static void reduceDemo() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Tuple3<Integer, Integer, Integer>> list = Arrays.asList(
                new Tuple3<>(1, 2, 3),
                new Tuple3<>(2, 2, 2),
                new Tuple3<>(3, 3, 3),
                new Tuple3<>(2, 8, 7),
                new Tuple3<>(1, 0, 12)
        );

        DataStreamSource<Tuple3<Integer, Integer, Integer>> source = env.fromCollection(list);

        source.keyBy(0)
                .reduce(new ReduceFunction<Tuple3<Integer, Integer, Integer>>() {
                    @Override
                    public Tuple3<Integer, Integer, Integer> reduce(Tuple3<Integer, Integer, Integer> a, Tuple3<Integer, Integer, Integer> b) throws Exception {
                        return new Tuple3<>((Integer) a.getField(0), 0, (Integer) a.getField(2) + (Integer) b.getField(2));
                    }
                }).print();
        env.execute("job reduceDemo");
    }
}
