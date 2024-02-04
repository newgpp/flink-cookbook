package com.felix.job;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;

/**
 * @ EventTimeTrigger
 * window的触发条件
 * watermark时间 >= window_end_time
 * [window_start_time, window_end_time)
 * <p>
 * https://zhuanlan.zhihu.com/p/126095557
 */
public class MyTimeJob {

    public static void main(String[] args) throws Exception {
        waterMarkJob1();
    }

    private static void waterMarkJob1() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(200);
        SingleOutputStreamOperator<String> dataStream = env.socketTextStream("192.168.159.111", 8000)
                .assignTimestampsAndWatermarks(new MyWatermark());

        dataStream.map(new MapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(String s) throws Exception {
                        return new Tuple2<String, String>(s.split(",")[0], s.split(",")[1]);
                    }
                })
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> reduce(Tuple2<String, String> value1, Tuple2<String, String> value2) throws Exception {
                        return new Tuple2<>(value1.f0, value1.f1 + "-" + value2.f1);
                    }
                })
                .print();

        env.execute("waterMarkJob1");
    }

    public static class MyWatermark implements AssignerWithPeriodicWatermarks<String> {

        // 当前时间戳
        long currentTimeStamp = 0L;
        // 允许的迟到数据
        long maxDelayAllowed = 3000L;
        // 当前水位线
        long currentWaterMark;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            currentWaterMark = currentTimeStamp - maxDelayAllowed;
            System.out.println("当前水位线:" + currentWaterMark);
            return new Watermark(currentWaterMark);
        }

        @Override
        public long extractTimestamp(String s, long l) {
            String[] arr = s.split(",");
            long timeStamp = Long.parseLong(arr[1]);
            currentTimeStamp = Math.max(timeStamp, currentTimeStamp);
            System.out.println("Key:" + arr[0] + ",EventTime:" + timeStamp + ",前一条数据的水位线:" + currentWaterMark);
            return timeStamp;
        }
    }
}
