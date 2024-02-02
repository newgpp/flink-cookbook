package com.felix.job;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;

public class MyTimeJob {

    public static void main(String[] args) throws Exception {
        waterMarkDemo();
    }

    /**
     * 实时接收Socket的DataStream程序
     * 代码中使用AssignerWithPeriodicWatermarks来设置水印
     * 将接收的数据进行转换，分组，并在一个5s的窗口内，获取该窗口中第二个元素最小的那条数据
     *
     * 实验数据:
     * flink,1588659181000
     * flink,1588659182000
     * flink,1588659183000
     * flink,1588659184000
     * flink,1588659185000
     *
     *
     *
     *
     */
    private static void waterMarkDemo() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置为EventTime事件
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置水印生成事件间隔100ms
        env.getConfig().setAutoWatermarkInterval(100);

        //linux环境监听端口 nc -lk 8000
        env.socketTextStream("192.168.159.111", 8000)
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {

                    private Long currentTimeStamp = 0L;
                    private Long maxOutOfOrderness = 5000L;

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(currentTimeStamp - maxOutOfOrderness);
                    }

                    @Override
                    public long extractTimestamp(String s, long previousElementTimestamp) {
                        String[] arr = s.split(",");
                        long timeStamp = Long.parseLong(arr[1]);
                        currentTimeStamp = Math.max(timeStamp, currentTimeStamp);
//                        System.err.println(s + "EventTime:" + timeStamp + ",watermark:" + (currentTimeStamp - maxOutOfOrderness));
                        return timeStamp;
                    }
                })
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        String[] split = s.split(",");

                        return new Tuple2<>(split[0], Long.parseLong(split[1]));
                    }
                }).keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .minBy(1)
                .print();

        env.execute("WaterMark Job");


    }
}
