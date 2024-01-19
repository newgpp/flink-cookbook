package com.felix.job;

import com.felix.datatypes.TaxiRide;
import com.felix.sources.TaxiRideGenerator;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class LongRidesExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStream<TaxiRide> rides = env.addSource(new TaxiRideGenerator());

        WatermarkStrategy<TaxiRide> strategy = WatermarkStrategy.<TaxiRide>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                .withTimestampAssigner(new SerializableTimestampAssigner<TaxiRide>() {
                    @Override
                    public long extractTimestamp(TaxiRide ride, long recordTimestamp) {
                        return ride.getEventTimeMillis();
                    }
                });

        DataStreamSink<Long> longDataStreamSink = rides.assignTimestampsAndWatermarks(strategy)
                .keyBy(ride -> ride.rideId)
                .process(new AlertFunction())
                .addSink(new PrintSinkFunction<>());

        env.execute("Long taxi rides");


    }

    @VisibleForTesting
    public static class AlertFunction extends KeyedProcessFunction<Long, TaxiRide, Long> {

        @Override
        public void open(Configuration config) throws Exception {
            System.out.println("open");
        }

        @Override
        public void processElement(TaxiRide ride, Context context, Collector<Long> out)
                throws Exception {
            System.out.println(ride);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<Long> out)
                throws Exception {
            System.out.println("ontimer");
        }
    }


}
