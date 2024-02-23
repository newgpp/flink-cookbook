package com.felix.job;

import com.felix.entity.UserClick;
import org.apache.commons.codec.digest.MurmurHash2;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * PV（Page View，网站的浏览量）即页面的浏览次数，PV指页面的刷新次数
 * UV（Unique Visitor，独立访客次数）是一天内访问某个站点的人数，一天内同一个用户访问多次网站仅记录一次，一般通过IP或者Cookie来判断
 */
public class MyPvUvJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<UserClick> source = env.addSource(new MyClickSource());
        DataStream<UserClick> stream = source.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserClick>(Time.minutes(60)) {
            @Override
            public long extractTimestamp(UserClick element) {
                return element.getTs();
            }
        });
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> pvStream = stream.keyBy(new KeySelector<UserClick, String>() {
                    @Override
                    public String getKey(UserClick value) throws Exception {
                        return DateFormatUtils.format(value.getTs(), "yyyy-MM-dd");
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(15)))
                //窗口计算完成后 剔除窗口中每条数据
                .evictor(TimeEvictor.of(Time.seconds(0), true))
                .process(new MyProcessWindowFunction());
        pvStream.print();
        env.execute("myPV job");
    }

    public static class MyProcessWindowFunction extends ProcessWindowFunction<UserClick, Tuple3<String, String, Integer>, String, TimeWindow> {

        private transient ValueState<Integer> pvState;
        private transient MapState<String, String> uvState;
        private transient ValueState<Roaring64NavigableMap> uvBitMapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            pvState = this.getRuntimeContext().getState(new ValueStateDescriptor<>("pv", Integer.class));
            uvState = this.getRuntimeContext().getMapState(new MapStateDescriptor<>("uv", String.class, String.class));
            uvBitMapState = this.getRuntimeContext().getState(new ValueStateDescriptor<>("uvBitMap", TypeInformation.of(new TypeHint<Roaring64NavigableMap>() {
            })));
        }

        @Override
        public void process(String s, Context context, Iterable<UserClick> elements, Collector<Tuple3<String, String, Integer>> out) throws Exception {
            Roaring64NavigableMap uvBitMap = uvBitMapState.value();
            if (uvBitMap == null) {
                uvBitMap = new Roaring64NavigableMap();
            }
            int pv = 0;
            for (UserClick ignored : elements) {
                pv = pv + 1;
                uvState.put(ignored.getUserId(), null);
                uvBitMap.add(MurmurHash2.hash64(ignored.getUserId()));
            }
            int value = pvState.value() == null ? 0 : pvState.value();
            pvState.update(value + pv);

            uvBitMapState.update(uvBitMap);
            int uv = 0;
            for (String ignored : uvState.keys()) {
                uv = uv + 1;
            }

            out.collect(Tuple3.of(s, "pv", pvState.value()));
            out.collect(Tuple3.of(s, "uv", uv));
            out.collect(Tuple3.of(s, "uv2", uvBitMap.getIntCardinality()));
            System.out.println("=========>processWindow, this hasCode:" + this.hashCode() + ", pvState hashCode:" + pvState.hashCode());
        }
    }

    private static class MyClickSource extends RichSourceFunction<UserClick> {

        private boolean isRunning = true;

        private List<UserClick> logs = new ArrayList<>();

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            logs = new ObjectMapper().readValue("[{\"userId\":\"84\",\"ts\":1708531200000},\n" +
                    "{\"userId\":\"5\",\"ts\":1708531800000},\n" +
                    "{\"userId\":\"36\",\"ts\":1708532400000},\n" +
                    "{\"userId\":\"82\",\"ts\":1708533000000},\n" +
                    "{\"userId\":\"76\",\"ts\":1708533600000},\n" +
                    "{\"userId\":\"26\",\"ts\":1708534200000},\n" +
                    "{\"userId\":\"79\",\"ts\":1708534800000},\n" +
                    "{\"userId\":\"26\",\"ts\":1708535400000},\n" +
                    "{\"userId\":\"27\",\"ts\":1708536000000},\n" +
                    "{\"userId\":\"28\",\"ts\":1708536600000},\n" +
                    "{\"userId\":\"11\",\"ts\":1708537200000},\n" +
                    "{\"userId\":\"44\",\"ts\":1708537800000},\n" +
                    "{\"userId\":\"61\",\"ts\":1708538400000},\n" +
                    "{\"userId\":\"47\",\"ts\":1708539000000},\n" +
                    "{\"userId\":\"82\",\"ts\":1708539600000},\n" +
                    "{\"userId\":\"65\",\"ts\":1708540200000},\n" +
                    "{\"userId\":\"43\",\"ts\":1708540800000},\n" +
                    "{\"userId\":\"23\",\"ts\":1708541400000},\n" +
                    "{\"userId\":\"62\",\"ts\":1708542000000},\n" +
                    "{\"userId\":\"34\",\"ts\":1708542600000},\n" +
                    "{\"userId\":\"24\",\"ts\":1708543200000},\n" +
                    "{\"userId\":\"79\",\"ts\":1708543800000},\n" +
                    "{\"userId\":\"34\",\"ts\":1708544400000},\n" +
                    "{\"userId\":\"7\",\"ts\":1708545000000},\n" +
                    "{\"userId\":\"22\",\"ts\":1708545600000},\n" +
                    "{\"userId\":\"44\",\"ts\":1708546200000},\n" +
                    "{\"userId\":\"9\",\"ts\":1708546800000},\n" +
                    "{\"userId\":\"7\",\"ts\":1708547400000},\n" +
                    "{\"userId\":\"57\",\"ts\":1708548000000},\n" +
                    "{\"userId\":\"71\",\"ts\":1708548600000},\n" +
                    "{\"userId\":\"1\",\"ts\":1708549200000},\n" +
                    "{\"userId\":\"100\",\"ts\":1708549800000},\n" +
                    "{\"userId\":\"25\",\"ts\":1708550400000},\n" +
                    "{\"userId\":\"59\",\"ts\":1708551000000},\n" +
                    "{\"userId\":\"20\",\"ts\":1708551600000},\n" +
                    "{\"userId\":\"77\",\"ts\":1708552200000},\n" +
                    "{\"userId\":\"21\",\"ts\":1708552800000},\n" +
                    "{\"userId\":\"78\",\"ts\":1708553400000},\n" +
                    "{\"userId\":\"7\",\"ts\":1708554000000},\n" +
                    "{\"userId\":\"100\",\"ts\":1708554600000},\n" +
                    "{\"userId\":\"83\",\"ts\":1708555200000},\n" +
                    "{\"userId\":\"78\",\"ts\":1708555800000},\n" +
                    "{\"userId\":\"35\",\"ts\":1708556400000},\n" +
                    "{\"userId\":\"79\",\"ts\":1708557000000},\n" +
                    "{\"userId\":\"94\",\"ts\":1708557600000},\n" +
                    "{\"userId\":\"13\",\"ts\":1708558200000},\n" +
                    "{\"userId\":\"39\",\"ts\":1708558800000},\n" +
                    "{\"userId\":\"17\",\"ts\":1708559400000},\n" +
                    "{\"userId\":\"64\",\"ts\":1708560000000},\n" +
                    "{\"userId\":\"78\",\"ts\":1708560600000},\n" +
                    "{\"userId\":\"25\",\"ts\":1708561200000},\n" +
                    "{\"userId\":\"66\",\"ts\":1708561800000},\n" +
                    "{\"userId\":\"13\",\"ts\":1708562400000},\n" +
                    "{\"userId\":\"24\",\"ts\":1708563000000},\n" +
                    "{\"userId\":\"21\",\"ts\":1708563600000},\n" +
                    "{\"userId\":\"65\",\"ts\":1708564200000},\n" +
                    "{\"userId\":\"46\",\"ts\":1708564800000},\n" +
                    "{\"userId\":\"78\",\"ts\":1708565400000},\n" +
                    "{\"userId\":\"39\",\"ts\":1708566000000},\n" +
                    "{\"userId\":\"3\",\"ts\":1708566600000},\n" +
                    "{\"userId\":\"83\",\"ts\":1708567200000},\n" +
                    "{\"userId\":\"32\",\"ts\":1708567800000},\n" +
                    "{\"userId\":\"53\",\"ts\":1708568400000},\n" +
                    "{\"userId\":\"23\",\"ts\":1708569000000},\n" +
                    "{\"userId\":\"22\",\"ts\":1708569600000},\n" +
                    "{\"userId\":\"45\",\"ts\":1708570200000},\n" +
                    "{\"userId\":\"99\",\"ts\":1708570800000},\n" +
                    "{\"userId\":\"80\",\"ts\":1708571400000},\n" +
                    "{\"userId\":\"38\",\"ts\":1708572000000},\n" +
                    "{\"userId\":\"18\",\"ts\":1708572600000},\n" +
                    "{\"userId\":\"69\",\"ts\":1708573200000},\n" +
                    "{\"userId\":\"32\",\"ts\":1708573800000},\n" +
                    "{\"userId\":\"74\",\"ts\":1708574400000},\n" +
                    "{\"userId\":\"88\",\"ts\":1708575000000},\n" +
                    "{\"userId\":\"54\",\"ts\":1708575600000},\n" +
                    "{\"userId\":\"79\",\"ts\":1708576200000},\n" +
                    "{\"userId\":\"42\",\"ts\":1708576800000},\n" +
                    "{\"userId\":\"5\",\"ts\":1708577400000},\n" +
                    "{\"userId\":\"33\",\"ts\":1708578000000},\n" +
                    "{\"userId\":\"47\",\"ts\":1708578600000},\n" +
                    "{\"userId\":\"100\",\"ts\":1708579200000},\n" +
                    "{\"userId\":\"22\",\"ts\":1708579800000},\n" +
                    "{\"userId\":\"9\",\"ts\":1708580400000},\n" +
                    "{\"userId\":\"28\",\"ts\":1708581000000},\n" +
                    "{\"userId\":\"19\",\"ts\":1708581600000},\n" +
                    "{\"userId\":\"56\",\"ts\":1708582200000},\n" +
                    "{\"userId\":\"60\",\"ts\":1708582800000},\n" +
                    "{\"userId\":\"46\",\"ts\":1708583400000},\n" +
                    "{\"userId\":\"41\",\"ts\":1708584000000},\n" +
                    "{\"userId\":\"8\",\"ts\":1708584600000},\n" +
                    "{\"userId\":\"93\",\"ts\":1708585200000},\n" +
                    "{\"userId\":\"100\",\"ts\":1708585800000},\n" +
                    "{\"userId\":\"6\",\"ts\":1708586400000},\n" +
                    "{\"userId\":\"59\",\"ts\":1708587000000},\n" +
                    "{\"userId\":\"13\",\"ts\":1708587600000},\n" +
                    "{\"userId\":\"30\",\"ts\":1708588200000},\n" +
                    "{\"userId\":\"71\",\"ts\":1708588800000},\n" +
                    "{\"userId\":\"6\",\"ts\":1708589400000},\n" +
                    "{\"userId\":\"28\",\"ts\":1708590000000},\n" +
                    "{\"userId\":\"39\",\"ts\":1708590600000},\n" +
                    "{\"userId\":\"49\",\"ts\":1708591200000},\n" +
                    "{\"userId\":\"20\",\"ts\":1708591800000},\n" +
                    "{\"userId\":\"71\",\"ts\":1708592400000},\n" +
                    "{\"userId\":\"96\",\"ts\":1708593000000},\n" +
                    "{\"userId\":\"93\",\"ts\":1708593600000},\n" +
                    "{\"userId\":\"83\",\"ts\":1708594200000},\n" +
                    "{\"userId\":\"94\",\"ts\":1708594800000},\n" +
                    "{\"userId\":\"49\",\"ts\":1708595400000},\n" +
                    "{\"userId\":\"44\",\"ts\":1708596000000},\n" +
                    "{\"userId\":\"21\",\"ts\":1708596600000},\n" +
                    "{\"userId\":\"53\",\"ts\":1708597200000},\n" +
                    "{\"userId\":\"34\",\"ts\":1708597800000},\n" +
                    "{\"userId\":\"45\",\"ts\":1708598400000},\n" +
                    "{\"userId\":\"27\",\"ts\":1708599000000},\n" +
                    "{\"userId\":\"15\",\"ts\":1708599600000},\n" +
                    "{\"userId\":\"44\",\"ts\":1708600200000},\n" +
                    "{\"userId\":\"35\",\"ts\":1708600800000},\n" +
                    "{\"userId\":\"1\",\"ts\":1708601400000},\n" +
                    "{\"userId\":\"48\",\"ts\":1708602000000},\n" +
                    "{\"userId\":\"27\",\"ts\":1708602600000},\n" +
                    "{\"userId\":\"25\",\"ts\":1708603200000},\n" +
                    "{\"userId\":\"52\",\"ts\":1708603800000},\n" +
                    "{\"userId\":\"11\",\"ts\":1708604400000},\n" +
                    "{\"userId\":\"55\",\"ts\":1708605000000},\n" +
                    "{\"userId\":\"100\",\"ts\":1708605600000},\n" +
                    "{\"userId\":\"79\",\"ts\":1708606200000},\n" +
                    "{\"userId\":\"33\",\"ts\":1708606800000},\n" +
                    "{\"userId\":\"84\",\"ts\":1708607400000},\n" +
                    "{\"userId\":\"8\",\"ts\":1708608000000},\n" +
                    "{\"userId\":\"26\",\"ts\":1708608600000},\n" +
                    "{\"userId\":\"89\",\"ts\":1708609200000},\n" +
                    "{\"userId\":\"7\",\"ts\":1708609800000},\n" +
                    "{\"userId\":\"80\",\"ts\":1708610400000},\n" +
                    "{\"userId\":\"96\",\"ts\":1708611000000},\n" +
                    "{\"userId\":\"38\",\"ts\":1708611600000},\n" +
                    "{\"userId\":\"66\",\"ts\":1708612200000},\n" +
                    "{\"userId\":\"80\",\"ts\":1708612800000},\n" +
                    "{\"userId\":\"10\",\"ts\":1708613400000},\n" +
                    "{\"userId\":\"42\",\"ts\":1708614000000},\n" +
                    "{\"userId\":\"4\",\"ts\":1708614600000},\n" +
                    "{\"userId\":\"94\",\"ts\":1708615200000},\n" +
                    "{\"userId\":\"79\",\"ts\":1708615800000},\n" +
                    "{\"userId\":\"4\",\"ts\":1708616400000},\n" +
                    "{\"userId\":\"68\",\"ts\":1708617000000},\n" +
                    "{\"userId\":\"93\",\"ts\":1708617600000},\n" +
                    "{\"userId\":\"23\",\"ts\":1708618200000},\n" +
                    "{\"userId\":\"95\",\"ts\":1708618800000},\n" +
                    "{\"userId\":\"39\",\"ts\":1708619400000},\n" +
                    "{\"userId\":\"42\",\"ts\":1708620000000},\n" +
                    "{\"userId\":\"88\",\"ts\":1708620600000},\n" +
                    "{\"userId\":\"60\",\"ts\":1708621200000},\n" +
                    "{\"userId\":\"8\",\"ts\":1708621800000},\n" +
                    "{\"userId\":\"66\",\"ts\":1708622400000},\n" +
                    "{\"userId\":\"32\",\"ts\":1708623000000},\n" +
                    "{\"userId\":\"41\",\"ts\":1708623600000},\n" +
                    "{\"userId\":\"16\",\"ts\":1708624200000},\n" +
                    "{\"userId\":\"55\",\"ts\":1708624800000},\n" +
                    "{\"userId\":\"29\",\"ts\":1708625400000},\n" +
                    "{\"userId\":\"65\",\"ts\":1708626000000},\n" +
                    "{\"userId\":\"86\",\"ts\":1708626600000},\n" +
                    "{\"userId\":\"33\",\"ts\":1708627200000},\n" +
                    "{\"userId\":\"10\",\"ts\":1708627800000},\n" +
                    "{\"userId\":\"20\",\"ts\":1708628400000},\n" +
                    "{\"userId\":\"30\",\"ts\":1708629000000},\n" +
                    "{\"userId\":\"80\",\"ts\":1708629600000},\n" +
                    "{\"userId\":\"22\",\"ts\":1708630200000},\n" +
                    "{\"userId\":\"86\",\"ts\":1708630800000},\n" +
                    "{\"userId\":\"68\",\"ts\":1708631400000}]", new TypeReference<List<UserClick>>() {
            });
        }

        @Override
        public void run(SourceContext<UserClick> ctx) throws Exception {
            for (int i = 0; i <= logs.size() && isRunning; i++) {
                if (i == logs.size()) {
                    i = 0;
                }
                ctx.collect(logs.get(i));
                TimeUnit.MILLISECONDS.sleep(500);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
