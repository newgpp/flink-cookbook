package com.felix.job;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class MyStateJob {

    public static void main(String[] args) throws Exception {
        stateJobDemo();
    }

    private static void stateJobDemo() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new MemoryStateBackend(MemoryStateBackend.DEFAULT_MAX_STATE_SIZE, false));
        env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 5L), Tuple2.of(1L, 2L))
                .keyBy(0)
                .flatMap(new MyFlatMap())
                .printToErr();

        env.execute("myStateJob");
    }

    /**
     * 每当第一个元素的和达到2，就把第二个元素的和和第一个元素的和相除，最后输出
     */
    public static class MyFlatMap extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

        private transient ValueState<Tuple2<Long, Long>> sum;

        @Override
        public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {
            Tuple2<Long, Long> currentSum;
            // 访问ValueState
            if(sum.value()==null){
                currentSum = Tuple2.of(0L, 0L);
            }else {
                currentSum = sum.value();
            }
            // 更新
            currentSum.f0 += 1;
            // 第二个元素加1
            currentSum.f1 += input.f1;
            // 更新state
            sum.update(currentSum);

            // 如果count的值大于等于2，求知道并清空state
            if (currentSum.f0 >= 2) {
                out.collect(new Tuple2<>(input.f0, currentSum.f1 / currentSum.f0));
                sum.clear();
            }

        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                    new ValueStateDescriptor<>(
                            "average", // state的名字
                            TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {})
                    );
            // 设置默认值
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.seconds(10))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();
            descriptor.enableTimeToLive(ttlConfig);

            sum = getRuntimeContext().getState(descriptor);
        }
    }
}
