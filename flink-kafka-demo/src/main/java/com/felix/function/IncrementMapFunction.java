package com.felix.function;

import org.apache.flink.api.common.functions.MapFunction;

public class IncrementMapFunction implements MapFunction<Long, Long> {

    @Override
    public Long map(Long value) throws Exception {
        return value + 1;
    }
}
