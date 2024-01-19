package com.felix.flink;

import com.felix.function.IncrementMapFunction;
import org.junit.Assert;
import org.junit.Test;

public class StreamJobTest {

    @Test
    public void incr_map_should_success() throws Exception {

        IncrementMapFunction incrMapFunction = new IncrementMapFunction();

        Long out = incrMapFunction.map(1L);

        Assert.assertEquals(2L, out.longValue());

    }
}
