package com.felix.job;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

public class MyDimensionTableJob {

    public static void main(String[] args) throws Exception {

    }

    public static class AsyncFunction extends RichAsyncFunction<String, String> {

        @Override
        public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {

        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }
    }
}
