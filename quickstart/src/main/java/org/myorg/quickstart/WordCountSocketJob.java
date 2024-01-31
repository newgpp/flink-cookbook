/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.myorg.quickstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * https://github.com/will-che/flink-simple-tutorial
 *
 * linux环境监听端口 nc -lk 8000
 */
public class WordCountSocketJob {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //linux环境监听端口 nc -lk 8000
        DataStream<String> dataStream = env.socketTextStream("192.168.159.111", 8000);

        DataStream<WC> sumStream = dataStream.flatMap(new FlatMapFunction<String, WC>() {
            @Override
            public void flatMap(String s, Collector<WC> collector) throws Exception {
                for (String word : s.toLowerCase().split("\\W+")) {
                    if(word.length() > 0){
                        collector.collect(new WC(word, 1));
                    }
                }
            }
        })
        .keyBy("word")
        .timeWindow(Time.seconds(5), Time.seconds(1))
        .reduce(new ReduceFunction<WC>() {
            @Override
            public WC reduce(WC a, WC b) throws Exception {
                return new WC(a.getWord(), a.getCnt() + b.getCnt());
            }
        });

        sumStream.printToErr();

        env.execute("Flink Streaming Java API Skeleton");
    }
}
