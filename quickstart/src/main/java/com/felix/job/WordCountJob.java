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

package com.felix.job;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.util.Collector;
import com.felix.entity.WC;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 */
public class WordCountJob {

    public static void main(String[] args) throws Exception {
        dataSetDemo();
    }

    private static void dataSetDemo() throws Exception {
        //执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> dataSet = env.fromElements(
                "Flink Spark Storm", "Flink Flink Flink", "Spark Spark Spark", "Storm Storm Storm"
        );
        DataSet<Tuple2<String, Long>> sumSet = dataSet.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                for (String word : s.toLowerCase().split("\\W+")) {
                    if (word.length() > 0) {
                        collector.collect(new Tuple2<>(word, 1L));
                    }
                }
            }
        }).groupBy(0).sum(1);
        sumSet.printToErr();
        env.execute("job dataSetDemo");
    }

    private static void dataStreamSocketDemo() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //linux环境监听端口 nc -lk 8000
        DataStream<String> dataStream = env.socketTextStream("192.168.159.111", 8000);
        DataStream<WC> sumStream = dataStream.flatMap(new FlatMapFunction<String, WC>() {
                    @Override
                    public void flatMap(String s, Collector<WC> collector) throws Exception {
                        for (String word : s.toLowerCase().split("\\W+")) {
                            if (word.length() > 0) {
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
        env.execute("job dataStreamSocketDemo");
    }

    private static void flinkSqlDemo() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String words = "hello flink hello word";
        List<WC> list = Arrays.stream(words.split("\\W+")).map(x -> new WC(x, 1))
                .collect(Collectors.toList());
        DataSource<WC> dataSource = env.fromCollection(list);

        BatchTableEnvironment bEnv = BatchTableEnvironment.create(env);
        Table table = bEnv.fromDataSet(dataSource, "word,cnt");
        table.printSchema();
        //注册为一张表
        bEnv.createTemporaryView("words", table);
        //执行sql
        Table resultTable = bEnv.sqlQuery("select word, sum(cnt) as cnt from words group by word");
        //将表转换为DataSet
        DataSet<WC> resultSet = bEnv.toDataSet(resultTable, WC.class);
        try {
            resultSet.printToErr();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
