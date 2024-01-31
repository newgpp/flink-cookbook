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
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 *
 */
public class WordCountJob {

    public static void main(String[] args) throws Exception {
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

        env.execute("Flink Streaming Java API Skeleton");
    }
}
