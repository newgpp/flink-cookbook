package com.felix.job;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class MyCacheJob {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //可以是本地文件也可以是HDFS
        String cachePath = "F:\\tmp\\cache.txt";
        String cacheName = "myCache";

        env.registerCachedFile(cachePath, cacheName);
        DataSet<String> dataSet = env.fromElements("1", "2", "3", "4", "5", "6");
        dataSet.map(new RichMapFunction<String, String>() {

            final List<String> cacheList = new ArrayList<>();

            @Override
            public String map(String s) throws Exception {
                //使用分布式缓存
                return s + cacheList.get(0);
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                File file = getRuntimeContext().getDistributedCache().getFile(cacheName);
                List<String> lines = FileUtils.readLines(file);
                this.cacheList.addAll(lines);

            }
        }).print();

        env.execute("job Cache");
    }
}
