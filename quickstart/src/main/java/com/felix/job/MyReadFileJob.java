package com.felix.job;

import com.felix.entity.WaterGate;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class MyReadFileJob {

    public static void main(String[] args) throws Exception {
        readCsvDemo();
    }

    private static void readCsvDemo() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String path = MyReadFileJob.class.getClassLoader().getResource("water-gate.csv").getPath().replaceAll("%20", " ");
        String fields = "gateName,lon,lat,xzq,location,riverName,szks,zkjk,zjbgc,zmdgc,zdgc,pjgc,zmxs,qbj,qbjxs,djgl,sjbz";
        DataSet<WaterGate> dataSet = env.readCsvFile(path).pojoType(WaterGate.class, fields.split(","));
        dataSet.print();
        env.execute("readCsvDemo");
    }
}
