package org.myorg.quickstart;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class WordCountSqlJob {

    public static void main(String[] args) {

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
