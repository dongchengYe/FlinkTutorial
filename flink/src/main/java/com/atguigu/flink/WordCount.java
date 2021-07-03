package com.atguigu.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author ydc
 */
public class WordCount {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> dataSource = env.readTextFile("E:\\programs\\ideaworkspace\\FlinkTutorial\\Flink_Scala\\src\\main\\resources\\wordcount.txt");

        AggregateOperator<Tuple2<String, Integer>> sum = dataSource.flatMap(new MyFlatMapper())
                .groupBy(0)
                .sum(1);
        sum.print();
    }

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] values = value.split(" ");
            for (String word : values) {
                out.collect(new Tuple2<>(word,1));
            }
        }
    }
}
