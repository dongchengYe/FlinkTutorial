package com.atguigu.flinkscala

import org.apache.flink.api.scala._

object WordCount {

  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val inputPath = "E:\\programs\\ideaworkspace\\FlinkTutorial\\Flink_Scala\\src\\main\\resources\\wordcount.txt"
    val wordCountDataSet: DataSet[String] = env.readTextFile(inputPath)

    val aggDataSet: AggregateDataSet[(String, Int)] = wordCountDataSet.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)
    aggDataSet.print()
  }

}
