package com.example.wc

import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}

/**
 * 批处理的wordcount
 */
object WordCount {

  def main(args: Array[String]): Unit = {

    // 创建一个批处理的执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 从文件中读取数据
    val inputPath = "/Users/yuwei1/Documents/scala/project/flink-studing/src/main/resources/hello.txt"
    val inputDataSet = env.readTextFile(inputPath)

    // 对数据进行转换处理统计，先分词，再按照word进行分组，最后进行聚合统计
    val resultDataSet = inputDataSet.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)
    resultDataSet.print()

  }
}
