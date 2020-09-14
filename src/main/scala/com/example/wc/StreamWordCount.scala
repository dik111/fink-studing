package com.example.wc

import org.apache.flink.streaming.api.scala._

/**
 * 流处理wordcount
 */
object StreamWordCount {

  def main(args: Array[String]): Unit = {

    // 创建流处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 接收一个socket文本流 (nc -lk 7777)
    val inputDataStream = env.socketTextStream("localhost", 7777)

    // 进行转化处理统计
    val result = inputDataStream.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)

    result.print()

    // 启动任务执行
    env.execute("stream word count")
  }

}
