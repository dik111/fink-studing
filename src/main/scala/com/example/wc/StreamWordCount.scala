package com.example.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
 * 流处理wordcount
 */
object StreamWordCount {

  def main(args: Array[String]): Unit = {

    // 创建流处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 从外部命令中提取参数，作为socket主机名和端口号
    val parameterTool = ParameterTool.fromArgs(args)
    val host = parameterTool.get("host")
    val port = parameterTool.getInt("port")

    // 接收一个socket文本流 (nc -lk 7777)
    val inputDataStream = env.socketTextStream(host, port)

    // 进行转化处理统计
    val result = inputDataStream.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)

    result.print().setParallelism(1)

    // 启动任务执行
    env.execute("stream word count")
  }

}
