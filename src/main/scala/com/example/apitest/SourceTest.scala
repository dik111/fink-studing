package com.example.apitest

import org.apache.flink.streaming.api.scala._

/**
 * 定义样例类，温度传感器
 * @param id
 * @param timestamp
 * @param temperature
 */
case class SensorReading(id:String,timestamp:Long,temperature:Double)

/**
 * source api 测试
 */
object SourceTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 从集合中读取数据
    val dataList = List(
        SensorReading("sensor_1", 1547718199, 35.8),
        SensorReading("sensor_6", 1547718201, 15.4),
        SensorReading("sensor_7", 1547718202, 6.7),
        SensorReading("sensor_10", 1547718205, 38.1)
    )
//    val stream1 =env.fromCollection(dataList)
    // 从文件中读取数据
    val inputPath = "src/main/resources/sensor.txt"
    val stream2 = env.readTextFile(inputPath)

    stream2.print()
    env.execute("source test")
  }
}
