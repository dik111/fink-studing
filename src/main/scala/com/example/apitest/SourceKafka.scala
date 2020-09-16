package com.example.apitest
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * 从kafka中读取数据
 */
object SourceKafka {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers","10.10.19.233:9092")
    properties.setProperty("group.id","consumer-group")

    val stream3 = env.addSource(new FlinkKafkaConsumer011[String]("example",new SimpleStringSchema(),properties))
    stream3.print()

    env.execute("source test kafka")
  }
}
