package raksit.example.spark.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.TaskContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import raksit.example.spark.InitSpark

object WordCountStreamingApp extends InitSpark {
  def main(args: Array[String]): Unit = {
    val streamingContext = new StreamingContext(sparkContext, Seconds(10))
    val topics = Set("wordcount")
    val kafkaParameters = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "test",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    val messages = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParameters)
    )

    messages.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { _ =>
        val offsetRange = offsetRanges(TaskContext.getPartitionId())
        println(s"Kafka info: fromOffset=${offsetRange.fromOffset}, untilOffset=${offsetRange.untilOffset}")
      }
    }

    val text = messages.map(_.value())
    text.print() // Hello world
    val words = text.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_+_)

    wordCounts.print() // (Hello,1) (world,1)

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
