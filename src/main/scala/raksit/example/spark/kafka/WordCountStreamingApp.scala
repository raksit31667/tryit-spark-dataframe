package raksit.example.spark.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.TaskContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderFailures
import pureconfig.generic.auto._
import raksit.example.spark.InitSpark
import raksit.example.spark.config.Configuration

object WordCountStreamingApp extends InitSpark {
  def main(args: Array[String]): Unit = {
    val streamingContext = new StreamingContext(sparkContext, Seconds(10))
    val topics = Set("wordcount")
    val kafkaParameters = getKafkaParameters

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
    val wordCounts = WordCounter.count(text)

    wordCounts.print() // (Hello,1) (world,1)

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  private def getKafkaParameters: Map[String, Object] = {
    val configuration: Either[ConfigReaderFailures, Configuration] =
      ConfigSource.default.load[Configuration]

    configuration match {
      case Left(exception) =>
        exception.toList.foreach(println)
        throw new RuntimeException("Test configuration is missing...")

      case Right(configuration) =>
        Map[String, Object](
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> configuration.kafka.bootstrapServer,
          ConsumerConfig.GROUP_ID_CONFIG -> configuration.kafka.groupId,
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
        )
    }
  }
}
