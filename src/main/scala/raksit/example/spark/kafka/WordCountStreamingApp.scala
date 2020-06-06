package raksit.example.spark.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.TaskContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import raksit.example.spark.InitSpark
import raksit.example.spark.config.KafkaConfiguration


object WordCountStreamingApp extends InitSpark {

  def getContext(configuration: KafkaConfiguration): StreamingContext = {
    val streamingContext = new StreamingContext(sparkContext, Seconds(10))
    val topics = Set("wordcount")
    val kafkaParameters = getKafkaParameters(configuration)

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

    streamingContext
  }

  private def getKafkaParameters(configuration: KafkaConfiguration): Map[String, Object] = {
    Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> configuration.bootstrapServers,
      ConsumerConfig.GROUP_ID_CONFIG -> configuration.groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
    )
  }
}
