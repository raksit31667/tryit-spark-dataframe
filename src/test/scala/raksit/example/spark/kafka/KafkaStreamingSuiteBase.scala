package raksit.example.spark.kafka

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.{BeforeAndAfterEach, Suite}
import org.testcontainers.containers.KafkaContainer
import pureconfig.{ConfigReader, ConfigSource}
import pureconfig.generic.auto._
import raksit.example.spark.config.KafkaConfiguration

trait KafkaStreamingSuiteBase extends BeforeAndAfterEach { this: Suite =>

  var kafkaContainer: KafkaContainer = _
  var bootstrapServers: String = _
  var producer: KafkaProducer[String, String] = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    kafkaContainer = new KafkaContainer()
    kafkaContainer.start()
    val properties: Properties = new Properties()
    bootstrapServers = kafkaContainer.getBootstrapServers.substring(12)
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    producer = new KafkaProducer[String, String](properties)
  }

  override def afterEach(): Unit = {
    producer.close()
    kafkaContainer.stop()
    super.afterEach()
  }

  def getKafkaParameters: Map[String, Object] = {
    val configuration: ConfigReader.Result[KafkaConfiguration] =
      ConfigSource.string(
        s"""
          {
            bootstrap-servers = "$bootstrapServers",
            group-id = "test"
          }
      """).load[KafkaConfiguration]

    configuration match {
      case Left(exception) =>
        exception.toList.foreach(println)
        throw new RuntimeException("Test configuration is missing...")

      case Right(configuration) =>
        Map[String, Object](
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> configuration.bootstrapServers,
          ConsumerConfig.GROUP_ID_CONFIG -> configuration.groupId,
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
          ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
        )
    }
  }
}
