package raksit.example.spark.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.testcontainers.containers.KafkaContainer
import pureconfig.{ConfigReader, ConfigSource}
import pureconfig.generic.auto._
import raksit.example.spark.InitSpark
import raksit.example.spark.config.KafkaConfiguration

class WordCountStreamingAppTest extends FunSuite with BeforeAndAfterEach with InitSpark {

  var kafkaContainer: KafkaContainer = _
  var bootstrapServers: String = _
  var producer: KafkaProducer[String, String] = _

  override protected def beforeEach(): Unit = {
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

  override protected def afterEach(): Unit = {
    producer.close()
    kafkaContainer.stop()
    super.afterEach()
  }

  test("should stream word counting successfully") {
    // When
    val streamingContext = WordCountStreamingApp.getContext(getConfiguration)
    val record = new ProducerRecord[String, String]("wordcount", "1", "Hello world")
    producer.send(record)

    // Then
    streamingContext.start()
    streamingContext.awaitTerminationOrTimeout(6000)
  }

  private def getConfiguration: KafkaConfiguration = {
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
        configuration
    }
  }
}
