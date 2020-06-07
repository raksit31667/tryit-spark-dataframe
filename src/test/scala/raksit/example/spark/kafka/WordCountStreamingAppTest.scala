package raksit.example.spark.kafka

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.FunSuite

class WordCountStreamingAppTest extends FunSuite with KafkaStreamingSuiteBase with SharedSparkContext {

  var streamingContext: StreamingContext = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    streamingContext = new StreamingContext(sc, Seconds(1))
  }

  override def afterEach(): Unit = {
    super.afterEach()
  }

  test("should stream word counting successfully") {
    // Given
    val accumulator = sc.collectionAccumulator[(String, Long)]("wordcount")
    val record = new ProducerRecord[String, String]("wordcount", "1", "Hello world")
    producer.send(record)

    val messages = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("wordcount"), getKafkaParameters)
    )

    // When
    WordCountStreamingApp.runJob(accumulator, messages)
    streamingContext.start()
    streamingContext.awaitTerminationOrTimeout(3000)

    // Then
    assert(accumulator.value.contains(("Hello", 1L)))
    assert(accumulator.value.contains(("world", 1L)))
  }
}
