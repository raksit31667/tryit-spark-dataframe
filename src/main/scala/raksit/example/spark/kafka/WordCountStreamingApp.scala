package raksit.example.spark.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.TaskContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.HasOffsetRanges
import org.apache.spark.util.CollectionAccumulator
import raksit.example.spark.InitSpark

object WordCountStreamingApp extends InitSpark {

  def runJob(accumulator: CollectionAccumulator[(String, Long)], messages: InputDStream[ConsumerRecord[String, String]]): Unit = {
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
    wordCounts.foreachRDD { rdd =>
      rdd.foreach { pair =>
        accumulator.add(pair)
      }
    }
  }
}
