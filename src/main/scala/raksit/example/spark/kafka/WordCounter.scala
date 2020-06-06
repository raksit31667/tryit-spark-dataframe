package raksit.example.spark.kafka

import org.apache.spark.streaming.dstream.DStream

object WordCounter {

  def count(text: DStream[String]): DStream[(String, Long)] = {
    val words = text.flatMap(_.split(" "))
    val wordCounts = words.filter(!_.isEmpty).map(x => (x, 1L)).reduceByKey(_+_)
    wordCounts
  }
}
