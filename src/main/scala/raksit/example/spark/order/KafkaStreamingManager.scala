package raksit.example.spark.order

import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object KafkaStreamingManager {

  def fromKafka(schema: StructType, topic: String): SparkSession => DataFrame = {
    spark =>
      import spark.implicits._

      spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", topic)
        .load()
        .selectExpr("CAST (value AS STRING)").as[String]
        .select(from_json(col("value"), schema))
  }

  def sendBackToKafka(topic: String): DataFrame => StreamingQuery = {
    dataFrame =>
      dataFrame.selectExpr("CAST(clientId AS STRING) AS key", "to_json(struct(*)) AS value")
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", topic)
        .start()
  }
}
