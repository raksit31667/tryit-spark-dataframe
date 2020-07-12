package raksit.example.spark.order

import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import raksit.example.spark.InitSpark
import raksit.example.spark.order.OrderAggregator.collectOrdersByClientId
import raksit.example.spark.order.OrderIdValidator.addValidOrderIdFlag
import raksit.example.spark.order.OrderQuantitativeValidator.addValidPriceAndAmountFlag

object OrderStreamingApp extends InitSpark {

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    initSession() andThen fromKafka() andThen processOrder andThen sendBackToKafka()
  }

  def processOrder(orderDataFrame: DataFrame): DataFrame = {
    addValidOrderIdFlag() andThen
      addValidPriceAndAmountFlag() andThen
      collectOrdersByClientId() apply orderDataFrame
  }

  private def fromKafka(): SparkSession => DataFrame = {
    val orderSchema = StructType(
      List(StructField("orderId", LongType),
        StructField("clientId", StringType),
        StructField("amount", IntegerType),
        StructField("price", DoubleType)))

    spark =>
      spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "order")
        .load()
        .selectExpr("CAST (value AS STRING)").as[String]
        .select(from_json(col("value"), orderSchema))
  }

  private def sendBackToKafka(): DataFrame => StreamingQuery = {
    dataFrame =>
      dataFrame.selectExpr("CAST(clientId AS STRING) AS key", "to_json(struct(*)) AS value")
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "order_aggregated")
        .start()
  }
}
