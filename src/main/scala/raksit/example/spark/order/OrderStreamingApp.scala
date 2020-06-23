package raksit.example.spark.order

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, from_json}
import raksit.example.spark.InitSpark

object OrderStreamingApp extends InitSpark {

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val inputStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "order")
      .load()

    val inputJsonString = inputStream.selectExpr("CAST (value AS STRING)").as[String]

    val orderSchema = StructType(
      List(StructField("orderId", LongType),
        StructField("clientId", StringType),
        StructField("amount", IntegerType),
        StructField("price", DoubleType)))

    val orderDataFrame = inputJsonString.select(from_json(col("value"), orderSchema))

    val aggregatedOrderDataFrame: DataFrame = processOrder(orderDataFrame)

    val streamingQuery = aggregatedOrderDataFrame
      .selectExpr("CAST(clientId AS STRING) AS key", "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "order_aggregated")
      .start()

    streamingQuery.awaitTermination()
  }

  def processOrder(orderDataFrame: DataFrame): DataFrame = {
    val orderDataFrameWithValidIdFlag = OrderIdValidator.addValidOrderIdFlag(orderDataFrame)
    val orderDataFrameWithValidPriceAndAmountFlag = OrderQuantitativeValidator.addValidPriceAndAmountFlag(orderDataFrameWithValidIdFlag)
    val aggregatedOrderDataFrame = OrderAggregator.collectOrdersByClientId(orderDataFrameWithValidPriceAndAmountFlag)
    aggregatedOrderDataFrame
  }
}
