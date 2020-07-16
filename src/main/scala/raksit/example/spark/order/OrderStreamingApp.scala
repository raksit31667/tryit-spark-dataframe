package raksit.example.spark.order

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import raksit.example.spark.InitSpark
import raksit.example.spark.order.KafkaStreamingManager.{fromKafka, sendBackToKafka}
import raksit.example.spark.order.OrderAggregator.collectOrdersByClientId
import raksit.example.spark.order.OrderIdValidator.addValidOrderIdFlag
import raksit.example.spark.order.OrderQuantitativeValidator.addValidPriceAndAmountFlag

object OrderStreamingApp extends InitSpark {

  def main(args: Array[String]): Unit = {
    initSession() andThen
      fromKafka(getOrderSchema) andThen
      processOrder andThen
      sendBackToKafka("order_aggregated")
  }

  def processOrder(orderDataFrame: DataFrame): DataFrame = {
    addValidOrderIdFlag() andThen
      addValidPriceAndAmountFlag() andThen
      collectOrdersByClientId() apply orderDataFrame
  }

  private def getOrderSchema = {
    StructType(
      List(StructField("orderId", LongType),
        StructField("clientId", StringType),
        StructField("amount", IntegerType),
        StructField("price", DoubleType)))
  }
}
