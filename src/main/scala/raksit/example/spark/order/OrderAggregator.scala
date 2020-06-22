package raksit.example.spark.order

import org.apache.spark.sql.functions.{col, collect_list, min, sum}
import org.apache.spark.sql.{Column, DataFrame}

object OrderAggregator {

  def collectOrdersByClientId(dataFrame: DataFrame): DataFrame = {
    val validationFlagColumns = Array(min("_isValidOrderId").alias("_isValidOrderId"),
      min("_isValidPriceAndAmount").alias("_isValidPriceAndAmount"))
    val subtotalColumn = Array(sum(col("price") * col("amount")).alias("subtotal"))
    val allColumns = Array.empty[Column] ++ validationFlagColumns ++ subtotalColumn
    dataFrame.groupBy("clientId")
      .agg(collect_list("orderId").alias("orderIds"), allColumns: _*)
  }
}
