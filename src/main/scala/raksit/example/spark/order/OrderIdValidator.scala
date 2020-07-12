package raksit.example.spark.order

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object OrderIdValidator {

  def addValidOrderIdFlag(): DataFrame => DataFrame = {
    dataFrame =>
      dataFrame.withColumn("_isValidOrderId",
        col("orderId").isNotNull and col("orderId").isInCollection(Seq(123456, 987654)))
  }
}
