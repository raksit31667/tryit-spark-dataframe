package raksit.example.spark.order

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object OrderQuantitativeValidator {

  def addValidPriceAndAmountFlag(dataFrame: DataFrame): DataFrame = {
    val priceAndAmountColumnNames = Seq("price", "amount")
    dataFrame.withColumn("_isValidPriceAndAmount",
      priceAndAmountColumnNames.map(name => col(name) > 0).reduce(_ && _))
  }
}
