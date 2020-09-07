package raksit.example.spark.deequ

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite

class AmazonDeequExampleTest extends FunSuite with DataFrameSuiteBase {

  test("it should print failed constraint from verification result when verify by columns given test dataset") {
    // Given
    val data = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(getClass.getResource("/items.csv").getPath)

    // When
    // Then
    AmazonDeequExample.verifyByColumns(data)
  }

  test("it should print invalid rows when verify by row given test dataset") {
    // Given
    val data = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(getClass.getResource("/items.csv").getPath)

    // When
    // Then
    AmazonDeequExample.verifyByRows(data)
  }
}
