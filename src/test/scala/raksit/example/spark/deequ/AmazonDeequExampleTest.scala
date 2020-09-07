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
    val t0 = System.nanoTime()
    AmazonDeequExample.verifyByColumns(data)
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / math.pow(10, 9) + "s")
  }

  test("it should print column statistic when see column statistic given test dataset") {
    // Given
    val data = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(getClass.getResource("/items.csv").getPath)

    // When
    // Then
    val t0 = System.nanoTime()
    AmazonDeequExample.seeColumnStatistic(data)
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / math.pow(10, 9) + "s")
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
    val t0 = System.nanoTime()
    AmazonDeequExample.verifyByRows(data)
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / math.pow(10, 9) + "s")
  }
}
