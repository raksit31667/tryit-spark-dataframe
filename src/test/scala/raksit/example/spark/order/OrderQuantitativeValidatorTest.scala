package raksit.example.spark.order

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite

class OrderQuantitativeValidatorTest extends FunSuite with DataFrameSuiteBase {
  
  import spark.implicits._

  test("should return dataframe with validation flag true when addValidPriceAndAmountFlag given positive price and amount") {
    // Given
    val input = Seq((123456, "abcd", 2, 100)).toDF("orderId", "clientId", "amount", "price")

    // When
    val actual = OrderQuantitativeValidator.addValidPriceAndAmountFlag().apply(input)

    // Then
    val expected = Seq((123456, "abcd", 2, 100, true)).toDF("orderId", "clientId", "amount", "price", "_isValidPriceAndAmount")
    assertDataFrameDataEquals(expected, actual)
  }

  test("should return dataframe with validation flag false when addValidPriceAndAmountFlag given zero price and amount") {
    // Given
    val input = Seq((234567, "abcd", 0, 0)).toDF("orderId", "clientId", "amount", "price")

    // When
    val actual = OrderQuantitativeValidator.addValidPriceAndAmountFlag().apply(input)

    // Then
    val expected = Seq((234567, "abcd", 0, 0, false)).toDF("orderId", "clientId", "amount", "price", "_isValidPriceAndAmount")
    assertDataFrameDataEquals(expected, actual)
  }

  test("should return dataframe with validation flag false when addValidPriceAndAmountFlag given negative price and amount") {
    // Given
    val input = Seq((null, "abcd", -1, -99)).toDF("orderId", "clientId", "amount", "price")

    // When
    val actual = OrderQuantitativeValidator.addValidPriceAndAmountFlag().apply(input)

    // Then
    val expected = Seq((null, "abcd", -1, -99, false)).toDF("orderId", "clientId", "amount", "price", "_isValidPriceAndAmount")
    assertDataFrameDataEquals(expected, actual)
  }

  test("should return dataframe with validation flag false when addValidPriceAndAmountFlag given " +
    "positive price but negative amount") {
    // Given
    val input = Seq((null, "abcd", 2, -100)).toDF("orderId", "clientId", "amount", "price")

    // When
    val actual = OrderQuantitativeValidator.addValidPriceAndAmountFlag().apply(input)

    // Then
    val expected = Seq((null, "abcd", 2, -100, false)).toDF("orderId", "clientId", "amount", "price", "_isValidPriceAndAmount")
    assertDataFrameDataEquals(expected, actual)
  }

  test("should return dataframe with validation flag false when addValidPriceAndAmountFlag given " +
    "negative price but positive amount") {
    // Given
    val input = Seq((null, "abcd", -3, 80)).toDF("orderId", "clientId", "amount", "price")

    // When
    val actual = OrderQuantitativeValidator.addValidPriceAndAmountFlag().apply(input)

    // Then
    val expected = Seq((null, "abcd", -3, 80, false)).toDF("orderId", "clientId", "amount", "price", "_isValidPriceAndAmount")
    assertDataFrameDataEquals(expected, actual)
  }
}
